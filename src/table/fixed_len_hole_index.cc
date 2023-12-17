#include "co_index.h"
#include <table/top_table_common.h>
#include <terark/fsa/da_cache_fixed_strvec.hpp>
#include <terark/io/MemStream.hpp>
#include "top_zip_internal.h"
#include <topling/json.h>

namespace rocksdb {
using namespace terark;

fstring COIndexGetClassName(const std::type_info&);
void VerifyClassName(const char* class_name, const std::type_info&);
void MmapRead_FixedLenStrVec(fstring, const COIndex::KeyStat&, FixedLenStrVec&);
void MmapRead_ReadSortedStrVec(fstring, const COIndex::KeyStat&, SortedStrVec&);

static size_t KeyMetaSize(size_t prefixLen, size_t suffixLen) {
  return align_up(prefixLen, 2) + sizeof(uint16_t) * suffixLen;
}

class FixedLenHoleIndex : public COIndex {
public:
  static constexpr char index_name[] = "FixedLenHoleIndex";
  DaCacheFixedStrVec m_keys;
  uint32_t m_commonPrefixLen;
  uint32_t m_suffixLen;

  struct Header : ToplingIndexHeader {
    uint32_t suffixLen;
    uint32_t reserved_FixedLenHoleIndex;
    uint64_t checksum;
  };
  const Header* m_header = nullptr;
  class Iter;
  class MyFactory;

  void Delete() override {
    this->~FixedLenHoleIndex();
    free(this);
  }

  const char* Name() const override { return index_name; }
  void SaveMmap(std::function<void(const void *, size_t)> write) const override;

  void PopulateHoles(const COIndex::KeyStat& ks, size_t cplen,
                     const byte_t* key_data_with_holes) {
    TERARK_VERIFY(ks.enableFixedLenHoleIndex);
    TERARK_VERIFY_EQ(cplen, commonPrefixLen(ks.minKey, ks.maxKey));
    TERARK_VERIFY_EQ(cplen + ks.holeMeta.size(), ks.maxKeyLen);
    uint16_t* holeMeta = MutableHoleMeta();
    memcpy(holeMeta, ks.holeMeta.data(), ks.holeMeta.used_mem_size());
    m_suffixLen = uint32_t(ks.minKey.size() - cplen);
    m_keys.m_fixlen = uint32_t(m_suffixLen - ks.holeLen);
  }

  void PopulateIndexContent(const FixedLenStrVec& keysWithHoles) {
    const uint16_t* holeMeta = GetHoleMeta();
    size_t srcFixLen = keysWithHoles.m_fixlen;
    size_t dstFixLen = m_keys.m_fixlen;
    size_t numKeys = m_num_keys;
    m_keys.m_size = m_num_keys;
    m_keys.m_strpool.resize_no_init(dstFixLen * numKeys);
    const byte_t* src = keysWithHoles.data();
    byte_t* dst = m_keys.data();
    for (size_t k = 0; k < numKeys; k++) {
      size_t j = 0;
      for (size_t i = 0; i < srcFixLen; i++) {
        if (holeMeta[i] >= 256)
          dst[j++] = src[i];
      }
      src += srcFixLen;
      dst += dstFixLen;
    }
    m_keys.optimize_func();
  }

  const uint16_t* GetHoleMeta() const {
    size_t aligned = pow2_align_up(m_commonPrefixLen, 2);
    return (const uint16_t*)(m_commonPrefixData + aligned);
  }
  uint16_t* MutableHoleMeta() {
    size_t aligned = pow2_align_up(m_commonPrefixLen, 2);
    return (uint16_t*)(m_commonPrefixData + aligned);
  }

  // key len has been checked
  bool IsHoleMatch(const char* keySuffix, const uint16_t* holeMeta) const {
    const size_t suffixLen = m_suffixLen;
    for (size_t i = 0; i < suffixLen; i++) {
      if (holeMeta[i] < 256) { // pos i is a hole
        if (holeMeta[i] != byte_t(keySuffix[i]))
          return false;
      }
      else {
        // pos i is not a hole
      }
    }
    return true;
  }

  ///@param vbuf is by alloca
  fstring SuffixWithoutHole(const char* keySuffix, const uint16_t* holeMeta, void* vbuf) const {
    char* buf = (char*)vbuf;
    const size_t suffixLen = m_suffixLen;
    size_t j = 0; // slow ptr
    for (size_t i = 0; i < suffixLen; i++) {
      if (holeMeta[i] < 256) { // pos i is a hole
        // do nothing
      }
      else {
        buf[j++] = keySuffix[i];
      }
    }
    TERARK_ASSERT_EQ(j, m_keys.m_fixlen);
    return fstring(buf, j);
  }

  inline size_t lower_bound_fixed(fstring suffix) const {
    if (m_keys.has_cache()) {
      const auto res = m_keys.da_match(suffix);
      return m_keys.lower_bound_fixed(res.lo, res.hi, suffix.p);
    } else {
      return m_keys.lower_bound_fixed(suffix.p);
    }
  }

  ///@param indexKey has no hole
  ///@param searchKey has hole
  int SuffixCompare(const byte_t* indexKey, const byte_t* searchKeySuffix,
                    const size_t len, const uint16_t* holeMeta) const {
    TERARK_ASSERT_LE(len, m_suffixLen);
    size_t j = 0;
    for (size_t i = 0; i < len; i++) {
      byte_t cIndex = holeMeta[i] < 256 ? byte_t(holeMeta[i]) : indexKey[j++];
      byte_t cSearch = searchKeySuffix[i];
      if (cIndex != cSearch)
        return cIndex - cSearch;
    }
    return 0;
  }

  size_t lower_bound_impl(size_t lo, size_t hi, const byte_t* suffix,
                          size_t cmpLen, const uint16_t* holeMeta) const {
    size_t fixlen = m_keys.m_fixlen;
    auto data = m_keys.m_strpool.data();
    while (lo < hi) {
      size_t mid_idx = (lo + hi) / 2;
      size_t mid_pos = fixlen * mid_idx;
      if (SuffixCompare(data + mid_pos, suffix, cmpLen, holeMeta) < 0)
        lo = mid_idx + 1;
      else
        hi = mid_idx;
    }
    return lo;
  }

  inline size_t lower_bound_prefix(fstring suffix) const {
    const uint16_t* holeMeta = GetHoleMeta();
    size_t cmpLen = std::min(suffix.size(), size_t(m_suffixLen));
    if (m_keys.has_cache()) {
      const auto res = m_keys.da_match_with_hole(suffix.udata(), cmpLen, holeMeta);
      if (UNLIKELY(suffix.size() == res.depth))
        return res.lo; // MyTopling short seek may hit this case
      else
        return lower_bound_impl(res.lo, res.hi, suffix.udata(), cmpLen, holeMeta);
    } else {
      return lower_bound_impl(0, m_keys.m_size, suffix.udata(), cmpLen, holeMeta);
    }
  }

  size_t Find(fstring key) const override {
    const size_t pref_len = m_commonPrefixLen;
    if (UNLIKELY(key.size() != pref_len + m_suffixLen)) {
      return size_t(-1);
    }
    if (UNLIKELY(memcmp(key.p, m_commonPrefixData, pref_len) != 0)) {
      return size_t(-1);
    }
    fstring suffix = key.substr(pref_len);
    const uint16_t* holeMeta = GetHoleMeta();
    if (!IsHoleMatch(suffix.p, holeMeta)) {
      return size_t(-1);
    }
    suffix = SuffixWithoutHole(suffix.p, holeMeta, alloca(m_keys.m_fixlen));
    size_t lo = this->lower_bound_fixed(suffix);
    if (lo < m_keys.m_size) {
      if (memcmp(m_keys.nth_data(lo), suffix.p, suffix.n) == 0)
        return lo;
    }
    return size_t(-1);
  }
  size_t AccurateRank(fstring key) const override {
    const size_t pref_len = m_commonPrefixLen;
    if (UNLIKELY(key.size() <= pref_len)) {
      if (memcmp(key.p, m_commonPrefixData, key.size()) <= 0)
        return 0;
      else
        return m_keys.m_size;
    }
    int cmp = memcmp(key.p, m_commonPrefixData, pref_len);
    if (cmp != 0) {
      if (cmp < 0)
        return 0;
      else
        return m_keys.m_size;
    }
    fstring suffix = key.substr(pref_len);
    size_t  lo = this->lower_bound_prefix(suffix);
    if (lo < m_keys.m_size) {
      if (suffix.size() > m_suffixLen) {
        const fstring hit_key = m_keys[lo];
        const uint16_t* holeMeta = GetHoleMeta();
        int cmp = SuffixCompare(hit_key.udata(), suffix.udata(), m_suffixLen, holeMeta);
        if (0 == cmp) {
          ++lo;
        }
      }
    }
    return lo;
  }
  size_t ApproximateRank(fstring key) const override {
  #if 0
    // just search da cache is not monotone by key
    const size_t pref_len = m_commonPrefixLen;
    if (UNLIKELY(key.size() <= pref_len)) {
      if (memcmp(key.p, m_commonPrefixData, key.size()) <= 0)
        return 0;
      else
        return m_keys.m_size;
    }
    int cmp = memcmp(key.p, m_commonPrefixData, pref_len);
    if (cmp != 0) {
      if (cmp < 0)
        return 0;
      else
        return m_keys.m_size;
    }
    fstring suffix = key.substr(pref_len);
    minimize(suffix.n, ptrdiff_t(m_suffixLen));
    auto rng = m_keys.da_match_with_hole(suffix, GetHoleMeta());
    return rng.lo;
  #else
    return AccurateRank(key);
  #endif
  }
  size_t TotalKeySize() const override {
    return (m_commonPrefixLen + m_suffixLen) * m_keys.m_size;
  }
  fstring Memory() const override {
    TERARK_VERIFY(nullptr != m_header);
    return fstring((const char*)m_header, m_header->file_size);
  }
  fstring ResidentMemory() const noexcept override {
    return m_keys.da_memory();
  }
  std::string NthKey(size_t nth) const final {
    TERARK_VERIFY_LT(nth, m_num_keys);
    const size_t pref_len = m_commonPrefixLen;
    const size_t suff_len = m_suffixLen;
    const uint16_t* holeMeta = GetHoleMeta();
    std::string key(pref_len + suff_len, '\0');
    char* key_data = key.data();
    memcpy(key_data, m_commonPrefixData, pref_len);
    auto src = (const char*)m_keys.nth_data(nth);
    auto dst = key_data + pref_len;
    for (size_t i = 0, j = 0; i < suff_len; i++) {
      if (holeMeta[i] < 256) {
        dst[i] = (char)holeMeta[i];
      } else {
        dst[i] = src[j++]; // j is slow ptr
      }
    }
    TERARK_ASSERT_EQ(Find(key), nth);
    return key;
  }
  Iterator* NewIterator() const override;
  bool NeedsReorder() const override { return false; }
  void GetOrderMap(terark::UintVecMin0&) const override {
    TERARK_DIE("Should not goes here");
  }
  void BuildCache(double cacheRatio) override {
    // do nothing
  }
  json MetaToJson() const override {
    TERARK_VERIFY(nullptr != m_header);
    json djs;
    djs["CommonPrefixLen"] = m_commonPrefixLen;
    djs["CommonPrefix"] = Slice(m_commonPrefixData, m_commonPrefixLen).hex();
    djs["NumKeys"] = m_keys.m_size;
    djs["SuffixLen"] = m_suffixLen;
    djs["HoleLen"] = m_suffixLen - m_keys.m_fixlen;
    djs["FixLen"] = m_keys.m_fixlen;
    djs["MemSize"] = m_header->file_size;
    djs["DaStates"] = m_keys.da_size();
    djs["DaHoles"] = m_keys.da_free();
    djs["DaUtilize"] = 0 == m_keys.da_size() ? 0.0 :
      1.0*(m_keys.da_size() - m_keys.da_free()) / m_keys.da_size();
    return djs;
  }
  bool HasFastApproximateRank() const noexcept override {
  #if 0
    // Fast ApproximateRank is not monotone by key, so always return false.
    // And search on da cache is slower than binary search rank cache!
    return m_keys.has_cache();
  #else
    return false;
  #endif
  }

  // gcc bug: gcc allows fields after zero len array
  char   m_commonPrefixData[0]; // must be last field
};

class FixedLenHoleIndex::Iter : public COIndex::FastIter {
protected:
  const FixedLenHoleIndex* m_index;
  const byte_t* m_fixed_data;
  const uint16_t* m_hole_meta;
  uint32_t m_suffix_len;
  uint32_t m_pref_len;
  uint32_t m_fixlen;
  uint32_t m_padding;
  byte_t m_key_data[0];

  inline bool Done(size_t id) {
    auto src = m_fixed_data + m_fixlen * id;
    __builtin_prefetch(src);
    auto holeMeta = m_hole_meta;
    __builtin_prefetch(holeMeta);
    m_id = id;
    auto dst = m_key_data + m_pref_len;
    size_t suffixLen = m_suffix_len;
    for (size_t i = 0, j = 0; i < suffixLen; i++) {
      if (holeMeta[i] >= 256) {
        dst[i] = src[j++];
      }
    }
    return true;
  }
  bool Fail() { m_id = size_t(-1); return false; }
public:
  // caller should use placement new and allocate extra mem for m_key_data
  explicit Iter(const FixedLenHoleIndex* index) : m_index(index) {
    m_fixed_data = index->m_keys.data();
    m_suffix_len = index->m_suffixLen;
    m_hole_meta = index->GetHoleMeta();
    m_fixlen = index->m_keys.m_fixlen;
    m_num = index->NumKeys();
    m_pref_len = index->m_commonPrefixLen;
    m_padding = 0;
    size_t full_key_len = m_pref_len + index->m_suffixLen;
    memcpy(m_key_data, index->m_commonPrefixData, m_pref_len);
    for (size_t i = 0; i < m_suffix_len; i++) { // fill hole
      if (m_hole_meta[i] < 256) {
        m_key_data[m_pref_len + i] = byte_t(m_hole_meta[i]);
      }
    }
    m_key = fstring(m_key_data, full_key_len);
  }
  void Delete() override { free(this); }
  bool SeekToFirst() override { return Done(0); }
  bool SeekToLast()  override { return Done(m_num - 1); }
  bool Seek(fstring key) override {
    if (UNLIKELY(key.size() <= m_pref_len)) {
      if (memcmp(key.p, m_key_data, key.size()) <= 0)
        return Done(0);
      else
        return Fail();
    }
    int cmp = memcmp(key.p, m_key_data, m_pref_len);
    if (cmp != 0) {
      if (cmp < 0)
        return Done(0);
      else
        return Fail();
    }
    fstring suffix = key.substr(m_pref_len);
    size_t  lo = m_index->lower_bound_prefix(suffix);
    if (UNLIKELY(lo >= m_num)) {
      return Fail();
    }
    if (UNLIKELY(suffix.size() > m_suffix_len)) {
      const byte_t* hit_key = m_index->m_keys.nth_data(lo);
      int cmp = m_index->SuffixCompare(hit_key, suffix.udata(),
                                       m_suffix_len, m_hole_meta);
      TERARK_ASSERT_GE(cmp, 0);
      if (0 == cmp) { // suffix > hit_key because suffix is longer
        if (++lo == m_num)
          return Fail();
      }
    }
    return Done(lo);
  }
  bool Next() override {
    if (++m_id < m_num)
      return Done(m_id);
    else
      return Fail();
  }
  bool Prev() override {
    if (m_id > 0)
      return Done(--m_id);
    else
      return Fail();
  }
  size_t DictRank() const override { return m_id; }
};
COIndex::Iterator* FixedLenHoleIndex::NewIterator() const {
  auto ikey_cap = std::max<size_t>(m_commonPrefixLen + m_suffixLen + 8, 64u);
  auto mem = (char*)malloc(sizeof(Iter) + ikey_cap);
  memset(mem + sizeof(Iter), 0, ikey_cap);
  return new(mem)Iter(this);
}

class FixedLenHoleIndex::MyFactory : public COIndex::Factory {
public:
  COIndex* Build(const std::string& keyFilePath,
                     const ToplingZipTableOptions& tzopt,
                     const KeyStat& ks,
                     const ImmutableOptions* iopt) const override {
    size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
    size_t full_cplen = ks.prefix.size() + cplen;
    size_t suffix_len = ks.minKey.size() - cplen;
    size_t key_meta_size = KeyMetaSize(full_cplen, suffix_len);
    TERARK_VERIFY_GT(ks.holeLen, 0);
    auto raw = malloc(sizeof(FixedLenHoleIndex) + key_meta_size);
    auto fix = new(raw)FixedLenHoleIndex();
    COIndexUP fix_up(fix); // guard
    FixedLenStrVec fixed_keys;
    MmapRead_FixedLenStrVec(keyFilePath, ks, fixed_keys);
    MinMemIO mio(fix->m_commonPrefixData);
    mio.ensureWrite(ks.prefix.data(), ks.prefix.size());
    mio.ensureWrite(ks.minKey.data(), cplen);
    if (full_cplen % 2) {
      mio.writeByte(0); // align to 2
    }
    fix->m_commonPrefixLen = uint32_t(full_cplen);
    fix->m_num_keys = ks.numKeys;
    fix->PopulateHoles(ks, cplen, fixed_keys.data());
    fix->PopulateIndexContent(fixed_keys);
    if (tzopt.fixedLenIndexCacheLeafSize >= 64) {
      #define FMT "FixedLenHoleIndex build_cache: num_keys %zd, fixlen %zd, prefix %zd, cplen %zd, suffix %d(hole %zd)"
      #define ARG ks.numKeys, ks.minKeyLen, ks.prefix.size(), cplen, fix->m_suffixLen, fix->m_suffixLen - full_cplen
      ROCKS_LOG_DEBUG(iopt->info_log, FMT, ARG);
      auto t0 = g_pf.now();
      fix->m_keys.build_cache(tzopt.fixedLenIndexCacheLeafSize);
      auto t1 = g_pf.now();
      ROCKS_LOG_INFO(iopt->info_log, FMT ", time = %.3f sec", ARG, g_pf.sf(t0, t1));
    }
    fix_up.release(); // must release
    return fix;
  }
  COIndexUP LoadFile(fstring fpath) const override {
    TERARK_DIE("Should not goes here");
  }
  size_t MemSizeForBuild(const KeyStat& ks) const override {
    TERARK_VERIFY_EQ(ks.minKeyLen, ks.maxKeyLen);
    size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
    return (ks.minKeyLen - cplen) * ks.numKeys;
  }
  COIndexUP LoadMemory(fstring mem) const override {
    auto header = (const Header*)mem.data();
    TERARK_VERIFY_EQ(mem.size(), header->file_size);
    uint32_t pref_len = header->reserved_80_4;
    uint32_t fixlen = header->reserved_92_4;
    uint32_t suffix_len = header->suffixLen;
    size_t num = header->reserved_102_24;
    size_t key_meta_size = KeyMetaSize(pref_len, suffix_len);
    size_t meta_size = align_up(sizeof(Header) + key_meta_size, 16);
    auto raw = malloc(sizeof(FixedLenHoleIndex) + key_meta_size);
    auto fix = new(raw)FixedLenHoleIndex();
    fix->m_header = header;
    fix->m_num_keys = num;
    memcpy(fix->m_commonPrefixData, header + 1, key_meta_size);
    fix->m_commonPrefixLen = pref_len;
    fix->m_suffixLen = suffix_len;
    fix->m_keys.load_mmap((byte_t*)mem.p + meta_size, header->file_size - meta_size);
    TERARK_VERIFY_EQ(fixlen, fix->m_keys.m_fixlen);
    TERARK_VERIFY_EQ(num, fix->m_keys.m_size);
    return COIndexUP(fix);
  }
};
void FixedLenHoleIndex::SaveMmap(std::function<void(const void*, size_t)> write) const {
  Header h;
  memset(&h, 0, sizeof(h));
  h.magic_len = strlen(index_name);
  static_assert(sizeof(index_name) <= sizeof(h.magic));
  strcpy(h.magic, index_name);
  fstring clazz = COIndexGetClassName(typeid(FixedLenHoleIndex));
  TERARK_VERIFY_LT(clazz.size(), sizeof(h.class_name));
  memcpy(h.class_name, clazz.c_str(), clazz.size());
  h.header_size = sizeof(h);
  h.reserved_80_4 = m_commonPrefixLen;
  h.reserved_92_4 = m_keys.m_fixlen;
  h.reserved_102_24 = m_keys.m_size;
  h.suffixLen = m_suffixLen;
  h.version = 1; // version = 1 means don't checksum
  h.checksum = 0; // don't checksum now
  size_t key_meta_size = KeyMetaSize(m_commonPrefixLen, m_suffixLen);
  auto meta_size = align_up(sizeof(h) + key_meta_size, 16);
  h.file_size = align_up(meta_size + m_keys.size_mmap(), 64);
  write(&h, sizeof(h));
  write(m_commonPrefixData, key_meta_size);
  Padzero<16>(write, sizeof(h) + key_meta_size);
  size_t size_mmap = m_keys.save_mmap(write);
  TERARK_VERIFY_EQ(m_keys.size_mmap(), size_mmap);
  Padzero<64>(write, meta_size + size_mmap);
}

COIndexRegister(FixedLenHoleIndex);

} // namespace rocksdb
