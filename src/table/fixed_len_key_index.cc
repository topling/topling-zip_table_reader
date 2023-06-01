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

class FixedLenKeyIndex : public COIndex {
public:
  static constexpr char index_name[] = "FixedLenKeyIndex";
  DaCacheFixedStrVec m_keys;
  size_t m_commonPrefixLen;

  struct Header : ToplingIndexHeader {
    uint64_t checksum;
  };
  const Header* m_header = nullptr;
  class Iter;
  class MyFactory;

  void Delete() override {
    this->~FixedLenKeyIndex();
    free(this);
  }

  const char* Name() const override { return index_name; }
  void SaveMmap(std::function<void(const void *, size_t)> write) const override;

  inline size_t lower_bound_fixed(fstring suffix) const {
    if (m_keys.has_cache()) {
      const auto res = m_keys.da_match(suffix);
      return m_keys.lower_bound_fixed(res.lo, res.hi, suffix.p);
    } else {
      return m_keys.lower_bound_fixed(suffix.p);
    }
  }
  inline size_t lower_bound_prefix(fstring suffix) const {
    if (m_keys.has_cache()) {
      const auto res = m_keys.da_match(suffix);
      if (UNLIKELY(suffix.size() == res.depth))
        return res.lo; // MyTopling short seek may hit this case
      else
        return m_keys.lower_bound_prefix(res.lo, res.hi, suffix);
    } else {
      return m_keys.lower_bound_prefix(suffix);
    }
  }

  size_t Find(fstring key) const override {
    const size_t pref_len = m_commonPrefixLen;
    if (UNLIKELY(key.size() != pref_len + m_keys.m_fixlen)) {
      return size_t(-1);
    }
    if (UNLIKELY(memcmp(key.p, m_commonPrefixData, pref_len) != 0)) {
      return size_t(-1);
    }
    fstring suffix = key.substr(pref_len);
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
      const fstring hit_key = m_keys[lo];
      if (suffix.size() > hit_key.size()) {
        int cmp = memcmp(hit_key.p, suffix.p, hit_key.size());
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
    auto rng = m_keys.da_match(suffix);
    return rng.lo;
  #else
    return AccurateRank(key);
  #endif
  }
  size_t TotalKeySize() const override {
    return (m_commonPrefixLen + m_keys.m_fixlen) * m_keys.m_size;
  }
  fstring Memory() const override {
    TERARK_VERIFY(nullptr != m_header);
    return fstring((const char*)m_header, m_header->file_size);
  }
  std::string NthKey(size_t nth) const final {
    TERARK_VERIFY_LT(nth, m_num_keys);
    const size_t pref_len = m_commonPrefixLen;
    const size_t suff_len = m_keys.m_fixlen;
    std::string key;
    key.reserve(pref_len + suff_len);
    key.append(m_commonPrefixData, pref_len);
    key.append((const char*)m_keys.nth_data(nth), suff_len);
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

class FixedLenKeyIndex::Iter : public COIndex::FastIter {
protected:
  const FixedLenKeyIndex* m_index;
  uint32_t m_pref_len;
  uint32_t m_total_len;
  byte_t m_key_data[0];

  inline bool Done(size_t id) {
    m_id = id;
    fstring key = m_index->m_keys[id];
    memcpy(m_key_data + m_pref_len, key.data(), key.size());
    return true;
  }
  bool Fail() { m_id = size_t(-1); return false; }
public:
  // caller should use placement new and allocate extra mem for m_key_data
  explicit Iter(const FixedLenKeyIndex* index) : m_index(index) {
    m_num = index->NumKeys();
    m_pref_len = index->m_commonPrefixLen;
    m_total_len = m_pref_len + index->m_keys.m_fixlen;
    memcpy(m_key_data, index->m_commonPrefixData, m_pref_len);
    // m_total_len is redundant, it is same as m_key.n, don't change, KISS
    m_key = fstring(m_key_data, m_total_len);
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
    if (UNLIKELY(key.size() > m_total_len)) {
      const fstring hit_key = m_index->m_keys[lo];
      int cmp = memcmp(hit_key.p, suffix.p, hit_key.size());
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
template<int SuffixLen>
class FixedLenKeyIndexIterTmpl : public FixedLenKeyIndex::Iter {
public:
  using Iter::Iter;
  inline bool DoneConstLen(size_t id) {
    auto key = m_index->m_keys.data() + SuffixLen * id;
    memcpy(m_key_data + m_pref_len, key, SuffixLen);
    return true;
  }
  bool Next() override {
    if (++m_id < m_num)
      return DoneConstLen(m_id);
    else
      return Fail();
  }
  bool Prev() override {
    if (m_id > 0)
      return DoneConstLen(--m_id);
    else
      return Fail();
  }
};
COIndex::Iterator* FixedLenKeyIndex::NewIterator() const {
  void* mem = malloc(sizeof(Iter) + m_commonPrefixLen + m_keys.m_fixlen + 16);
  switch (m_keys.m_fixlen) {
#define case_of(SuffixLen) \
  case SuffixLen: return new(mem)FixedLenKeyIndexIterTmpl<SuffixLen>(this)
  case_of( 1);
  case_of( 2);
  case_of( 3);
  case_of( 4);
  case_of( 5);
  case_of( 6);
  case_of( 7);
  case_of( 8);
  case_of( 9);
  case_of(10);
  case_of(11);
  case_of(12);
  case_of(13);
  case_of(14);
  case_of(15);
  case_of(16);
  case_of(20);
  case_of(24);
  case_of(32);
  case_of(40);
  case_of(48);
  case_of(56);
  case_of(64);
  default: break; // fallback
  }
  return new(mem)Iter(this);
}

class FixedLenKeyIndex::MyFactory : public COIndex::Factory {
public:
  COIndex* Build(const std::string& keyFilePath,
                     const ToplingZipTableOptions& tzopt,
                     const KeyStat& ks,
                     const ImmutableOptions* iopt) const override {
    size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
    size_t full_cplen = ks.prefix.size() + cplen;
    auto raw = malloc(sizeof(FixedLenKeyIndex) + full_cplen);
    auto fix = new(raw)FixedLenKeyIndex();
    COIndexUP fix_up(fix); // guard
    MmapRead_FixedLenStrVec(keyFilePath, ks, fix->m_keys);
    MinMemIO mio(fix->m_commonPrefixData);
    mio.ensureWrite(ks.prefix.data(), ks.prefix.size());
    mio.ensureWrite(ks.minKey.data(), cplen);
    fix->m_commonPrefixLen = full_cplen;
    fix->m_num_keys = ks.numKeys;
    if (tzopt.fixedLenIndexCacheLeafSize >= 64) {
      ROCKS_LOG_DEBUG(iopt->info_log,
        "FixedLenKeyIndex build_cache Start: num_keys = %zd, fixlen = %zd, pref_len = %zd, cplen = %zd",
        ks.numKeys, ks.minKeyLen, ks.prefix.size(), cplen);
      auto t0 = g_pf.now();
      fix->m_keys.build_cache(tzopt.fixedLenIndexCacheLeafSize);
      auto t1 = g_pf.now();
      ROCKS_LOG_INFO(iopt->info_log,
        "FixedLenKeyIndex build_cache Finish: num_keys = %zd, fixlen = %zd, pref_len = %zd, cplen = %zd, time = %.3f sec",
        ks.numKeys, ks.minKeyLen, ks.prefix.size(), cplen, g_pf.sf(t0, t1));
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
    size_t pref_len = header->reserved_80_4;
    size_t fixlen = header->reserved_92_4;
    size_t num = header->reserved_102_24;
    size_t meta_size = align_up(sizeof(Header) + pref_len, 16);
    auto raw = malloc(sizeof(FixedLenKeyIndex) + pref_len);
    auto fix = new(raw)FixedLenKeyIndex();
    fix->m_header = header;
    fix->m_num_keys = num;
    memcpy(fix->m_commonPrefixData, header + 1, pref_len);
    fix->m_commonPrefixLen = pref_len;
    fix->m_keys.load_mmap((byte_t*)mem.p + meta_size, header->file_size - meta_size);
    TERARK_VERIFY_EQ(fixlen, fix->m_keys.m_fixlen);
    TERARK_VERIFY_EQ(num, fix->m_keys.m_size);
    return COIndexUP(fix);
  }
};
void FixedLenKeyIndex::SaveMmap(std::function<void(const void*, size_t)> write) const {
  Header h;
  memset(&h, 0, sizeof(h));
  h.magic_len = strlen(index_name);
  static_assert(strlen(index_name) < sizeof(h.magic));
  strcpy(h.magic, index_name);
  fstring clazz = COIndexGetClassName(typeid(FixedLenKeyIndex));
  TERARK_VERIFY_LT(clazz.size(), sizeof(h.class_name));
  memcpy(h.class_name, clazz.c_str(), clazz.size());
  h.header_size = sizeof(h);
  h.reserved_80_4 = m_commonPrefixLen;
  h.reserved_92_4 = m_keys.m_fixlen;
  h.reserved_102_24 = m_keys.m_size;
  h.version = 1; // version = 1 means don't checksum
  h.checksum = 0; // don't checksum now
  auto meta_size = align_up(sizeof(h) + m_commonPrefixLen, 16);
  h.file_size = align_up(meta_size + m_keys.size_mmap(), 64);
  write(&h, sizeof(h));
  write(m_commonPrefixData, m_commonPrefixLen);
  Padzero<16>(write, sizeof(h) + m_commonPrefixLen);
  size_t size_mmap = m_keys.save_mmap(write);
  TERARK_VERIFY_EQ(m_keys.size_mmap(), size_mmap);
  Padzero<64>(write, meta_size + size_mmap);
}

COIndexRegister(FixedLenKeyIndex);

} // namespace rocksdb
