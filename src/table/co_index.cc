
#include "co_index.h"
#include "top_zip_table.h"
#include <table/top_table_common.h>
#include <terark/hash_strmap.hpp>
#include <terark/util/hugepage.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/sortable_strvec.hpp>
#include "top_zip_internal.h"

namespace rocksdb {

using namespace terark;

static bool g_indexEnableFewZero            = getEnvBool("COIndex_enableFewZero", false);

static auto& GetCOIndexFactroyMap() {
  static hash_strmap<COIndex::FactoryPtr> g_COIndexFactroy;
  return g_COIndexFactroy;
}
static auto& GetCOIndexNameMap() {
static hash_strmap<std::string,
                   fstring_func::hash_align,
                   fstring_func::equal_align,
                   ValueInline,
                   SafeCopy ///< some std::string is not memmovable
                  >
       g_COIndexName;
  return g_COIndexName;
}
#define g_COIndexFactroy GetCOIndexFactroyMap()
#define g_COIndexName    GetCOIndexNameMap()

void TIH_set_class_name(ToplingIndexHeader* h, const std::type_info& ti) {
  size_t name_i = g_COIndexName.find_i(ti.name());
  assert(name_i < g_COIndexFactroy.end_i());
  fstring clazz = g_COIndexName.val(name_i);
  TERARK_VERIFY_LT(clazz.size(), sizeof(h->class_name));
  memcpy(h->class_name, clazz.c_str(), clazz.size());
}

void VerifyClassName(const char* class_name, const std::type_info& ti) {
  size_t name_i = g_COIndexName.find_i(ti.name());
  size_t self_i = g_COIndexFactroy.find_i(g_COIndexName.val(name_i));
  ROCKSDB_VERIFY_F(self_i < g_COIndexFactroy.end_i(), "%s", class_name);
  size_t head_i = g_COIndexFactroy.find_i(class_name);
  ROCKSDB_VERIFY_F(head_i < g_COIndexFactroy.end_i(), "%s", class_name);
  ROCKSDB_VERIFY_F(g_COIndexFactroy.val(head_i) ==
                   g_COIndexFactroy.val(self_i), "%s", class_name);
}

fstring COIndexGetClassName(const std::type_info& ti) {
  size_t name_i = g_COIndexName.find_i(ti.name());
  assert(name_i < g_COIndexFactroy.end_i());
  fstring clazz = g_COIndexName.val(name_i);
  return clazz;
}

// 0 < cnt0 and cnt0 < 0.01 * total
bool IsFewZero(size_t total, size_t cnt0) {
  if (!g_indexEnableFewZero)
    return false;
  assert(total > 0);
  return (0 < cnt0) &&
    (cnt0 <= (double)total * 0.01);
}
bool IsFewOne(size_t total, size_t cnt1) {
  if (!g_indexEnableFewZero)
    return false;
  assert(total > 0);
  return (0 < cnt1) &&
    ((double)total * 0.005 < cnt1) &&
    (cnt1 <= (double)total * 0.01);
}

COIndex::AutoRegisterFactory::AutoRegisterFactory(
                          std::initializer_list<const char*> names,
                          const char* rtti_name,
                          Factory* factory) {
  assert(names.size() > 0);
  fstring wireName = *names.begin();
  TERARK_VERIFY_S(!g_COIndexFactroy.exists(wireName), "%s", wireName);
  factory->mapIndex = g_COIndexFactroy.end_i();
  for (const char* name : names) {
    auto ib = g_COIndexFactroy.insert_i(name, FactoryPtr(factory));
    TERARK_VERIFY_S(ib.second, "rtti = %s, wire = %s", rtti_name, name);
  }
  auto ib = g_COIndexName.insert_i(rtti_name, wireName.str());
  TERARK_VERIFY_S(ib.second, "rtti = %s, wire = %s", rtti_name, wireName);
}

const COIndex::Factory* COIndex::GetFactory(fstring name) {
  size_t idx = g_COIndexFactroy.find_i(name);
  if (idx < g_COIndexFactroy.end_i()) {
    auto factory = g_COIndexFactroy.val(idx).get();
    return factory;
  }
  STD_WARN("COIndex::GetFactory: NotFound: name = %s", name.c_str());
  return NULL;
}

const char* COIndex::Factory::WireName() const {
  TERARK_VERIFY_LT(mapIndex, g_COIndexFactroy.end_i());
  return g_COIndexFactroy.key_c_str(mapIndex);
}

bool COIndex::SeekCostEffectiveIndexLen(const KeyStat& ks, size_t& ceLen) {
  /*
   * the length of index1,
   * 1. 1 byte => too many sub-indexes under each index1
   * 2. 8 byte => a lot more gaps compared with 1 byte index1
   * !!! more bytes with less gap is preferred, in other words,
   * prefer smaller gap-ratio, smaller compress_ratio
   * best case is : w1 * gap_ratio + w2 * compress_ratio
   *               (smaller is better)
   *        gap_ratio = (diff - numkeys) / diff,
   *   compress_ratio = index_mem_size / original,
   *
   * to calculate space usage, assume 1000 keys, maxKeyLen = 16,
   *   original cost = 16,000 * 8                     = 128,000 bit
   *   8 bytes, 0.5 gap => 2000 + (16 - 8) * 8 * 1000 =  66,000
   *   2 bytes, 0.5 gap => 2000 + (16 - 2) * 8 * 1000 = 114,000
   */
  const double w1 = 0.1;
  const double w2 = 1.2;
  const double min_gap_ratio = 0.1;
  const double max_gap_ratio = 0.9;
  const double fewone_min_gap_ratio = 0.990; // fewone 0.010, 1 '1' out of 100
  const double fewone_max_gap_ratio = 0.995; // fewone 0.005, 1 '1' out of 200
  const size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
  const size_t maxLen = std::min<size_t>(8, ks.maxKeyLen - cplen);
  const double originCost = ks.numKeys * ks.maxKeyLen * 8;
  double score = INT_MAX;
  double minCost = originCost;
  size_t scoreLen = maxLen;
  size_t minCostLen = maxLen;
  ceLen = maxLen;
  for (size_t i = maxLen; i > 0; i--) {
    auto minValue = ReadBigEndianUint64(ks.minKey.begin() + cplen, i);
    auto maxValue = ReadBigEndianUint64(ks.maxKey.begin() + cplen, i);
    uint64_t diff1st = abs_diff(minValue, maxValue); // don't +1, which may cause overflow
    uint64_t diff2nd = ks.numKeys;
    // one index1st with a collection of index2nd, that's when diff < numkeys
    double gap_ratio = diff1st <= ks.numKeys ? min_gap_ratio :
      (double)(diff1st - ks.numKeys) / diff1st;
    if (g_indexEnableFewZero &&
        fewone_min_gap_ratio <= gap_ratio &&
        gap_ratio < fewone_max_gap_ratio) { // fewone branch
      // to construct rankselect for fewone, much more extra space is needed
      size_t bits = (diff1st < UINT32_MAX && ks.numKeys < UINT32_MAX) ? 32 : 64;
      double cost = ks.numKeys * bits + diff2nd * 1.2 +
        (ks.maxKeyLen - cplen - i) * ks.numKeys * 8;
      if (cost > originCost * 0.8)
        continue;
      if (cost < minCost) {
        minCost = cost;
        minCostLen = i;
      }
    } else if (gap_ratio > max_gap_ratio) { // skip branch
      continue;
    } else {
      gap_ratio = std::max(gap_ratio, min_gap_ratio);
      // diff is bitmap, * 1.2 is extra cost to build RankSelect
      double cost = ((double)diff1st + diff2nd) * 1.2 +
        (ks.maxKeyLen - cplen - i) * ks.numKeys * 8;
      if (cost > originCost * 0.8)
        continue;
      double compress_ratio = cost / originCost;
      double cur = w1 * gap_ratio + w2 * compress_ratio;
      if (cur < score) {
        score = cur;
        scoreLen = i;
      }
    }
  }

  if (minCost <= originCost * 0.5) {
    ceLen = (score < INT_MAX) ? scoreLen : minCostLen;
    return true;
  } else {
    return false;
  }
}

const COIndex::Factory*
COIndex::SelectFactory(const KeyStat& ks, fstring name) {
  assert(ks.numKeys > 0);
  size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
  size_t ceLen = 0; // cost effective index1st len if any
  if (ks.enableUintIndex &&
      ks.maxKeyLen == ks.minKeyLen &&
      ks.maxKeyLen - cplen <= sizeof(uint64_t)) {
    auto minValue = ReadBigEndianUint64(ks.minKey.begin() + cplen, ks.minKey.end());
    auto maxValue = ReadBigEndianUint64(ks.maxKey.begin() + cplen, ks.maxKey.end());
    if (minValue > maxValue) {
      std::swap(minValue, maxValue);
    }
    uint64_t diff = maxValue - minValue; // don't +1, may overflow
    if (diff < ks.numKeys * 30) {
      if (diff + 1 == ks.numKeys) {
        return GetFactory("UintIndex_AllOne");
      }
      else if (diff < UINT32_MAX) {
        return GetFactory("UintIndex_IL_256_32");
      }
      else {
        return GetFactory("UintIndex_SE_512_64");
      }
    }
  }
  if (ks.enableCompositeUintIndex &&
      false && // disable now
      ks.maxKeyLen == ks.minKeyLen &&
      ks.maxKeyLen - cplen <= 16 && // !!! plain index2nd may occupy too much space
      SeekCostEffectiveIndexLen(ks, ceLen) &&
      ks.maxKeyLen > cplen + ceLen) {
    auto minValue = ReadBigEndianUint64(ks.minKey.begin() + cplen, ceLen);
    auto maxValue = ReadBigEndianUint64(ks.maxKey.begin() + cplen, ceLen);
    if (minValue > maxValue) {
      std::swap(minValue, maxValue);
    }
    if (maxValue - minValue < UINT32_MAX-1 &&
        ks.numKeys < UINT32_MAX-1) { // for composite cluster key, key1:key2 maybe 1:N
      return GetFactory("CompositeUintIndex_IL_256_32_IL_256_32");
    } else {
      return GetFactory("CompositeUintIndex_SE_512_64_SE_512_64");
    }
  }
 #if 0
  // NLT can compress much better, in tpcc, NLT compress bmsql_oorder_idx1 to
  // 6.1%, FixedLenHoleIndex compress to 33%, and NLT speed is ~50% of
  // FixedLenHoleIndex, iter scan 55ns vs 26ns per key, this is acceptable, so
  // FixedLenHoleIndex should not be selected here. We disable this code to
  // postpone the index selection after NLT build, if NLT compression ratio
  // is bad, then select FixedLenHoleIndex or FixedLenKeyIndex.
  if (ks.maxKeyLen == ks.minKeyLen && ks.minKeyLen - cplen - ks.holeLen <= 8) {
    if (ks.holeLen)
      return GetFactory("FixedLenHoleIndex");
    else
      return GetFactory("FixedLenKeyIndex");
  }
 #endif
  if (ks.sumKeyLen - ks.numKeys * cplen > 0x1E0000000) { // 7.5G
    return GetFactory("SE_512_64");
  }
  return GetFactory(name);
}

COIndex::~COIndex() {}
void COIndex::Delete() { delete this; }
void COIndexDel::operator()(COIndex* ptr) const {
  ptr->Delete();
}

COIndex::Factory::~Factory() {}
COIndex::Iterator::~Iterator() {}
void COIndex::Iterator::Delete() { delete this; }

void MmapRead_FixedLenStrVec(fstring fname, const COIndex::KeyStat& ks,
                             FixedLenStrVec& keyVec) {
  const size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
  MmapWholeFile mmap(fname, false, true); // readonly populate
  TERARK_VERIFY_EQ(mmap.size, ks.sumKeyLen);
  TERARK_VERIFY_EQ(mmap.size, ks.minKeyLen * ks.numKeys);
  keyVec.m_size = ks.numKeys;
  if (0 == cplen && ks.minKey < ks.maxKey) {
    keyVec.m_fixlen = uint32_t(ks.minKeyLen);
    keyVec.m_strpool_mem_type = MemType::Mmap;
    keyVec.m_strpool.risk_set_data((byte_t*)mmap.base, mmap.size);
    mmap.base = nullptr; // release
  }
  else {
    const size_t keynum = ks.numKeys;
    const size_t fixdst = keyVec.m_fixlen = uint32_t(ks.minKeyLen - cplen);
    const size_t fixsrc = ks.minKeyLen;
    use_hugepage_resize_no_init(&keyVec.m_strpool, fixdst * ks.numKeys);
    auto src = (const byte_t*)mmap.base + cplen;
    if (ks.maxKey < ks.minKey) { // is reverse
      auto dst = keyVec.m_strpool.end();
      for (size_t i = 0; i < keynum; ++i) {
        dst -= fixdst;
        for (size_t j = 0; j < fixdst; ++j) dst[j] = src[j];
        src += fixsrc;
      }
    }
    else {
      auto dst = keyVec.m_strpool.begin();
      for (size_t i = 0; i < keynum; ++i) {
        for (size_t j = 0; j < fixdst; ++j) dst[j] = src[j];
        src += fixsrc;
        dst += fixdst;
      }
    }
  }
  keyVec.optimize_func();
}

void MmapRead_ReadSortedStrVec(fstring fname, const COIndex::KeyStat& ks,
                               SortedStrVec& keyVec) {
  TERARK_VERIFY_EQ(ks.keyOffsets.size(), ks.numKeys + 1);
  const size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
  MmapWholeFile mmap(fname, false, true); // readonly populate
  if (0 == cplen && ks.minKey < ks.maxKey) {
    keyVec.m_offsets_mem_type = MemType::User;
    keyVec.m_offsets.clear();
    keyVec.m_offsets.risk_set_data((byte_t*)ks.keyOffsets.data(),
                                   ks.keyOffsets.size(),
                                   ks.keyOffsets.uintbits());
    keyVec.m_strpool_mem_type = MemType::Mmap;
    keyVec.m_strpool.risk_set_data((byte_t*)mmap.base, mmap.size);
    mmap.base = nullptr; // release
  }
  else {
    const size_t keynum = ks.numKeys;
    const size_t vecSumKeyLen = ks.sumKeyLen - (cplen * keynum);
    use_hugepage_resize_no_init(&keyVec.m_strpool, vecSumKeyLen);
    UintVecMin0 offsets(keynum + 1, vecSumKeyLen);
    auto odata = ks.keyOffsets.data();
    auto omask = ks.keyOffsets.uintmask();
    auto obits = ks.keyOffsets.uintbits();
    auto dst_base = keyVec.m_strpool.data();
    auto src_base = (const byte_t*)mmap.base;
    auto src_off = size_t(0);
    if (ks.maxKey < ks.minKey) { // is reverse
      offsets.set_wire(keynum, vecSumKeyLen);
      auto dst_off = vecSumKeyLen;
      for (size_t i = 0; i < keynum; ++i) {
        size_t src_end = UintVecMin0::fast_get(odata, obits, omask, i+1);
        size_t dst_len = src_end - src_off - cplen;
        dst_off -= dst_len;
        auto src = src_base + src_off + cplen;
        auto dst = dst_base + dst_off;
        for (size_t j = 0; j < dst_len; ++j) dst[j] = src[j];
        src_off = src_end;
        offsets.set_wire(keynum - i - 1, dst_off);
      }
      TERARK_VERIFY_EZ(dst_off);
      TERARK_VERIFY_EQ(src_off, ks.sumKeyLen);
    }
    else {
      // forward & cplen != 0
      auto dst_off = size_t(0);
      for (size_t i = 0; i < keynum; ++i) {
        offsets.set_wire(i, dst_off);
        size_t src_end = UintVecMin0::fast_get(odata, obits, omask, i+1);
        size_t dst_len = src_end - src_off - cplen;
        auto src = src_base + src_off + cplen;
        auto dst = dst_base + dst_off;
        for (size_t j = 0; j < dst_len; ++j) dst[j] = src[j];
        src_off  = src_end;
        dst_off += dst_len;
      }
      offsets.set_wire(keynum, dst_off);
      TERARK_VERIFY_EQ(dst_off, vecSumKeyLen);
      TERARK_VERIFY_EQ(src_off, ks.sumKeyLen);
    }
    keyVec.m_offsets.swap(offsets);
  }
}

COIndexUP COIndex::LoadFile(fstring fpath) {
  COIndex::Factory* factory = NULL;
  {
    MmapWholeFile mmap(fpath);
    auto header = (const ToplingIndexHeader*)mmap.base;
    size_t idx = g_COIndexFactroy.find_i(header->class_name);
    if (idx >= g_COIndexFactroy.end_i()) {
      throw std::invalid_argument(
          "COIndex::LoadFile(" + fpath + "): Unknown class: "
          + header->class_name);
    }
    factory = g_COIndexFactroy.val(idx).get();
  }
  return factory->LoadFile(fpath);
}

COIndexUP COIndex::LoadMemory(fstring mem) {
  auto header = (const ToplingIndexHeader*)mem.data();
  size_t idx = g_COIndexFactroy.find_i(header->class_name);
  if (idx >= g_COIndexFactroy.end_i()) {
    throw std::invalid_argument(
        std::string("COIndex::LoadMemory(): Unknown class: ")
        + header->class_name);
  }
  COIndex::Factory* factory = g_COIndexFactroy.val(idx).get();
  return factory->LoadMemory(mem);
}

static void
VerifyDictRankNeighbor(fstring key, size_t nth, const COIndex* index) {
  std::string keybuf = key.str();
#define VERIFY_RANK(OP, ArgNth) TERARK_VERIFY_F(index->AccurateRank(keybuf) OP ArgNth, \
  "%zd %zd : nth = %zd : %s", index->AccurateRank(keybuf), ArgNth, nth, index->Name())
  if (!keybuf.empty() && byte_t(keybuf.back()) > 0) {
    keybuf.back()--;
    VERIFY_RANK(<=, nth); // AccurateRank semantic is lower_bound
    keybuf.back()++;
  }
  if (!keybuf.empty() && byte_t(keybuf.back()) < 255) {
    keybuf.back()++;
    VERIFY_RANK(>=, nth+1);
    keybuf.back()--;
  }
  keybuf.push_back('\0');
  VERIFY_RANK(==, nth+1);
  keybuf.pop_back();
  if (!keybuf.empty()) {
    keybuf.pop_back();
    // AccurateRank semantic is lower_bound
    TERARK_VERIFY_LE(index->AccurateRank(keybuf), nth);
  }
}

void COIndex::VerifyDictRank() const {
  Iterator* iter = this->NewIterator();
  size_t nth = 0;
  bool hasNext = iter->SeekToFirst();
  TERARK_VERIFY_EQ(this->AccurateRank(""), 0);
  while (hasNext) {
    TERARK_VERIFY_EQ(nth, iter->DictRank());
    TERARK_VERIFY_EQ(nth, this->AccurateRank(iter->key()));
    VerifyDictRankNeighbor(iter->key(), nth, this);
    hasNext = iter->Next();
    nth++;
  }
  TERARK_VERIFY_EQ(nth, this->NumKeys());
  iter->Delete();
}

bool COIndex::HasFastApproximateRank() const noexcept {
  return true;
}

fstring COIndex::ResidentMemory() const noexcept {
  return Memory();
}

#if TOPLING_USE_BOUND_PMF
COIndex::FastCallFindFunc COIndex::GetFastCallFindFunc() const {
  FastCallFindFunc f;
  f.m_obj = this;
  f.m_func = terark::ExtractFuncPtr<FindFunc>(this, &COIndex::Find);
  return f;
}
#endif

} // namespace rocksdb
