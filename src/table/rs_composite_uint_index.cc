#include "co_index.h"
#include "top_zip_table.h"
#include <table/top_table_common.h>
#include <terark/succinct/rank_select_few.hpp>
#include <terark/succinct/rank_select_il_256.hpp>
#include <terark/succinct/rank_select_mixed_se_512.hpp>
#include <terark/succinct/rank_select_mixed_xl_256.hpp>
#include <terark/succinct/rank_select_se_256.hpp>
#include <terark/succinct/rank_select_se_512.hpp>
#include <terark/succinct/rank_select_simple.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/sorted_uint_vec.hpp>
#include <terark/util/throw.hpp>
#include <terark/num_to_str.hpp>

#include "top_zip_internal.h"

#include <topling/json.h>

#include "rs_index_common.h"

#if defined(_MSC_VER)
  #pragma warning(disable: 4458) // index_ hide class member(intentional)
  #pragma warning(disable: 4702) // unreachable code
#endif

namespace rocksdb {
using namespace terark;

static bool g_indexEnableSortedUint = getEnvBool("ToplingZipTable_enableSortedUint", false);
static bool g_indexEnableBigUint0   = getEnvBool("ToplingZipTable_enableBigUint0", false);

bool IsFewZero(size_t total, size_t cnt0);
bool IsFewOne(size_t total, size_t cnt1);
void VerifyClassName(const char* class_name, const std::type_info& ti);

template<class Uint>
struct oldname_rank_select_fewzero : terark::rank_select_fewzero<sizeof(Uint)> {
    typedef Uint index_t;
    typedef terark::rank_select_fewzero<sizeof(Uint)> super;
    using super::super;
    //std::unique_ptr<rank_select_few_builder<0, sizeof(Uint)> > m_builder;
    std::unique_ptr<terark::rank_select_il> m_temp;
    explicit oldname_rank_select_fewzero(size_t=0) {}
    template<class SrcRS>
    void build_from(const SrcRS& rs) {
      rank_select_few_builder<0, sizeof(Uint)> builder(rs.max_rank0(), rs.max_rank1(), false);
      for (size_t i = 0, n = rs.size(); i < n; ++i) {
        if (rs.is0(i))
          builder.insert(i), ++i;
        else
          i += rs.one_seq_len(i);
      }
      builder.finish(this);
    }
    void set0(size_t i) {
      if (terark_unlikely(!m_temp)) {
        m_temp.reset(new terark::rank_select_il());
      }
      if (m_temp->size() <= i) {
        m_temp->resize_fill(i+1, 1);
      }
      m_temp->set0(i);
    }
    void set1(size_t i) {
      if (terark_unlikely(!m_temp)) {
        m_temp.reset(new terark::rank_select_il());
      }
      if (m_temp->size() <= i) {
        m_temp->resize_fill(i+1, 1);
      }
      m_temp->set1(i);
    }
    void build_cache(bool, bool) {
      build_from(*m_temp);
      m_temp.reset();
    }
    bool isall0() const { return this->max_rank1() == 0; }
    bool isall1() const { return this->max_rank0() == 0; }
};
template<class Uint>
struct oldname_rank_select_fewone : terark::rank_select_fewone<sizeof(Uint)> {
    typedef Uint index_t;
    typedef terark::rank_select_fewone<sizeof(Uint)> super;
    using super::super;
    std::unique_ptr<terark::rank_select_il> m_temp;
    explicit oldname_rank_select_fewone(size_t=0) {}
    template<class SrcRS>
    void build_from(const SrcRS& rs) {
      rank_select_few_builder<1, sizeof(Uint)> builder(rs.max_rank0(), rs.max_rank1(), false);
      for (size_t i = 0, n = rs.size(); i < n; ++i) {
        if (rs.is1(i))
          builder.insert(i), ++i;
        else
          i += rs.zero_seq_len(i);
      }
      builder.finish(this);
    }
    void set0(size_t i) {
      if (terark_unlikely(!m_temp)) {
        m_temp.reset(new terark::rank_select_il());
      }
      if (m_temp->size() <= i) {
        m_temp->resize_fill(i+1, 0);
      }
      m_temp->set0(i);
    }
    void set1(size_t i) {
      if (terark_unlikely(!m_temp)) {
        m_temp.reset(new terark::rank_select_il());
      }
      if (m_temp->size() <= i) {
        m_temp->resize_fill(i+1, 0);
      }
      m_temp->set1(i);
    }
    void build_cache(bool, bool) {
      build_from(*m_temp);
      m_temp.reset();
    }
    bool isall0() const { return this->max_rank1() == 0; }
    bool isall1() const { return this->max_rank0() == 0; }
};

#define rank_select_fewzero oldname_rank_select_fewzero
#define rank_select_fewone  oldname_rank_select_fewone

typedef rank_select_fewzero<uint32_t> rs_fewzero_32;
typedef rank_select_fewzero<uint64_t> rs_fewzero_64;
typedef rank_select_fewone<uint32_t> rs_fewone_32;
typedef rank_select_fewone<uint64_t> rs_fewone_64;

// -- fast zero-seq-len
template<class RankSelect>
size_t fast_zero_seq_len(const RankSelect& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_len(pos);
}
template<class Uint>
size_t
fast_zero_seq_len(const rank_select_fewzero<Uint>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_len(pos, hint);
}
template<class Uint>
size_t
fast_zero_seq_len(const rank_select_fewone<Uint>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_len(pos, hint);
}
// -- fast zero-seq-revlen
template<class RankSelect>
size_t fast_zero_seq_revlen(const RankSelect& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_revlen(pos);
}
template<class Uint>
size_t
fast_zero_seq_revlen(const rank_select_fewzero<Uint>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_revlen(pos, hint);
}
template<class Uint>
size_t
fast_zero_seq_revlen(const rank_select_fewone<Uint>& rs, size_t pos, size_t& hint) {
  return rs.zero_seq_revlen(pos, hint);
}

class SortedUintDataCont {
private:
  size_t get_val(size_t idx) const {
    return container_[idx] + min_value_;
  }
  uint64_t to_uint64(fstring val) const {
    byte_t targetBuffer[8] = { 0 };
    memcpy(targetBuffer + (8 - key_len_), val.data(),
           std::min(key_len_, val.size()));
    return ReadBigEndianUint64(targetBuffer, 8);
  }
public:
  void swap(SortedUintDataCont& other) {
    container_.swap(other.container_);
    std::swap(min_value_, other.min_value_);
    std::swap(key_len_, other.key_len_);
  }
  void swap(SortedUintVec& other) {
    container_.swap(other);
  }
  void set_min_value(uint64_t min_value) {
    min_value_ = min_value;
  }
  void set_key_len(size_t key_len) {
    key_len_ = key_len;
  }
  void set_size(size_t sz) { TERARK_DIE("Unexpected"); }
  void risk_release_ownership() {
    container_.risk_release_ownership();
  }
  const byte_t* data() const { return container_.data(); }
  size_t mem_size() const { return container_.mem_size(); }
  size_t size() const { return container_.size(); }
  bool equals(size_t idx, fstring val) const {
    assert(idx < container_.size());
    if (val.size() != key_len_)
      return false;
    uint64_t n = to_uint64(val);
    return get_val(idx) == n;
  }
  void risk_set_data(byte_t* data, size_t num, size_t maxValue) {
    TERARK_DIE("Unexpected");
  }
  void risk_set_data(byte_t* data, size_t sz) {
    assert(data != nullptr);
    container_.risk_set_data(data, sz);
  }

  void copy_to(size_t idx, byte_t* data) const {
    assert(idx < container_.size());
    size_t v = get_val(idx);
    SaveAsBigEndianUint64(data, key_len_, v);
  }
  int compare(size_t idx, fstring another) const {
    assert(idx < container_.size());
    byte_t arr[8] = { 0 };
    copy_to(idx, arr);
    fstring me(arr, arr + key_len_);
    return fstring_func::compare3()(me, another);
  }
  /*
   * 1. m_len == 8,
   * 2. m_len < 8, should align
   */
  size_t lower_bound(size_t lo, size_t hi, fstring val) const {
    uint64_t n = to_uint64(val);
    if (n < min_value_) // if equal, val len may > 8
      return lo;
    n -= min_value_;
    size_t pos = container_.lower_bound(lo, hi, n);
    while (pos != hi) {
      if (compare(pos, val) >= 0)
        return pos;
      pos++;
    }
    return pos;
  }


private:
  SortedUintVec container_;
  uint64_t min_value_;
  size_t key_len_;
};

class Min0DataCont {
private:
  size_t get_val(size_t idx) const {
    return container_[idx] + min_value_;
  }
  uint64_t to_uint64(fstring val) const {
    byte_t targetBuffer[8] = { 0 };
    memcpy(targetBuffer + (8 - key_len_), val.data(),
           std::min(key_len_, val.size()));
    return ReadBigEndianUint64(targetBuffer, 8);
  }
public:
  void swap(Min0DataCont& other) {
    container_.swap(other.container_);
    std::swap(min_value_, other.min_value_);
    std::swap(key_len_, other.key_len_);
  }
  void swap(BigUintVecMin0& other) {
    container_.swap(other);
  }
  void set_min_value(uint64_t min_value) {
    min_value_ = min_value;
  }
  void set_key_len(size_t key_len) {
    key_len_ = key_len;
  }
  void set_size(size_t sz) { TERARK_DIE("Unexpected"); }
  void risk_release_ownership() {
    container_.risk_release_ownership();
  }
  const byte_t* data() const { return container_.data(); }
  size_t mem_size() const { return container_.mem_size(); }
  size_t size() const { return container_.size(); }
  bool equals(size_t idx, fstring val) const {
    assert(idx < container_.size());
    if (val.size() != key_len_)
      return false;
    uint64_t n = to_uint64(val);
    return get_val(idx) == n;
  }
  void risk_set_data(byte_t* data, size_t num, size_t maxValue) {
    size_t bits = BigUintVecMin0::compute_uintbits(maxValue);
    container_.risk_set_data(data, num, bits);
  }
  void risk_set_data(byte_t* data, size_t sz) {
    TERARK_DIE("Unexpected");
  }

  void copy_to(size_t idx, byte_t* data) const {
    assert(idx < container_.size());
    size_t v = get_val(idx);
    SaveAsBigEndianUint64(data, key_len_, v);
  }
  int compare(size_t idx, fstring another) const {
    assert(idx < container_.size());
    byte_t arr[8] = { 0 };
    copy_to(idx, arr);
    fstring me(arr, arr + key_len_);
    return fstring_func::compare3()(me, another);
  }
  size_t lower_bound(size_t lo, size_t hi, fstring val) const {
    uint64_t n = to_uint64(val);
    if (n < min_value_) // if equal, val len may > key_len_
      return lo;
    n -= min_value_;
    size_t pos = lower_bound_n<const BigUintVecMin0&>(container_, lo, hi, n);
    while (pos != hi) {
      if (compare(pos, val) >= 0)
        return pos;
      pos++;
    }
    return pos;
  }

private:
  BigUintVecMin0 container_;
  uint64_t min_value_;
  size_t key_len_;
};

class StrDataCont {
public:
  void swap(StrDataCont& other) {
    container_.swap(other.container_);
  }
  void swap(FixedLenStrVec& other) {
    container_.swap(other);
  }
  void risk_release_ownership() {
    container_.risk_release_ownership();
  }
  void set_key_len(size_t len) {
    container_.m_fixlen = uint32_t(len);
  }
  void set_min_value(uint64_t min_value) { TERARK_DIE("Unexpected"); }
  void set_size(size_t sz) {
    container_.m_size = sz;
  }
  const byte_t* data() const { return container_.data(); }
  size_t mem_size() const { return container_.mem_size(); }
  size_t size() const { return container_.size(); }
  bool equals(size_t idx, fstring other) const {
    return container_[idx] == other;
  }
  void risk_set_data(byte_t* data, size_t sz) {
    assert(data != nullptr);
    // here, sz == count, since <byte_t>
    container_.m_strpool.risk_set_data(data, sz);
  }
  void risk_set_data(byte_t* data, size_t num, size_t maxValue) {
    TERARK_DIE("Unexpected");
  }

  void copy_to(size_t idx, byte_t* data) const {
    assert(idx < container_.size());
    memcpy(data, container_[idx].data(), container_[idx].size());
  }
  int compare(size_t idx, fstring another) const {
    assert(idx < container_.size());
    return fstring_func::compare3()(container_[idx], another);
  }
  size_t lower_bound(size_t lo, size_t hi, fstring val) const {
    return container_.lower_bound(lo, hi, val);
  }

private:
  FixedLenStrVec container_;
};

void TIH_set_class_name(ToplingIndexHeader*, const std::type_info&);

struct CompositeUintIndexBase : public COIndex {
  static const char index_name[15];
  struct MyBaseFileHeader : public ToplingIndexHeader {
    uint64_t key1_min_value;
    uint64_t key1_max_value;
    uint64_t rankselect1_mem_size;
    uint64_t rankselect2_mem_size;
    uint64_t key2_data_mem_size;
    /*
     * per key length = common_prefix_len + key1_fixed_len + key2_fixed_len
     */
    uint32_t key1_fixed_len;
    uint32_t key2_fixed_len;

    uint32_t common_prefix_length;
    uint32_t reserved32;
    uint64_t key2_min_value; // old reserved64
    /*
     * For backward compatibility.
     * do NOT use reserved_102_24 any more
     */
#define key2_max_value reserved_102_24

    MyBaseFileHeader(size_t body_size, const std::type_info& ti) {
      memset(this, 0, sizeof *this);
      magic_len = strlen(index_name);
      strncpy(magic, index_name, sizeof(magic));
      TIH_set_class_name(this, ti);
      header_size = sizeof *this;
      version = 1;
      file_size = sizeof *this + body_size;
    }
  };
  enum ContainerUsedT {
    kFixedLenStr = 0,
    kUintMin0,
    kSortedUint
  };
  // handlers
  static uint64_t Read1stKey(fstring key, size_t cplen, size_t key1_len) {
    return ReadBigEndianUint64((const byte_t*)key.data() + cplen, key1_len);
  }
  static uint64_t Read1stKey(const byte_t* key, size_t cplen, size_t key1_len) {
    return ReadBigEndianUint64(key + cplen, key1_len);
  }
  class MyBaseFactory : public COIndex::Factory {
  public:
    // composite index as cluster index
    // secondary index which contain 'id' as key2
    enum AmazingCombinationT {
      kAllOne_AllZero = 0, // secondary index no gap
      kAllOne_FewZero,     // c-cluster index
      kAllOne_FewOne,
      kAllOne_Normal,      // c-cluster index
      kFewZero_AllZero = 4,// secondary index with gap
      kFewZero_FewZero,    //
      kFewZero_FewOne,
      kFewZero_Normal,
      kFewOne_AllZero = 8, // secondary index with lots of gap
      kFewOne_FewZero,     // c-cluster index with lots of gap
      kFewOne_FewOne,
      kFewOne_Normal,      // c-cluster index with lots of gap
      kNormal_AllZero = 12,
      kNormal_FewZero,
      kNormal_FewOne,
      kNormal_Normal
    };
    size_t MemSizeForBuild(const KeyStat& ks) const override {
      ROCKSDB_VERIFY_EQ(ks.minKeyLen, ks.maxKeyLen);
      size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
      size_t key1_len = 0;
      bool check = SeekCostEffectiveIndexLen(ks, key1_len);
      ROCKSDB_VERIFY_GT(ks.maxKeyLen, cplen + key1_len);
      if (!check) {
        STD_WARN("Bad index choice, reclen = %zd, cplen = %zd, key1_len = %zd",
                 ks.minKeyLen, cplen, key1_len);
      }
      uint64_t minValue = Read1stKey(ks.minKey, cplen, key1_len);
      uint64_t maxValue = Read1stKey(ks.maxKey, cplen, key1_len);
      if (minValue > maxValue) {
        std::swap(minValue, maxValue);
      }
      uint64_t diff = maxValue - minValue + 1;
      size_t key2_len = ks.minKey.size() - cplen - key1_len;
      // maximum
      size_t rankselect_1st_sz = size_t(std::ceil(diff * 1.25 / 8));
      size_t rankselect_2nd_sz = size_t(std::ceil(ks.numKeys * 1.25 / 8));
      size_t sum_key2_sz = std::ceil(ks.numKeys * key2_len) * 1.25; // sorteduint as the extra cost
      return rankselect_1st_sz + rankselect_2nd_sz + sum_key2_sz;
    }
    static void
    updateMinMax(fstring data, valvec<byte_t>& minData, valvec<byte_t>& maxData) {
      if (minData.empty() || data < fstring(minData.begin(), minData.size())) {
        minData.assign(data.data(), data.size());
      }
      if (maxData.empty() || data > fstring(maxData.begin(), maxData.size())) {
        maxData.assign(data.data(), data.size());
      }
    }
    template<class RankSelect1, class RankSelect2>
    static AmazingCombinationT
    figureCombination(const RankSelect1& rs1, const RankSelect2& rs2) {
      bool isRS1FewZero = IsFewZero(rs1.size(), rs1.max_rank0());
      bool isRS2FewZero = IsFewZero(rs2.size(), rs2.max_rank0());
      bool isRS1FewOne  = IsFewOne(rs1.size(), rs1.max_rank1());
      bool isRS2FewOne  = IsFewOne(rs2.size(), rs2.max_rank1());
      // all one && ...
      if (rs1.isall1() && rs2.isall0())
        return kAllOne_AllZero;
      else if (rs1.isall1() && isRS2FewZero)
        return kAllOne_FewZero;
      else if (rs1.isall1() && isRS2FewOne)
        return kAllOne_FewOne;
      else if (rs1.isall1())
        return kAllOne_Normal;
      // few zero && ...
      else if (isRS1FewZero && rs2.isall0())
        return kFewZero_AllZero;
      else if (isRS1FewZero && isRS2FewZero)
        return kFewZero_FewZero;
      else if (isRS1FewZero && isRS2FewOne)
        return kFewZero_FewOne;
      else if (isRS1FewZero)
        return kFewZero_Normal;
      // few one && ...
      else if (isRS1FewOne && rs2.isall0())
        return kFewOne_AllZero;
      else if (isRS1FewOne && isRS2FewZero)
        return kFewOne_FewZero;
      else if (isRS1FewOne && isRS2FewOne)
        return kFewOne_FewOne;
      else if (isRS1FewOne)
        return kFewOne_Normal;
      // normal && ...
      else if (rs2.isall0())
        return kNormal_AllZero;
      else if (isRS2FewZero)
        return kNormal_FewZero;
      else if (isRS2FewOne)
        return kNormal_FewOne;
      else
        return kNormal_Normal;
    }
    template<class RankSelect1, class RankSelect2>
    static COIndex*
    CreateIndexWithSortedUintCont(RankSelect1& rankselect1,
                                  RankSelect2& rankselect2,
                                  FixedLenStrVec& keyVec,
                                  const KeyStat& ks,
                                  uint64_t key1MinValue,
                                  uint64_t key1MaxValue,
                                  size_t key1_len,
                                  valvec<byte_t>& minKey2Data,
                                  valvec<byte_t>& maxKey2Data) {
      const size_t kBlockUnits = 128;
      const size_t kLimit = (1ull << 48) - 1;
      uint64_t key2MinValue = ReadBigEndianUint64(minKey2Data);
      uint64_t key2MaxValue = ReadBigEndianUint64(maxKey2Data);
      //if (key2MinValue == key2MaxValue) // MyRock UT will fail on this condition
      //  return nullptr;
      unique_ptr<SortedUintVec::Builder> builder(SortedUintVec::createBuilder(false, kBlockUnits));
      uint64_t prev = ReadBigEndianUint64(keyVec[0]) - key2MinValue;
      builder->push_back(prev);
      for (size_t i = 1; i < keyVec.size(); i++) {
        fstring str = keyVec[i];
        uint64_t key2 = ReadBigEndianUint64(str) - key2MinValue;
        if (terark_unlikely(abs_diff(key2, prev) > kLimit)) // should not use sorted uint vec
          return nullptr;
        builder->push_back(key2);
        prev = key2;
      }
      SortedUintVec uintVec;
      auto rs = builder->finish(&uintVec);
      if (rs.mem_size > keyVec.mem_size() / 2.0) // too much ram consumed
        return nullptr;
      SortedUintDataCont container;
      container.swap(uintVec);
      //container.init(minKey2Data.size(), key2MinValue);
      container.set_key_len(minKey2Data.size());
      container.set_min_value(key2MinValue);
      return CreateIndex(rankselect1, rankselect2, container, ks,
                         key1MinValue, key1MaxValue,
                         key1_len, key2MinValue, key2MaxValue);
    }
    template<class RankSelect1, class RankSelect2>
    static COIndex*
    CreateIndexWithUintCont(RankSelect1& rankselect1,
                            RankSelect2& rankselect2,
                            FixedLenStrVec& keyVec,
                            const KeyStat& ks,
                            uint64_t key1MinValue,
                            uint64_t key1MaxValue,
                            size_t key1_len,
                            valvec<byte_t>& minKey2Data,
                            valvec<byte_t>& maxKey2Data) {
      uint64_t key2MinValue = ReadBigEndianUint64(minKey2Data);
      uint64_t key2MaxValue = ReadBigEndianUint64(maxKey2Data);
      uint64_t diff = key2MaxValue - key2MinValue + 1;
      size_t bitUsed = BigUintVecMin0::compute_uintbits(diff);
      if (bitUsed > keyVec.m_fixlen * 8 * 0.9) // compress ratio just so so
        return nullptr;
      // reuse memory from keyvec, since vecMin0 should consume less mem
      // compared with fixedlenvec
      BigUintVecMin0 vecMin0;
      vecMin0.risk_set_data(const_cast<byte_t*>(keyVec.data()), keyVec.size(), bitUsed);
      for (size_t i = 0; i < keyVec.size(); i++) {
        fstring str = keyVec[i];
        uint64_t key2 = ReadBigEndianUint64(str);
        vecMin0.set_wire(i, key2 - key2MinValue);
      }
      keyVec.risk_release_ownership();
      Min0DataCont container;
      container.swap(vecMin0);
      //container.init(minKey2Data.size(), key2MinValue);
      container.set_key_len(minKey2Data.size());
      container.set_min_value(key2MinValue);
      return CreateIndex(rankselect1, rankselect2, container, ks,
                         key1MinValue, key1MaxValue,
                         key1_len, key2MinValue, key2MaxValue);
    }
    template<class RankSelect1, class RankSelect2>
    static COIndex*
    CreateIndexWithStrCont(RankSelect1& rankselect1,
                           RankSelect2& rankselect2,
                           FixedLenStrVec& keyVec,
                           const KeyStat& ks,
                           uint64_t key1MinValue,
                           uint64_t key1MaxValue,
                           size_t key1_len) {
      StrDataCont container;
      container.swap(keyVec);
      return CreateIndex(rankselect1, rankselect2, container, ks,
                         key1MinValue, key1MaxValue, key1_len, 0, 0);
    }
    template<class RankSelect1, class RankSelect2, class DataCont>
    static COIndex*
    CreateIndex(RankSelect1& rankselect1,
                RankSelect2& rankselect2,
                DataCont& container, const KeyStat& ks,
                uint64_t key1MinValue, uint64_t key1MaxValue, size_t key1_len,
                uint64_t key2MinValue, uint64_t key2MaxValue);
    bool verifyBaseHeader(fstring mem) const {
      auto header = (const MyBaseFileHeader*)mem.data();
      using FormatError = std::length_error;
      TERARK_EXPECT_LT(sizeof(MyBaseFileHeader), mem.size(), FormatError);
      TERARK_EXPECT_EQ(sizeof(MyBaseFileHeader), header->header_size, FormatError);
      TERARK_EXPECT_EQ(header->magic_len, strlen(index_name), FormatError);
      TERARK_EXPECT_EQ(header->version  , 1         , FormatError);
      TERARK_EXPECT_EQ(align_up(header->file_size, 64), align_up(mem.size(), 64), FormatError);
      TERARK_EXPECT_LE(header->file_size, mem.size(), FormatError);
      TERARK_EXPECT_F(strcmp(header->magic, index_name) == 0, FormatError,
                    "%s %s", header->magic, index_name);
      return true;
    }

#define Disable_BuildImpl(rs1, rs2) \
    COIndex* BuildImpl(const byte_t*& reader, \
                           const ToplingZipTableOptions& tzopt, \
                           const KeyStat& ks, \
                           rs1*, rs2*, const ImmutableOptions*) \
    const { TERARK_DIE("Template instantiation is disabled"); return NULL; }

    template<class rs2> Disable_BuildImpl(rs_fewone_32, rs2);
    template<class rs2> Disable_BuildImpl(rs_fewone_64, rs2);
    template<class rs2> Disable_BuildImpl(rs_fewzero_32, rs2);
    template<class rs2> Disable_BuildImpl(rs_fewzero_64, rs2);
    template<class rs2> Disable_BuildImpl(rank_select_allone, rs2);
    //template<class rs2> Disable_BuildImpl(rank_select_allzero, rs2);

    template<class rs1> Disable_BuildImpl(rs1, rs_fewone_32);
    template<class rs1> Disable_BuildImpl(rs1, rs_fewone_64);
    template<class rs1> Disable_BuildImpl(rs1, rs_fewzero_32);
    template<class rs1> Disable_BuildImpl(rs1, rs_fewzero_64);
    //template<class rs1> Disable_BuildImpl(rs1, rank_select_allone);
    template<class rs1> Disable_BuildImpl(rs1, rank_select_allzero);

    Disable_BuildImpl(rs_fewone_32, rs_fewone_32);
    Disable_BuildImpl(rs_fewone_32, rs_fewzero_32);
    //Disable_BuildImpl(rs_fewone_32, rank_select_allone);
    Disable_BuildImpl(rs_fewone_32, rank_select_allzero);

    Disable_BuildImpl(rs_fewzero_32, rs_fewone_32);
    Disable_BuildImpl(rs_fewzero_32, rs_fewzero_32);
    //Disable_BuildImpl(rs_fewzero_32, rank_select_allone);
    Disable_BuildImpl(rs_fewzero_32, rank_select_allzero);

    Disable_BuildImpl(rs_fewone_64, rs_fewone_64);
    Disable_BuildImpl(rs_fewone_64, rs_fewzero_64);
    //Disable_BuildImpl(rs_fewone_64, rank_select_allone);
    Disable_BuildImpl(rs_fewone_64, rank_select_allzero);

    Disable_BuildImpl(rs_fewzero_64, rs_fewone_64);
    Disable_BuildImpl(rs_fewzero_64, rs_fewzero_64);
    //Disable_BuildImpl(rs_fewzero_64, rank_select_allone);
    Disable_BuildImpl(rs_fewzero_64, rank_select_allzero);

    Disable_BuildImpl(rank_select_allone, rs_fewone_32);
    Disable_BuildImpl(rank_select_allone, rs_fewzero_32);
    //Disable_BuildImpl(rank_select_allone, rank_select_allone);
    Disable_BuildImpl(rank_select_allone, rank_select_allzero);

    //Disable_BuildImpl(rank_select_allzero, rs_fewone_32);
    //Disable_BuildImpl(rank_select_allzero, rs_fewzero_32);
    //Disable_BuildImpl(rank_select_allzero, rank_select_allone);
    //Disable_BuildImpl(rank_select_allzero, rank_select_allzero);

    Disable_BuildImpl(rank_select_allone, rs_fewone_64);
    Disable_BuildImpl(rank_select_allone, rs_fewzero_64);

    //Disable_BuildImpl(rank_select_allzero, rs_fewone_64);
    //Disable_BuildImpl(rank_select_allzero, rs_fewzero_64);

    template<class RankSelect1, class RankSelect2>
    COIndex* BuildImpl(const byte_t*& reader,
                           const ToplingZipTableOptions& tzopt,
                           const KeyStat& ks,
                           RankSelect1*, RankSelect2*,
                           const ImmutableOptions* ioptions) const {
      ROCKSDB_VERIFY_EQ(ks.minKeyLen, ks.maxKeyLen);
      size_t reclen = ks.minKeyLen;
      size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
      TERARK_VERIFY_F(ks.maxKeyLen == ks.minKeyLen && ks.maxKeyLen > cplen + 1,
          "maxlen %zu, minlen %zu, cplen %zd\n", ks.maxKeyLen, ks.minKeyLen, cplen);

      size_t key1_len = 0;
      bool check = SeekCostEffectiveIndexLen(ks, key1_len);
      ROCKSDB_VERIFY_GT(ks.maxKeyLen, cplen + key1_len);
      if (!check) {
        STD_WARN("Bad index choice, reclen = %zd, cplen = %zd, key1_len = %zd",
                 reclen, cplen, key1_len);
      }
      ullong minValue = Read1stKey(ks.minKey, cplen, key1_len);
      ullong maxValue = Read1stKey(ks.maxKey, cplen, key1_len);
      // if rocksdb reverse comparator is used, then minValue
      // is actually the largest one
      if (minValue > maxValue) {
        std::swap(minValue, maxValue);
      }
      /*
       * 1stRS stores bitmap [minValue, maxValue] for index1st
       * 2ndRS stores bitmap for StaticMap<key1, list<key2>>
       * order of rs2 follows the order of index data, and order
       * of rs1 follows the order of rs2
       */
      RankSelect1 rankselect1(maxValue - minValue + 1);
      RankSelect2 rankselect2(ks.numKeys + 1); // append extra '0' at back
      valvec<byte_t> minKey2Data, maxKey2Data;
      uint64_t prev = size_t(-1);
      size_t key2_len = ks.maxKeyLen - cplen - key1_len;
      size_t sumKey2Len = key2_len * ks.numKeys;
      FixedLenStrVec keyVec(key2_len);
      keyVec.m_strpool.reserve(sumKey2Len + 8);
      if (ks.minKey < ks.maxKey) { // ascend
        for (size_t i = 0; i < ks.numKeys; ++i) {
          uint64_t offset = Read1stKey(reader, cplen, key1_len) - minValue;
          rankselect1.set1(offset);
          if (terark_unlikely(i == 0)) // make sure 1st prev != offset
            prev = offset - 1;
          if (offset != prev) { // new key1 encountered
            rankselect2.set0(i);
          } else {
            rankselect2.set1(i);
          }
          prev = offset;
          fstring key2Data(reader, cplen + key1_len);
          updateMinMax(key2Data, minKey2Data, maxKey2Data);
          keyVec.push_back(key2Data);
          reader += reclen;
        }
      } else { // descend, reverse comparator
        size_t pos = sumKey2Len;
        keyVec.m_size = ks.numKeys;
        keyVec.m_strpool.resize(sumKey2Len);
        // compare with '0', do NOT use size_t
        for (size_t i = ks.numKeys; i > 0; ) {
          i--;
          uint64_t offset = Read1stKey(reader, cplen, key1_len) - minValue;
          rankselect1.set1(offset);
          if (terark_unlikely(i == ks.numKeys - 1)) { // make sure 1st prev != offset
            prev = offset - 1;
          }
          if (offset != prev) { // next index1 is new one
            rankselect2.set0(i + 1);
          } else {
            rankselect2.set1(i + 1);
          }
          prev = offset;
          // save index data
          fstring key2Data(reader, cplen + key1_len);
          updateMinMax(key2Data, minKey2Data, maxKey2Data);
          pos -= key2Data.size();
          memcpy(keyVec.m_strpool.data() + pos, key2Data.data(), key2Data.size());
          reader += reclen;
        }
        rankselect2.set0(0); // set 1st element to 0
        assert(pos == 0);
      }
      rankselect2.set0(ks.numKeys);
      // TBD: build histogram, which should set as 'true'
      rankselect1.build_cache(false, false);
      rankselect2.build_cache(false, false);
      // try order: 1. sorteduint; 2. uintmin0; 3. fixlen
      // TBD: following skips are only for test right now
#ifndef INDEX_UT
      if (ioptions) {
        INFO(ioptions->info_log,
             "StrIdmapCompositeUintIndex::Build(): key1Min %llu, key1Max %llu, "
             "cplen %zu, key1_len %zu, key2_len %zu,\n"
             "key1 size %zu, key2 size %zu, "
             "rankselect combination is %d\n",
             minValue, maxValue, cplen, key1_len, key2_len,
             rankselect1.size(), rankselect2.size(),
             figureCombination(rankselect1, rankselect2)
          );
      }
#endif
      if (key2_len <= 8) {
        COIndex* index = nullptr;
        if (g_indexEnableSortedUint)
          index = CreateIndexWithSortedUintCont(
              rankselect1, rankselect2, keyVec, ks,
              minValue, maxValue, key1_len, minKey2Data, maxKey2Data);
        if (!index && g_indexEnableBigUint0) {
          index = CreateIndexWithUintCont(
              rankselect1, rankselect2, keyVec, ks,
              minValue, maxValue, key1_len, minKey2Data, maxKey2Data);
        }
        if (index)
          return index;
      }
      return CreateIndexWithStrCont(
          rankselect1, rankselect2, keyVec, ks,
          minValue, maxValue, key1_len);
    }

    void loadImplBase(fstring& mem, fstring fpath, CompositeUintIndexBase* ptr)
    const {
      if (mem.data() == nullptr) {
        MmapWholeFile mmapFile(fpath);
        mem = mmapFile.memory();
        mmapFile.base = nullptr;
        ptr->workingState_ = WorkingState::MmapFile;
      } else {
        ptr->workingState_ = WorkingState::UserMemory;
      }
      // make sure header is valid
      verifyBaseHeader(mem);
      // construct composite index
      const MyBaseFileHeader* header = (const MyBaseFileHeader*)mem.data();
      ptr->header_ = header;
      ptr->minValue_ = header->key1_min_value;
      ptr->maxValue_ = header->key1_max_value;
      ptr->key2_min_value_ = header->key2_min_value; // old version do NOT has this attribute
      ptr->key2_max_value_ = header->key2_max_value;
      ptr->key1_len_ = header->key1_fixed_len;
      ptr->key2_len_ = header->key2_fixed_len;
      if (header->common_prefix_length > 0) {
        ptr->commonPrefix_.risk_set_data((byte_t*)mem.data() + header->header_size,
                                         header->common_prefix_length);
      }
    }
  };
  int CommonSeek(fstring target, uint64_t* pKey1, fstring* pKey2,
                 size_t& id, size_t& rankselect1_idx) const {
    size_t cplen = target.commonPrefixLen(commonPrefix_);
    if (cplen != commonPrefix_.size()) {
      assert(target.size() >= cplen);
      assert(target.size() == cplen ||
        byte_t(target[cplen]) != byte_t(commonPrefix_[cplen]));
      if (target.size() == cplen ||
        byte_t(target[cplen]) < byte_t(commonPrefix_[cplen])) {
        rankselect1_idx = 0;
        id = 0;
        return 1;
      }
      else {
        id = size_t(-1);
        return 0;
      }
    }
    uint64_t key1 = 0;
    fstring  key2;
    if (target.size() <= cplen + key1_len_) {
      fstring sub = target.substr(cplen);
      byte_t targetBuffer[8] = { 0 };
      /*
       * do not think hard about int, think about string instead.
       * assume key1_len is 6 byte len like 'abcdef', target without
       * commpref is 'b', u should compare 'b' with 'a' instead of 'f'.
       * that's why assign sub starting in the middle instead at tail.
       */
      memcpy(targetBuffer + (8 - key1_len_), sub.data(), sub.size());
      *pKey1 = key1 = Read1stKey(targetBuffer, 0, 8);
      *pKey2 = key2 = fstring(); // empty
    }
    else {
      *pKey1 = key1 = Read1stKey(target, cplen, key1_len_);
      *pKey2 = key2 = target.substr(cplen + key1_len_);
    }
    if (key1 > maxValue_) {
      id = size_t(-1);
      return 0;
    }
    else if (key1 < minValue_) {
      rankselect1_idx = 0;
      id = 0;
      return 1;
    }
    else {
      return 2;
    }
  }

  class MyBaseIterator : public COIndex::FastIter {
  protected:
    size_t rankselect1_idx_; // used to track & decode index1 value
    size_t access_hint_;
    ushort full_len_;
    ushort pref_len_;
    ushort key1_len_;
    ushort key2_len_;
    const CompositeUintIndexBase& index_; // just for debug
   #if defined(_MSC_VER)
    byte_t* get_buffer_ptr() { return (byte_t*)(this + 1); }
    #define buffer_ get_buffer_ptr()
   #else
    byte_t buffer_[0];
   #endif

    void Delete() override final {
      // no need to call destructor
      free(this);
    }
    MyBaseIterator(const CompositeUintIndexBase& index) : index_(index) {
      rankselect1_idx_ = size_t(-1);
      m_id = size_t(-1);
      full_len_ = index.KeyLen();
      pref_len_ = index.commonPrefix_.size();
      key1_len_ = index.key1_len_;
      key2_len_ = index.key2_len_;
      memcpy(buffer_, index.commonPrefix_.data(), index.commonPrefix_.size());
      access_hint_ = size_t(-1);
      // full_len_ is redundant, it is same as m_key.n, don't change, KISS
      m_key = fstring(buffer_, full_len_);
      m_num = index.m_num_keys;
    }
    /*
     * Various combinations of <RankSelect1, RankSelect2, DataCont> may cause tooooo many template instantiations,
     * which will cause code expansion & compiler errors (warnings). That's why we lift some methods from
     * NonBaseIterator to BaseIterator, like UpdateBuffer/UpdateBufferKey2/CommonSeek/SeekToFirst(), even though
     * it seems strange.
     */
    void UpdateBuffer() {
      // key = commonprefix + key1s + key2
      // assign key1
      auto key1 = rankselect1_idx_ + index_.minValue_;
      SaveAsBigEndianUint64(buffer_ + pref_len_, key1_len_, key1);
      UpdateBufferKey2(pref_len_ + key1_len_);
    }
    virtual void UpdateBufferKey2(size_t offset) = 0;
    size_t DictRank() const override {
      assert(m_id != size_t(-1));
      return m_id;
    }
    bool SeekToFirst() override {
      rankselect1_idx_ = 0;
      m_id = 0;
      UpdateBuffer();
      return true;
    }
  };
  void* AllocIterMem() const {
    // extra 8 bytes for KeyTag(SeqNum & ValueType)
    return malloc(sizeof(MyBaseIterator) + this->KeyLen() + 8);
  }

  size_t KeyLen() const {
    return commonPrefix_.size() + key1_len_ + key2_len_;
  }
  const char* Name() const override {
    return header_->class_name;
  }
  fstring Memory() const override {
    return fstring((const char*)header_, (ptrdiff_t)header_->file_size);
  }
  bool NeedsReorder() const override {
    return false;
  }
  void GetOrderMap(UintVecMin0& newToOld) const override {
    TERARK_DIE("this function should not be called");
  }
  void BuildCache(double cacheRatio) override {
    //do nothing
  }
  void MetaToJsonCommon(json& js) const {
    //js["Type"] = header_->class_name;
    js["Magic"] = header_->magic;
    js["MinValue"] = minValue_;
    js["MaxValue"] = maxValue_;
    js["Key1.KeyLen"] = key1_len_;
    js["Key2.KeyLen"] = key2_len_;
    js["Key2.KeyMin"] = key2_min_value_;
    js["Key2.KeyMax"] = key2_max_value_;
    js["CommonPrefix"] = Slice((char*)commonPrefix_.data(), commonPrefix_.size()).hex();
  }

  ~CompositeUintIndexBase() {
    if (workingState_ == WorkingState::Building) {
      delete header_;
    } else {
      if (workingState_ == WorkingState::MmapFile) {
        terark::mmap_close((void*) header_, header_->file_size);
      }
      commonPrefix_.risk_release_ownership();
    }
  }

  const MyBaseFileHeader* header_;
  valvec<byte_t>    commonPrefix_;
  uint64_t          minValue_;
  uint64_t          maxValue_;
  uint64_t          key2_min_value_;
  uint64_t          key2_max_value_;
  uint32_t          key1_len_;
  uint32_t          key2_len_;
  WorkingState      workingState_;
};
const char CompositeUintIndexBase::index_name[] = "CompositeIndex";

/*
 * For simplicity, let's take composite index => index1:index2. Then following
 * compositeindexes like,
 *   4:6, 4:7, 4:8, 7:19, 7:20, 8:3
 * index1: {4, 7}, use bitmap (also UintIndex's form) to represent
 *   4(1), 5(0), 6(0), 7(1), 8(1)
 * index1:index2: use bitmap to respresent 4:6(0), 4:7(1), 4:8(1), 7:19(0), 7:20(1), 8:3(0)
 * to search 7:20, use bitmap1 to rank1(7) = 2, then use bitmap2 to select0(2) = position-3,
 * use bitmap2 to select0(3) = position-5. That is, we should search within [3, 5).
 * iter [3 to 5), 7:20 is found, done.
 */
template<class RankSelect1, class RankSelect2, class Key2DataContainer>
class CompositeUintIndex : public CompositeUintIndexBase {
public:
  struct FileHeader : MyBaseFileHeader {
    FileHeader(size_t file_size)
      : MyBaseFileHeader(file_size, typeid(CompositeUintIndex))
    {}
  };
public:
  bool SeekImpl(fstring target, size_t& id, size_t& rankselect1_idx) const {
    uint64_t key1 = 0;
    fstring  key2;
    int ret = this->CommonSeek(target, &key1, &key2, id, rankselect1_idx);
    if (ret < 2) {
      return ret ? true : false;
    }
    // find the corresponding bit within 2ndRS
    uint64_t order, pos0, cnt;
    rankselect1_idx = key1 - minValue_;
    if (rankselect1_[rankselect1_idx]) {
      // find within this index
      order = rankselect1_.rank1(rankselect1_idx);
      pos0 = rankselect2_.select0(order);
      if (pos0 == key2_data_.size() - 1) { // last elem
        id = (key2_data_.compare(pos0, key2) >= 0) ? pos0 : size_t(-1);
        goto out;
      }
      else {
        cnt = rankselect2_.one_seq_len(pos0 + 1) + 1;
        id = Locate(key2_data_, pos0, cnt, key2);
        if (id != size_t(-1)) {
          goto out;
        }
        else if (pos0 + cnt == key2_data_.size()) {
          goto out;
        }
        else {
          // try next offset
          rankselect1_idx++;
        }
      }
    }
    // no such index, use the lower_bound form
    cnt = rankselect1_.zero_seq_len(rankselect1_idx);
    if (rankselect1_idx + cnt >= rankselect1_.size()) {
      id = size_t(-1);
      return false;
    }
    rankselect1_idx += cnt;
    order = rankselect1_.rank1(rankselect1_idx);
    id = rankselect2_.select0(order);
  out:
    if (id == size_t(-1)) {
      return false;
    }
    return true;
  }
  class CompositeUintIndexIterator : public MyBaseIterator {
  public:
    CompositeUintIndexIterator(const CompositeUintIndex& index)
      : MyBaseIterator(index) {}
    void UpdateBufferKey2(size_t offset) override {
      auto& index_ = static_cast<const CompositeUintIndex&>(MyBaseIterator::index_);
      index_.key2_data_.copy_to(m_id, buffer_ + offset);
    }

    bool SeekToLast() override {
      auto& index_ = static_cast<const CompositeUintIndex&>(MyBaseIterator::index_);
      rankselect1_idx_ = index_.rankselect1_.size() - 1;
      ROCKSDB_ASSERT_EQ(index_.key2_data_.size(), m_num);
      m_id = m_num - 1;
      UpdateBuffer();
      return true;
    }
    bool Seek(fstring target) override {
      auto& index = static_cast<const CompositeUintIndex&>(MyBaseIterator::index_);
      if (index.SeekImpl(target, m_id, rankselect1_idx_)) {
        UpdateBuffer();
        return true;
      }
      return false;
    }

    bool Next() override {
      auto& index_ = static_cast<const CompositeUintIndex&>(MyBaseIterator::index_);
      assert(m_id != size_t(-1));
      assert(index_.rankselect1_[rankselect1_idx_] != 0);
      ROCKSDB_ASSERT_EQ(index_.key2_data_.size(), m_num);
      if (terark_unlikely(m_id + 1 == m_num)) {
        m_id = size_t(-1);
        return false;
      } else {
        if (Is1stKeyDiff(m_id + 1)) {
          assert(rankselect1_idx_ + 1 <  index_.rankselect1_.size());
          //uint64_t cnt = index_.rankselect1_.zero_seq_len(rankselect1_idx_ + 1);
          uint64_t cnt = fast_zero_seq_len(index_.rankselect1_, rankselect1_idx_ + 1, access_hint_);
          rankselect1_idx_ += cnt + 1;
          assert(rankselect1_idx_ < index_.rankselect1_.size());
        }
        ++m_id;
        UpdateBuffer();
        return true;
      }
    }
    bool Prev() override {
      auto& index_ = static_cast<const CompositeUintIndex&>(MyBaseIterator::index_);
      assert(m_id != size_t(-1));
      assert(index_.rankselect1_[rankselect1_idx_] != 0);
      if (terark_unlikely(m_id == 0)) {
        m_id = size_t(-1);
        return false;
      } else {
        if (Is1stKeyDiff(m_id)) {
          /*
           * zero_seq_ has [a, b) range, hence next() need (pos_ + 1), whereas
           * prev() just called with (pos_) is enough
           * case1: 1 0 1, ...
           * case2: 1 1, ...
           */
          assert(rankselect1_idx_ > 0);
          //uint64_t cnt = index_.rankselect1_.zero_seq_revlen(rankselect1_idx_);
          uint64_t cnt = fast_zero_seq_revlen(index_.rankselect1_, rankselect1_idx_, access_hint_);
          assert(rankselect1_idx_ >= cnt + 1);
          rankselect1_idx_ -= (cnt + 1);
        }
        --m_id;
        UpdateBuffer();
        return true;
      }
    }

  protected:
    //use 2nd index bitmap to check if 1st index changed
    bool Is1stKeyDiff(size_t curr_id) {
      auto& index_ = static_cast<const CompositeUintIndex&>(MyBaseIterator::index_);
      return index_.rankselect2_.is0(curr_id);
    }
  };

public:
  class MyFactory : public MyBaseFactory {
  public:
    // no option.keyPrefixLen
    COIndex* Build(const std::string& keyFilePath,
                       const ToplingZipTableOptions& tzopt,
                       const KeyStat& ks,
                       const ImmutableOptions* ioption) const override {
      MmapWholeFile fm(keyFilePath);
      TERARK_VERIFY_EQ(fm.size, ks.sumKeyLen);
      auto reader = (const byte_t*)(fm.base);
      MmapAdvSeq(fm.base, fm.size);
      auto ti = BuildImpl(reader, tzopt, ks,
                       (RankSelect1*)(NULL), (RankSelect2*)(NULL), ioption);
      TERARK_VERIFY_EQ(reader, (byte_t*)fm.base + fm.size);
      return ti;
    }
  public:
    COIndexUP LoadMemory(fstring mem) const override {
      return COIndexUP(loadImpl(mem, {}));
    }
    COIndexUP LoadFile(fstring fpath) const override {
      return COIndexUP(loadImpl({}, fpath));
    }
  protected:
    COIndex* loadImpl(fstring mem, fstring fpath) const {
      auto ptr = new CompositeUintIndex<RankSelect1, RankSelect2, Key2DataContainer>();
      COIndexUP ptr_up(ptr); // guard
      loadImplBase(mem, fpath, ptr);
      const MyBaseFileHeader* header = (const MyBaseFileHeader*)mem.data();
      VerifyClassName(header->class_name, typeid(CompositeUintIndex));
      size_t offset = header->header_size +
        align_up(header->common_prefix_length, 8);
      ptr->rankselect1_.risk_mmap_from((unsigned char*)mem.data() + offset,
                                       header->rankselect1_mem_size);
      offset += header->rankselect1_mem_size;
      ptr->rankselect2_.risk_mmap_from((unsigned char*)mem.data() + offset,
                                       header->rankselect2_mem_size);
      offset += header->rankselect2_mem_size;
      if (std::is_same<Key2DataContainer, SortedUintDataCont>::value) {
        ptr->key2_data_.risk_set_data((unsigned char*)mem.data() + offset,
                                      header->key2_data_mem_size);
        //ptr->key2_data_.init(header->key2_fixed_len, header->key2_min_value);
        ptr->key2_data_.set_key_len(header->key2_fixed_len);
        ptr->key2_data_.set_min_value(header->key2_min_value);
      } else if (std::is_same<Key2DataContainer, Min0DataCont>::value) {
        size_t num = ptr->rankselect2_.size() - 1; // sub the extra append '0'
        size_t diff = header->key2_max_value - header->key2_min_value + 1;
        ptr->key2_data_.risk_set_data((unsigned char*)mem.data() + offset,
                                      num, diff);
        //ptr->key2_data_.init(header->key2_fixed_len, header->key2_min_value);
        ptr->key2_data_.set_key_len(header->key2_fixed_len);
        ptr->key2_data_.set_min_value(header->key2_min_value);
      } else { // FixedLenStr
        ptr->key2_data_.risk_set_data((unsigned char*)mem.data() + offset,
                                      header->key2_data_mem_size);
        //ptr->key2_data_.init(header->key2_fixed_len,
        //                   header->key2_data_mem_size / header->key2_fixed_len);
        ptr->key2_data_.set_key_len(header->key2_fixed_len);
        ptr->key2_data_.set_size(header->key2_data_mem_size / header->key2_fixed_len);
      }
      ptr->m_num_keys = ptr->key2_data_.size();
      ptr_up.release(); // must release
      return ptr;
    }
  };

public:
  using COIndex::FactoryPtr;
  CompositeUintIndex() {}
  CompositeUintIndex(RankSelect1& rankselect1, RankSelect2& rankselect2,
                     Key2DataContainer& key2Container, const KeyStat& ks,
                     uint64_t minValue, uint64_t maxValue, size_t key1_len,
                     uint64_t minKey2Value = 0, uint64_t maxKey2Value = 0) {
    //isBuilding_ = true;
    workingState_ = WorkingState::Building;
    m_num_keys = ks.numKeys;
    size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
    // save meta into header
    FileHeader* header = new FileHeader(
      rankselect1.mem_size() +
      rankselect2.mem_size() +
      align_up(key2Container.mem_size(), 8));
    header->key1_min_value = minValue;
    header->key1_max_value = maxValue;
    header->rankselect1_mem_size = rankselect1.mem_size();
    header->rankselect2_mem_size = rankselect2.mem_size();
    header->key2_data_mem_size = key2Container.mem_size();
    header->key1_fixed_len = uint32_t(key1_len);
    header->key2_min_value = minKey2Value;
    header->key2_max_value = maxKey2Value;
    header->key2_fixed_len = uint32_t(ks.minKeyLen - cplen - key1_len);
    header->common_prefix_length = uint32_t(ks.prefix.size() + cplen);
    commonPrefix_.assign(ks.prefix + fstring(ks.minKey.data(), cplen));
    header->file_size = align_up(header->file_size + ks.prefix.size() + cplen, 8);
    header_ = header;
    rankselect1_.swap(rankselect1);
    rankselect2_.swap(rankselect2);
    key2_data_.swap(key2Container);
    key1_len_ = uint32_t(key1_len);
    key2_len_ = header->key2_fixed_len;
    minValue_ = minValue;
    maxValue_ = maxValue;
    key2_min_value_ = minKey2Value;
    key2_max_value_ = maxKey2Value;
    //isUserMemory_ = false;
  }

  virtual ~CompositeUintIndex() {
    if (workingState_ == WorkingState::Building) {
      // do nothing, will destruct in base destructor
    } else {
      rankselect1_.risk_release_ownership();
      rankselect2_.risk_release_ownership();
      key2_data_.risk_release_ownership();
    }
  }
  void SaveMmap(std::function<void(const void *, size_t)> write) const override {
    write(header_, sizeof *header_);
    if (!commonPrefix_.empty()) {
      write(commonPrefix_.data(), commonPrefix_.size());
      Padzero<8>(write, commonPrefix_.size());
    }
    write(rankselect1_.data(), rankselect1_.mem_size());
    write(rankselect2_.data(), rankselect2_.mem_size());
    write(key2_data_.data(), key2_data_.mem_size());
    Padzero<8>(write, key2_data_.mem_size());
  }
  size_t Find(fstring key) const override {
    size_t cplen = commonPrefix_.size();
    if (key.size() != key2_len_ + key1_len_ + cplen ||
        !key.startsWith(commonPrefix_)) {
      return size_t(-1);
    }
    uint64_t key1 = Read1stKey(key, cplen, key1_len_);
    if (key1 < minValue_ || key1 > maxValue_) {
      return size_t(-1);
    }
    uint64_t offset = key1 - minValue_;
    if (!rankselect1_[offset]) {
      return size_t(-1);
    }
    uint64_t order = rankselect1_.rank1(offset);
    uint64_t pos0 = rankselect2_.select0(order);
    assert(pos0 != size_t(-1));
    size_t cnt = rankselect2_.one_seq_len(pos0 + 1);
    fstring key2 = key.substr(cplen + key1_len_);
    size_t id = Locate(key2_data_, pos0, cnt + 1, key2);
    if (id != size_t(-1) && key2_data_.equals(id, key2)) {
      return id;
    }
    return size_t(-1);
  }
  size_t AccurateRank(fstring key) const final {
    size_t id, rankselect1_idx;
    if (this->SeekImpl(key, id, rankselect1_idx)) {
      return id;
    }
    return key2_data_.size();
  }
  size_t ApproximateRank(fstring key) const final {
    return AccurateRank(key);
  }
  size_t TotalKeySize() const override final {
    return (commonPrefix_.size() + key1_len_ + key2_len_) * key2_data_.size();
  }
  std::string NthKey(size_t nth) const final {
    TERARK_DIE("TODO: realize me");
    TERARK_VERIFY_LT(nth, m_num_keys);
    //size_t cplen = commonPrefix_.size();
    std::string key;
    //key.reserve(cplen + key1_len_ + key2_len_);
    //auto value1 = minValue_ + rankselect1_.select1(nth);
    //key.append((const char*)commonPrefix_.data(), cplen);
    //key.append((const char*)m_keys.nth_data(nth), suff_len);
    return key;
  }
  Iterator* NewIterator() const override {
    void* mem = this->AllocIterMem();
    return new(mem)CompositeUintIndexIterator(*this);
  }
  json MetaToJson() const final {
    json js;
    MetaToJsonCommon(js);
    js["RankSelect1.NumBits"] = rankselect1_.size();
    js["RankSelect1.MemSize"] = rankselect1_.mem_size();
    js["RankSelect1.MaxRank0"] = rankselect1_.max_rank0();
    js["RankSelect1.MaxRank1"] = rankselect1_.max_rank1();
    js["RankSelect2.NumBits"] = rankselect2_.size();
    js["RankSelect2.MemSize"] = rankselect2_.mem_size();
    js["RankSelect2.MaxRank0"] = rankselect2_.max_rank0();
    js["RankSelect2.MaxRank1"] = rankselect2_.max_rank1();
    js["Key2.Data.Entries"] = key2_data_.size();
    js["Key2.Data.MemSize"] = key2_data_.mem_size();
    js["Key2.Data.AvgLen"] = double(key2_data_.mem_size() + 0.01) / (key2_data_.size() + 0.01);
    return js;
  }

public:
  size_t Locate(const Key2DataContainer& arr,
                size_t start, size_t cnt, fstring target) const {
    size_t lo = start, hi = start + cnt;
    size_t pos = arr.lower_bound(lo, hi, target);
    return (pos < hi) ? pos : size_t(-1);
  }

protected:
  RankSelect1       rankselect1_;
  RankSelect2       rankselect2_;
  Key2DataContainer key2_data_;
};

template<class RankSelect1, class RankSelect2, class DataCont>
COIndex*
CompositeUintIndexBase::MyBaseFactory::CreateIndex(
    RankSelect1& rankselect1,
    RankSelect2& rankselect2,
    DataCont& container,
    const KeyStat& ks,
    uint64_t key1MinValue, uint64_t key1MaxValue, size_t key1_len,
    uint64_t key2MinValue, uint64_t key2MaxValue)
{
  const size_t kRS1Cnt = rankselect1.size(); // to accompany ks2cnt
  const size_t kRS2Cnt = rankselect2.size(); // extra '0' append to rs2, included as well
  AmazingCombinationT cob = figureCombination(rankselect1, rankselect2);
  switch (cob) {
  // -- all one & ...
  case kAllOne_AllZero: {
    rank_select_allone rs1(kRS1Cnt);
    rank_select_allzero rs2(kRS2Cnt);
    return new CompositeUintIndex<rank_select_allone, rank_select_allzero, DataCont>(
      rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kAllOne_FewZero: {
    rank_select_allone rs1(kRS1Cnt);
    rank_select_fewzero<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<rank_select_allone, rank_select_fewzero<typename RankSelect2::index_t>, DataCont>(
      rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kAllOne_FewOne: {
    rank_select_allone rs1(kRS1Cnt);
    rank_select_fewone<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<rank_select_allone, rank_select_fewone<typename RankSelect2::index_t>, DataCont>(
      rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kAllOne_Normal: {
    rank_select_allone rs1(kRS1Cnt);
    return new CompositeUintIndex<rank_select_allone, RankSelect2, DataCont>(
      rs1, rankselect2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  // -- few zero & ...
  case kFewZero_AllZero: {
    rank_select_fewzero<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    rank_select_allzero rs2(kRS2Cnt);
    return new CompositeUintIndex<rank_select_fewzero<typename RankSelect1::index_t>, rank_select_allzero, DataCont>(
      rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kFewZero_FewZero: {
    rank_select_fewzero<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    rank_select_fewzero<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<rank_select_fewzero<typename RankSelect1::index_t>,
                                  rank_select_fewzero<typename RankSelect2::index_t>, DataCont>(
      rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kFewZero_FewOne: {
    rank_select_fewzero<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    rank_select_fewone<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<rank_select_fewzero<typename RankSelect1::index_t>,
                                  rank_select_fewone<typename RankSelect2::index_t>, DataCont>(
      rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kFewZero_Normal: {
    rank_select_fewzero<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    return new CompositeUintIndex<rank_select_fewzero<typename RankSelect1::index_t>, RankSelect2, DataCont>(
      rs1, rankselect2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  // few one & ...
  case kFewOne_AllZero: {
    rank_select_fewone<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    rank_select_allzero rs2(kRS2Cnt);
    return new CompositeUintIndex<rank_select_fewone<typename RankSelect1::index_t>, rank_select_allzero, DataCont>(
      rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kFewOne_FewZero: {
    rank_select_fewone<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    rank_select_fewzero<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<rank_select_fewone<typename RankSelect1::index_t>,
                                  rank_select_fewzero<typename RankSelect2::index_t>, DataCont>(
      rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kFewOne_FewOne: {
    rank_select_fewone<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    rank_select_fewone<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<rank_select_fewone<typename RankSelect1::index_t>,
                                  rank_select_fewone<typename RankSelect2::index_t>, DataCont>(
      rs1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kFewOne_Normal: {
    rank_select_fewone<typename RankSelect1::index_t> rs1(kRS1Cnt);
    rs1.build_from(rankselect1);
    return new CompositeUintIndex<rank_select_fewone<typename RankSelect1::index_t>, RankSelect2, DataCont>(
      rs1, rankselect2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  // normal & ...
  case kNormal_AllZero: {
    rank_select_allzero rs2(kRS2Cnt);
    return new CompositeUintIndex<RankSelect1, rank_select_allzero, DataCont>(
      rankselect1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kNormal_FewZero: {
    rank_select_fewzero<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<RankSelect1, rank_select_fewzero<typename RankSelect2::index_t>, DataCont>(
      rankselect1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kNormal_FewOne: {
    rank_select_fewone<typename RankSelect2::index_t> rs2(kRS2Cnt);
    rs2.build_from(rankselect2);
    return new CompositeUintIndex<RankSelect1, rank_select_fewone<typename RankSelect2::index_t>, DataCont>(
      rankselect1, rs2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  case kNormal_Normal: {
    return new CompositeUintIndex<RankSelect1, RankSelect2, DataCont>(
      rankselect1, rankselect2, container, ks, key1MinValue, key1MaxValue, key1_len, key2MinValue, key2MaxValue);
  }
  default:
    assert(0);
    return nullptr;
  }
}

typedef Min0DataCont BigUintDataCont;

typedef rs_fewone_32           FewOne32;
typedef rs_fewone_64           FewOne64;
typedef rs_fewzero_32          FewZero32;
typedef rs_fewzero_64          FewZero64;

#define RegisterIndex COIndexRegister
#define RegisterCompositeUintIndex4(rs1, rs2, Key2, _Key2) \
  typedef       CompositeUintIndex <rs1, rs2, Key2##DataCont> \
                CompositeUintIndex_##rs1##_##rs2##_Key2; \
  RegisterIndex(CompositeUintIndex_##rs1##_##rs2##_Key2)

#define RegisterCompositeUintIndex3(rs1, rs2, Key2) \
        RegisterCompositeUintIndex4(rs1, rs2, Key2, _##Key2)

#define RegisterCompositeUintIndex(rs1, rs2) \
  RegisterCompositeUintIndex4(rs1, rs2, Str,); \
  RegisterCompositeUintIndex3(rs1, rs2, SortedUint); \
  RegisterCompositeUintIndex3(rs1, rs2,    BigUint)

RegisterCompositeUintIndex(AllOne   , AllZero  );
RegisterCompositeUintIndex(AllOne   , FewZero32);
RegisterCompositeUintIndex(AllOne   , FewZero64);
RegisterCompositeUintIndex(AllOne   , FewOne32 );
RegisterCompositeUintIndex(AllOne   , FewOne64 );
RegisterCompositeUintIndex(AllOne   , IL_256_32);
RegisterCompositeUintIndex(AllOne   , SE_512_64);

RegisterCompositeUintIndex(FewOne32 , AllZero  );
RegisterCompositeUintIndex(FewOne64 , AllZero  );
RegisterCompositeUintIndex(FewOne32 , FewZero32);
RegisterCompositeUintIndex(FewOne64 , FewZero64);
RegisterCompositeUintIndex(FewOne32 , FewOne32 );
RegisterCompositeUintIndex(FewOne64 , FewOne64 );
RegisterCompositeUintIndex(FewOne32 , IL_256_32);
RegisterCompositeUintIndex(FewOne64 , SE_512_64);

RegisterCompositeUintIndex(FewZero32, AllZero  );
RegisterCompositeUintIndex(FewZero64, AllZero  );
RegisterCompositeUintIndex(FewZero32, FewZero32);
RegisterCompositeUintIndex(FewZero64, FewZero64);
RegisterCompositeUintIndex(FewZero32, FewOne32 );
RegisterCompositeUintIndex(FewZero64, FewOne64 );
RegisterCompositeUintIndex(FewZero32, IL_256_32);
RegisterCompositeUintIndex(FewZero64, SE_512_64);

RegisterCompositeUintIndex(IL_256_32, AllZero  );
RegisterCompositeUintIndex(SE_512_64, AllZero  );
RegisterCompositeUintIndex(IL_256_32, FewZero32);
RegisterCompositeUintIndex(SE_512_64, FewZero64);
RegisterCompositeUintIndex(IL_256_32, FewOne32 );
RegisterCompositeUintIndex(SE_512_64, FewOne64 );
RegisterCompositeUintIndex(IL_256_32, IL_256_32);
RegisterCompositeUintIndex(SE_512_64, SE_512_64);

} // namespace rocksdb
