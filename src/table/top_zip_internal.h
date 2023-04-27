/*
 * topling_zip_internal.h
 *
 *  Created on: 2017-05-02
 *      Author: zhaoming
 */

#pragma once

// project headers
#include "top_zip_table.h"
// std headers
#include <mutex>
#include <atomic>
#include <memory>
// boost headers
#include <boost/intrusive_ptr.hpp>
#include <boost/noncopyable.hpp>
// rocksdb headers
#include <rocksdb/slice.h>
#include <rocksdb/env.h>
#include <rocksdb/table.h>
#include <table/format.h>
#include <file/writable_file_writer.h>

// terark headers
#include <terark/fstring.hpp>
#include <terark/valvec.hpp>
#include <terark/stdtypes.hpp>
#include <terark/util/atomic.hpp>
#include <terark/util/profiling.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/zbs/lru_page_cache.hpp>
#include <terark/gold_hash_map.hpp>

namespace rocksdb {

using terark::fstring;
using terark::valvec;
using terark::byte_t;
using terark::LruReadonlyCache;

extern const std::string kToplingZipTableValueDictBlock;
extern const std::string kToplingZipTableOffsetBlock;
extern const std::string kZipTableKeyRankCacheBlock;
extern const std::string kToplingZipTableDictInfo;
extern const std::string kToplingZipTableDictSize;

ROCKSDB_ENUM_CLASS(ZipValueType, unsigned char,
  kZeroSeq = 0,
  kDelete = 1,
  kValue = 2,
  kMulti = 3
);
//const size_t kZipValueTypeBits = 2;

ROCKSDB_ENUM_PLAIN(TagRsKind, unsigned char,
  LegacyZvType = 0,

  // First map one user key to multiple(>=1) Tag(Seq+OpType),
  // encoded one user key map to bits: `0 + [1111](num = s-1) + 0`,
  // such as: 0 110 10 0 0 10 0 1110 (tatal (valueNum + 1) bits)
  //   the leading 0 is for covenient, each `11110` bits represent
  //   the bits for one user key
  //
  // Rank Select Key must be {normal, allzero},
  // Rank Select Tag can  be {normal, allzero, allone}
  RS_Key0_TagN = 1,
  RS_Key0_Tag0 = 2,
  RS_Key0_Tag1 = 3,
  RS_KeyN_TagN = 4,
//RS_KeyN_Tag0 = 5, // will not happen, but keep the enum value 5
  RS_KeyN_Tag1 = 6
);

struct ZipValueMultiValue {
  // use offset[0] as num, and do not store offsets[num]
  // when unzip, reserve num+1 cells, set offsets[0] to 0,
  // and set offsets[num] to length of value pack
  //	uint32_t num;
  uint32_t offsets[1];

  ///@size size include the extra uint32, == encoded_size + 4
  static
  const ZipValueMultiValue* decode(void* data, size_t size, uint32_t* pNum) {
    // data + 4 is the encoded data
    auto me = (ZipValueMultiValue*)(data);
    auto num = me->offsets[1];
    assert(num > 0);
    memmove(me->offsets + 1, me->offsets + 2, sizeof(uint32_t)*(num - 1));
    me->offsets[0] = 0;
    me->offsets[num] = uint32_t(size - sizeof(uint32_t)*(num + 1));
    *pNum = num;
    return me;
  }
  static
  const ZipValueMultiValue* decode(valvec<byte_t>& buf, uint32_t* pNum) {
    return decode(buf.data(), buf.size(), pNum);
  }
  Slice getValueData(size_t nth, size_t num) const {
    assert(nth < num);
    size_t offset0 = offsets[nth + 0];
    size_t offset1 = offsets[nth + 1];
    size_t dlength = offset1 - offset0;
    const char* base = (const char*)(offsets + num + 1);
    return Slice(base + offset0, dlength);
  }
  static size_t calcHeaderSize(size_t n) { return sizeof(uint32_t) * (n); }
};

template<class T>
class UintMap {
  terark::valvec<T> m_fast;
  terark::gold_hash_map<uint64_t, T> m_slow;
public:
  void resize_fill(size_t max_fast, const T& val = T()) {
    m_fast.resize_fill(max_fast, val);
  }
  T& operator[](uint64_t key) {
    if (key < m_fast.size())
      return m_fast[key];
    else
      return m_slow[key];
  }
  bool exists(uint64_t key) const {
    return key < m_fast.size() || m_slow.exists(key);
  }
  size_t fast_num() const { return m_fast.size(); }
  size_t slow_num() const { return m_slow.size(); }
  void add(const UintMap& um2) {
    size_t min_fast = std::min(m_fast.size(), um2.m_fast.size());
    for (size_t i = 0; i < min_fast; ++i) {
      m_fast[i].add(um2.m_fast[i]);
    }
    for (size_t i = min_fast, src_fast = um2.m_fast.size(); i < src_fast; ++i) {
      m_slow[i].add(um2.m_fast[i]);
    }
    for (size_t i = 0, n = um2.m_slow.end_i(); i < n; ++i) {
      m_slow[um2.m_slow.key(i)].add(um2.m_slow.val(i));
    }
  }
  template<class Func>
  void for_each(Func fn) const {
    const T* fast = m_fast.data();
    for (size_t i = 0, n = m_fast.size(); i < n; ++i) {
      fn(i, fast[i]);
    }
    TERARK_VERIFY_EZ(m_slow.delcnt());
    for (size_t i = 0, n = m_slow.end_i(); i < n; ++i) {
      fn(m_slow.key(i), m_slow.val(i));
    }
  }
  void sort() {
    m_slow.sort([](const auto& x, const auto& y) { return x.first < y.first;});
  }

  template<class DataIO>
  friend void DataIO_loadObject(DataIO& dio, UintMap& x) {
    dio >> x.m_fast;
    dio >> x.m_slow;
  }
  template<class DataIO>
  friend void DataIO_saveObject(DataIO& dio, const UintMap& x) {
    auto fast = x.m_fast.data();
    size_t upper = x.m_fast.size();
    for (; upper > 0; --upper) {
      if (bool(fast[upper-1])) { // != 0
        break;
      }
    }
    dio.write_var_uint64(upper);
    dio.ensureWrite(x.m_fast.data(), sizeof(T) * upper);
    dio << x.m_slow;
  }
};

struct ToplingZipTableFactoryState {
  mutable size_t num_new_table = 0;
  mutable size_t internal_key_len = 0; // <--> g_sumKeyLen
  mutable size_t internal_key_num = 0; // <--> g_sumEntryNum
  mutable size_t user_key_num = 0;     // <--> g_sumUserKeyNum
  mutable size_t value_raw_size = 0;   // <--> g_sumValueLen
  mutable size_t value_zip_size = 0;
  mutable size_t index_raw_size = 0;   // <--> g_sumUserKeyLen
  mutable size_t index_zip_size = 0;
  mutable size_t tagrs_zip_size = 0;
  mutable size_t gdict_raw_size = 0;
  mutable size_t gdict_zip_size = 0;
  mutable size_t sum_part_num = 0;
  struct ZipSizeInfo {
    ZipSizeInfo() {}
    uint64_t raw_size = 0;
    uint64_t zip_size = 0;
    void add(const ZipSizeInfo& x) {
      raw_size += x.raw_size;
      zip_size += x.zip_size;
    }
    double zipRatio(double staticRatio) const {
      const size_t MIN_RAW_SIZE = 1 << 20; // 1M
      if (raw_size > MIN_RAW_SIZE) {
        auto r = double(zip_size) / raw_size;
        if (r < 0.8)
          return r;
      }
      return staticRatio;
    }
    bool operator!() const { return 0 == raw_size; }
    explicit operator bool() const { return 0 != raw_size; }
  };
  mutable UintMap<ZipSizeInfo> level_zip_size_;
  mutable UintMap<ZipSizeInfo> level_zip_size_inc_;

  void add(const ToplingZipTableFactoryState& y);
};
class ToplingZipTableFactory : public TableFactory,
        public ToplingZipTableFactoryState, boost::noncopyable {
public:
  explicit
  ToplingZipTableFactory(const ToplingZipTableOptions&);
  ~ToplingZipTableFactory() override;

  const char* Name() const override { return "ToplingZipTable"; }

  using TableFactory::NewTableReader;
  Status
  NewTableReader(const ReadOptions&, const TableReaderOptions&,
          std::unique_ptr<RandomAccessFileReader>&&, uint64_t file_size,
          std::unique_ptr<TableReader>*,
          bool prefetch_index_and_filter_in_cache) const override;

  TableBuilder*
  NewTableBuilder(const TableBuilderOptions&,
                  WritableFileWriter*) const override;

  std::string GetPrintableOptions() const override;

  Status ValidateOptions(const DBOptions&,
                         const ColumnFamilyOptions&) const override;

  bool IsDeleteRangeSupported() const override { return true; }

  LruReadonlyCache* cache() const { return cache_.get(); }

// ToplingZipTableFactory is used internally, public all members for simple
  ToplingZipTableOptions table_options_;
  boost::intrusive_ptr<LruReadonlyCache> cache_;

  void add_zip_size_info(size_t prefix_idx, const ZipSizeInfo& zsi) const {
    auto& map = is_compaction_worker_ ? level_zip_size_inc_ : level_zip_size_;
    std::lock_guard<std::mutex> lk(mtx_);
    map[prefix_idx].add(zsi);
  }
  ZipSizeInfo get_zip_size_info(size_t prefix_idx) const {
    mtx_.lock();
    auto zsi = level_zip_size_[prefix_idx];
    TERARK_ASSERT_F(level_zip_size_.exists(prefix_idx), "%zd", prefix_idx);
    mtx_.unlock();
    return zsi;
  }
  bool is_compaction_worker_ = false;

  // ZipTableReader tickers
  mutable size_t  cumu_iter_num = 0;
  mutable size_t  live_iter_num = 0;
  mutable size_t  iter_seek_cnt = 0;
  mutable size_t  iter_next_cnt = 0;
  mutable size_t  iter_prev_cnt = 0;
  mutable size_t  iter_key_len = 0;
  mutable size_t  iter_val_len = 0;

  mutable std::mutex mtx_;
};

extern const uint64_t kToplingZipTableMagicNumber;

}  // namespace rocksdb
