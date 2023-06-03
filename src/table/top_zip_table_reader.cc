#include "top_zip_internal.h"
#include "co_index.h"

#include <memory/arena.h>
#include <table/block_based/block.h>
#include <table/get_context.h>
#include <table/internal_iterator.h>
#include <table/meta_blocks.h>

#include <topling/side_plugin_factory.h>
#include <topling/builtin_table_factory.h>
#include <table/top_table_common.h>

#include <terark/bitfield_array.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/lcast.hpp>
#include <terark/succinct/rank_select_il_256.hpp>
#include <terark/succinct/rank_select_mixed_xl_256.hpp>
#include <terark/succinct/rank_select_simple.hpp>
#include <terark/thread/fiber_aio.hpp>
#include <terark/util/crc.hpp>
#include <terark/util/function.hpp>
#include <terark/util/hugepage.hpp>
#include <terark/util/throw.hpp>
#include <terark/util/vm_util.hpp>
#include <terark/zbs/dict_zip_blob_store.hpp>

#ifdef _MSC_VER
# define NOMINMAX
# define WIN32_LEAN_AND_MEAN  // We only need minimal includes
# include <Windows.h>
# undef min
# undef max
#else
# include <fcntl.h>
# include <sys/mman.h>
# include <sys/unistd.h>
#endif

// for isChecksumVerifyEnabled()
#include <terark/zbs/blob_store_file_header.hpp>
// third party
#include <zstd/zstd.h>

#if defined(__linux__)
#include <linux/mman.h>
#endif

//#define TOP_ZIP_TABLE_KEEP_EXCEPTION
#if defined(TOP_ZIP_TABLE_KEEP_EXCEPTION)
  #define IF_TOP_ZIP_TABLE_KEEP_EXCEPTION(Then, Else) Then
#else
  #define IF_TOP_ZIP_TABLE_KEEP_EXCEPTION(Then, Else) Else
#endif

namespace rocksdb {

using namespace terark;

// does not need ROCKSDB_STATIC_TLS
static thread_local bool g_tls_is_seqscan = false;

void TopTableSetSeqScan(bool val) {
  g_tls_is_seqscan = val;
}

Status DecompressDict(Slice dictInfo, Slice dict, valvec<byte_t>* output_dict) {
  output_dict->clear();
  if (dictInfo.empty()) {
    return Status::OK();
  }
  if (!dictInfo.starts_with("ZSTD_")) {
    return Status::Corruption("Load global dict error", "unsupported dict format");
  }
  unsigned long long raw_size = ZSTD_getDecompressedSize(dict.data(), dict.size());
  if (raw_size == 0) {
    return Status::Corruption("Load global dict error", "zstd get raw size fail");
  }
  output_dict->resize_no_init(raw_size);
  size_t size = ZSTD_decompress(output_dict->data(), raw_size, dict.data(), dict.size());
  if (ZSTD_isError(size)) {
    return Status::Corruption("Load global dict ZSTD error", ZSTD_getErrorName(size));
  }
  TERARK_VERIFY_EQ(size, size_t(raw_size));
  MmapColdize(dict);
  return Status::OK();
}

/// @brief Zipped TagArray
class ZTagArray : public UintVecMin0 {
  uint64_t    min_seq_ = 0;
  byte_t      vtr_width_ = 0;
  byte_t      vtr_mask_ = 0;
  byte_t      vtr_num_ = 0;
  ValueType   vtr_to_vt_[13] = {};
public:
  ~ZTagArray() {
    this->risk_release_ownership();
  }
  void set_user_data(const byte_t* base, const TableMultiPartInfo::KeyValueOffset& kvo) {
    TERARK_VERIFY_LE(kvo.vtr_num, sizeof(vtr_to_vt_));
    memset(vtr_to_vt_, 255, sizeof(vtr_to_vt_)); // set as invalid
    vtr_width_ = compute_uintbits(kvo.vtr_num - 1);
    vtr_mask_ = byte_t((1 << vtr_width_) - 1);
    vtr_num_ = kvo.vtr_num;
    min_seq_ = kvo.min_seq;
    auto tag_width = kvo.seq_width + vtr_width_;
    UintVecMin0::risk_set_data((byte_t*)base, kvo.tag_num, tag_width);
    TERARK_VERIFY_EQ(UintVecMin0::mem_size(), kvo.tag_bytes);
    memcpy(vtr_to_vt_, base + UintVecMin0::mem_size(), kvo.vtr_num);
  }
  uint64_t operator[](size_t tagId) const {
    uint64_t raw = this->get(tagId);
    uint64_t vtr = raw & vtr_mask_; TERARK_ASSERT_LT(vtr, vtr_num_);
    uint64_t vt  = vtr_to_vt_[vtr];
    uint64_t seq = min_seq_ + (raw >> vtr_width_);
    return seq << 8 | vt;
  }
  void GetSequenceAndType(size_t tagId, uint64_t* seq, ValueType* vt) const {
    uint64_t raw = this->get(tagId);
    uint64_t vtr = raw & vtr_mask_; TERARK_ASSERT_LT(vtr, vtr_num_);
    *vt  = vtr_to_vt_[vtr];
    *seq = min_seq_ + (raw >> vtr_width_);
  }
  ValueType vtr_to_vt(size_t vtr) const {
    TERARK_ASSERT_LT(vtr, vtr_num_);
    return vtr_to_vt_[vtr];
  }
  size_t vtr_num() const { return vtr_num_; }
  size_t vtr_width() const { return vtr_width_; }
  size_t seq_width() const { return uintbits() - vtr_width_; }
  uint64_t min_seq() const { return min_seq_; }
};

struct ToplingZipSubReader {
  size_t rawReaderOffset_;
  size_t rawReaderSize_;
  size_t estimateUnzipCap_;
  byte_t prefix_data_[ToplingZipTableOptions::MAX_PREFIX_LEN];
  byte_t prefix_len_;
  bool   storeUsePread_ : 1;
  bool   zeroCopy_ : 1;
  signed char debugLevel_;
  TagRsKind  tag_rs_kind_ = TagRsKind(255);
  uint32_t   keyRankCacheStep_ = 0;
  intptr_t   storeFD_;
  FSRandomAccessFile* storeFileObj_;
  Statistics* stats_;
  size_t storeOffset_;
  fstring prefix() const { return fstring(prefix_data_, prefix_len_); }
  COIndexUP index_;
  unique_ptr<AbstractBlobStore> store_;
  ZTagArray tags_array_;
  struct legacy_zv {
    bitfield_array<2> type_;
    uint64_t type_histogram_[4];
  };
  struct krs0_type { // key rs all 0
    rank_select_allzero krs_;
    union { // seq rs
      rank_select_il_256  trsn_; // RS_Key0_TagN
      rank_select_allzero trs0_; // RS_Key0_Tag0
      rank_select_allone  trs1_; // RS_Key0_Tag1
    };
    krs0_type() {}
    ~krs0_type() {}
  };
  struct krsn_type { // key rs normal + trs0/1
    rank_select_il_256 krs_; // key rs
    union { // seq rs
    //rank_select_il_256  trsn_; // RS_KeyN_TagN, use mixed instead
    //rank_select_allzero trs0_; // RS_KeyN_Tag0, will not happen
      rank_select_allone  trs1_; // RS_KeyN_Tag1
    };
    krsn_type() {}
    ~krsn_type() {}
  };
  // RS_KeyN_TagN: rank_select_mixed is more CPU cache friendly
  struct krsx_type : rank_select_mixed_xl_256_0 {
    const auto& trs() const { return this->get<1>(); }
  };
  union u_key_tag_type {
    legacy_zv  legacy_;
    krs0_type  krs0_;
    krsn_type  krsn_;
    krsx_type  krsx_;
    byte_t     u_data_[1];
    u_key_tag_type() {
      memset(u_data_, 0, sizeof(u_key_tag_type));
    }
    ~u_key_tag_type() {}
  };
  u_key_tag_type         u_key_tag_;
#define type_            u_key_tag_.legacy_.type_
#define type_histogram_  u_key_tag_.legacy_.type_histogram_
#define krs0_            u_key_tag_.krs0_
#define krsn_            u_key_tag_.krsn_
#define krsx_            u_key_tag_.krsx_

  union { // for fast ApproximateOffsetOf
    DoSortedStrVec varCacheKeys_;
    FixedLenStrVec fixCacheKeys_;
  };
  KeyRankCacheEntry krce_{};

  void InitKeyRankCache(const void*, const KeyRankCacheEntry&);
  void InitUsePread(int minPreadLen);
  size_t StoreDataSize() const;

  void GetRecordAppend(size_t recId, valvec<byte_t>* tbuf, bool async) const;
  void IterGetRecordAppend(size_t recId, BlobStore::CacheOffsets*) const;

  Status Get(const ReadOptions&, SequenceNumber, const Slice& key, GetContext*) const;
  size_t ApproximateRank(fstring key) const;

  Status GetZV(const ReadOptions&, SequenceNumber, const Slice& key, GetContext*) const;
  template<class KeyRS, class TagRS>
  Status GetRS(const ReadOptions&, SequenceNumber, const Slice& key, GetContext*,
               const KeyRS&, const TagRS&) const;

  bool PointGet(const Slice& key, valvec<byte_t>* val) const;
  bool PointGetZV(const Slice& key, valvec<byte_t>* val) const;
  template<class KeyRS, class TagRS>
  bool PointGetRS(const Slice& key, valvec<byte_t>* val, const KeyRS&, const TagRS&) const;

  void InitKeyTags(byte_t* tag_rs_base, size_t keyNum, size_t valueNum,
                   const TableMultiPartInfo::KeyValueOffset& kvo) {
    TERARK_VERIFY_LE(keyNum, valueNum);
    tag_rs_kind_ = TagRsKind(kvo.tag_rs_kind);
    auto tags_array_base = tag_rs_base + align_up(kvo.rs_bytes, 16);
    switch (tag_rs_kind_) {
    case LegacyZvType:
      ROCKSDB_VERIFY_EQ(keyNum, valueNum);
      if (kvo.type > 0) {
        TERARK_VERIFY_EQ(bitfield_array<2>::compute_mem_size(keyNum), kvo.type);
        if (kvo.type_histogram[int(ZipValueType::kZeroSeq)] != keyNum) {
          type_.risk_set_data(tag_rs_base, keyNum);
        } else {
          goto RegardAs_RS_Key0_Tag0;
        }
      }
      else { // data format is compatible to RS_Key0_Tag0
      RegardAs_RS_Key0_Tag0:
        TERARK_VERIFY_EQ(kvo.type_histogram[int(ZipValueType::kZeroSeq)], keyNum);
        tag_rs_kind_ = RS_Key0_Tag0;
        krs0_.krs_  = rank_select_allzero(valueNum + 1);
        krs0_.trs0_ = rank_select_allzero(valueNum);
        return;
      }
      static_assert(sizeof(type_histogram_) == sizeof(kvo.type_histogram));
      memcpy(type_histogram_, kvo.type_histogram, sizeof(type_histogram_));
      return;
    case RS_Key0_TagN:
      TERARK_VERIFY_AL(kvo.rs_bytes, 8);
      krs0_.krs_  = rank_select_allzero(valueNum + 1);
      krs0_.trsn_.risk_mmap_from(tag_rs_base, kvo.rs_bytes);
      tags_array_.set_user_data(tags_array_base, kvo);
      ROCKSDB_VERIFY_EQ(krs0_.trsn_.size(), valueNum);
      ROCKSDB_VERIFY_EQ(krs0_.trsn_.max_rank1(), kvo.tag_num);
      return;
    case RS_Key0_Tag0:
      TERARK_VERIFY_AL(kvo.rs_bytes, 8);
      krs0_.krs_  = rank_select_allzero(valueNum + 1);
      krs0_.trs0_ = rank_select_allzero(valueNum);
      tags_array_.set_user_data(tags_array_base, kvo);
      ROCKSDB_VERIFY_EQ(krs0_.trs0_.max_rank1(), kvo.tag_num);
      return;
    case RS_Key0_Tag1:
      TERARK_VERIFY_AL(kvo.rs_bytes, 8);
      krs0_.krs_  = rank_select_allzero(valueNum + 1);
      krs0_.trs1_ = rank_select_allone (valueNum);
      tags_array_.set_user_data(tags_array_base, kvo);
      ROCKSDB_VERIFY_EQ(krs0_.trs1_.max_rank1(), kvo.tag_num);
      return;
    case RS_KeyN_TagN:
      TERARK_VERIFY_AL(kvo.rs_bytes, 8);
      krsx_.risk_mmap_from(tag_rs_base, kvo.rs_bytes);
      tags_array_.set_user_data(tags_array_base, kvo);
      ROCKSDB_VERIFY_EQ(krsx_.size(), valueNum + 1);
      ROCKSDB_VERIFY_EQ(krsx_.trs().max_rank1(), kvo.tag_num);
      return;
    case RS_KeyN_Tag1:
      TERARK_VERIFY_AL(kvo.rs_bytes, 8);
      krsn_.krs_.risk_mmap_from(tag_rs_base, kvo.rs_bytes);
      krsn_.trs1_ = rank_select_allone(valueNum);
      tags_array_.set_user_data(tags_array_base, kvo);
      ROCKSDB_VERIFY_EQ(krsn_.krs_.size(), valueNum + 1);
      ROCKSDB_VERIFY_EQ(krsn_.trs1_.max_rank1(), kvo.tag_num);
      return;
    }
    ROCKSDB_DIE("Bad tag_rs_kind = %d", tag_rs_kind_);
  }

  bool IsOneValuePerUK() const {
    switch (tag_rs_kind_) {
    case LegacyZvType: return index_->NumKeys() == type_histogram_[int(ZipValueType::kZeroSeq)] || type_.size() == 0;
    case RS_Key0_TagN: return true;
    case RS_Key0_Tag0: return true;
    case RS_Key0_Tag1: return true;
    case RS_KeyN_TagN: return false;
    case RS_KeyN_Tag1: return false;
    }
    ROCKSDB_DIE("Bad tag_rs_kind = %d", tag_rs_kind_);
  }

  bool IsWireSeqNumAllZero() const {
    switch (tag_rs_kind_) {
    case LegacyZvType: return index_->NumKeys() == type_histogram_[int(ZipValueType::kZeroSeq)] || type_.size() == 0;
    case RS_Key0_TagN: return false;
    case RS_Key0_Tag0: return true;
    case RS_Key0_Tag1: return false;
    case RS_KeyN_TagN: return false;
    case RS_KeyN_Tag1: return false;
    }
    ROCKSDB_DIE("Bad tag_rs_kind = %d", tag_rs_kind_);
  }

  RandomAccessFileReader* fileReader_; // for get file path name

  ToplingZipSubReader() {}
  ~ToplingZipSubReader();
};

class TooZipTableReaderBase : public TopTableReaderBase {
public:
  TooZipTableReaderBase(const ToplingZipTableFactory*);
  ~TooZipTableReaderBase();
  void ReadGlobalDict(const TableReaderOptions&);
  void SetupForCompaction() override;
  virtual void SetupForRandomRead() = 0; // revert SetupForCompaction
  Status DumpTable(WritableFile* /*out_file*/) override;

  void SetCompressionOptions(const ToplingZipSubReader* subs, size_t num) {
    auto props = this->table_properties_.get();
    props->compression_options.clear();
    for (size_t i = 0; i < num; ++i) {
      Slice rsKind = enum_name(subs[i].tag_rs_kind_);
      props->compression_options.append(rsKind.data(), rsKind.size());
      props->compression_options.append("-");
      props->compression_options.append(subs[i].index_->Name());
      props->compression_options.append("-");
      props->compression_options.append(subs[i].store_->name());
      props->compression_options.append(";");
    }
    props->compression_options.pop_back(); // remove last ';'
  }

  // must be called after ReadGlobalDict
  void ConfigureHugePage(const ToplingZipSubReader* parts,
                         const TableMultiPartInfo& offsetInfo,
                         const TableReaderOptions& tro) {
  #if defined(__linux__)
    if (!table_factory_->table_options_.indexMemAsHugePage) {
      return;
    }
    auto* log = tro.ioptions.info_log.get();
    const size_t partCount = offsetInfo.prefixSet_.size();
    size_t nBytes = pow2_align_up(dict_.size(), 64);
    for (size_t i = 0; i < partCount; i++) {
      auto& curr = offsetInfo.offset_[i];
      nBytes += curr.key + curr.type;
    }
    for (size_t i = 0; i < partCount; i++) {
      for (auto& block : parts[i].store_->get_meta_blocks())
        nBytes += pow2_align_up(block.data.size(), 64);
    }
    if (nBytes < hugepage_size / 2) {
      return;
    }
    byte_t* amem = NULL;
    int err = posix_memalign((void**)&amem, hugepage_size, nBytes);
    if (err) {
      WARN(log, "posix_memalign(%zd, %zd) = %m", hugepage_size, nBytes);
      return;
    }
    err = madvise(amem, nBytes, MADV_HUGEPAGE);
    if (err) {
      WARN(log, "madvise(MADV_HUGEPAGE, size=%zd[0x%zX]) = %m", nBytes, nBytes);
      free(amem);
      return;
    }
    hugepage_mem_.risk_set_data(amem);
    hugepage_mem_.risk_set_capacity(nBytes);
    if (dict_.size()) {
      hugepage_mem_.unchecked_append(dict_.data(), dict_.size());
      hugepage_mem_.risk_set_size(pow2_align_up(dict_.size(), 64));
      if (dict_zip_size_) {
        free((void*)dict_.data());
      } else {
        MmapColdize(dict_);
      }
      dict_.data_ = (const char*)hugepage_mem_.data();
    }
  #endif
  }

  void LoadStore(ToplingZipSubReader*, TableMultiPartInfo&,
                 const AbstractBlobStore::Dictionary&);
  size_t InitPart(size_t nth, size_t offset, ToplingZipSubReader&,
                  TableMultiPartInfo&, const TableReaderOptions&,
                  const AbstractBlobStore::Dictionary&);

  using TopTableReaderBase::isReverseBytewiseOrder_; // make public
  Slice  dict_; // unzipped
  size_t dict_zip_size_;
  const ToplingZipTableFactory* table_factory_;
  valvec<byte_t> hugepage_mem_;
  intptr_t seqscan_iter_cnt_ = 0;
  mutable std::mutex seqscan_iter_mtx_;
};

static const size_t kMaxNumAnchors = 128;

class ToplingZipTableReader : public TooZipTableReaderBase {
public:
  InternalIterator*
  NewIterator(const ReadOptions&, const SliceTransform*, Arena*,
              bool skip_filters, TableReaderCaller,
              size_t compaction_readahead_size,
              bool allow_unprepared_value) override;

  template<bool reverse, bool ZipOffset, class IterOpt>
  InternalIterator* NewIteratorImp(const ReadOptions&, Arena*);

  Status Get(const ReadOptions&, const Slice& key, GetContext*,
             const SliceTransform*, bool skip_filters) override;

  uint64_t ApproximateOffsetOf(ROCKSDB_8_X_COMMA(const ReadOptions& readopt) const Slice&, TableReaderCaller) override;
  uint64_t ApproximateSize(ROCKSDB_8_X_COMMA(const ReadOptions& readopt) const Slice&, const Slice&, TableReaderCaller) override;
  Status ApproximateKeyAnchors(const ReadOptions&, std::vector<Anchor>&) override;

  ~ToplingZipTableReader() override;
  ToplingZipTableReader(const ToplingZipTableFactory*);
  void Open(RandomAccessFileReader*, Slice file_data, const TableReaderOptions&, TableMultiPartInfo&);
  void SetupForRandomRead() override;
  std::string ToWebViewString(const json& dump_options) const override;
private:
  ToplingZipSubReader subReader_;
};

class ToplingZipTableMultiReader : public TooZipTableReaderBase {
public:
  InternalIterator*
  NewIterator(const ReadOptions&, const SliceTransform*, Arena*,
              bool skip_filters, TableReaderCaller,
              size_t compaction_readahead_size,
              bool allow_unprepared_value) override;

  template<bool reverse, bool ZipOffset, class IterOpt>
  InternalIterator* NewIteratorImp(const ReadOptions&, Arena*);

  Status Get(const ReadOptions&, const Slice& key, GetContext*,
             const SliceTransform*, bool skip_filters) override;

  uint64_t ApproximateOffsetOf(ROCKSDB_8_X_COMMA(const ReadOptions& readopt) const Slice&, TableReaderCaller) override;
  uint64_t ApproximateSize(ROCKSDB_8_X_COMMA(const ReadOptions& readopt) const Slice&, const Slice&, TableReaderCaller) override;
  Status ApproximateKeyAnchors(const ReadOptions&, std::vector<Anchor>&) override;

  ~ToplingZipTableMultiReader() override;
  ToplingZipTableMultiReader(const ToplingZipTableFactory*);
  void Open(RandomAccessFileReader*, Slice file_data, const TableReaderOptions&, TableMultiPartInfo&);
  void SetupForRandomRead() override;

  FixedLenStrVec prefixSet_;
  valvec<ToplingZipSubReader> subReader_;
  bool hasAnyZipOffset_;

  void Init(TableMultiPartInfo&, const TableReaderOptions&);
  size_t GetSubCount() const { return prefixSet_.size(); }
  const ToplingZipSubReader* GetSubReader(size_t i) const { return &subReader_[i]; }
  const ToplingZipSubReader* LastSubReader() const { return &subReader_.back(); }
  const ToplingZipSubReader* LowerBoundSubReader(fstring key) const;
  bool HasAnyZipOffset() const { return hasAnyZipOffset_; }
  bool IterIsLegacyZV() const;
  bool IsOneValuePerUK() const {
    for (auto& sub : subReader_) {
      if (!sub.IsOneValuePerUK())
        return false;
    }
    return true;
  }
  json ToWebViewJson(const json& dump_options) const;
  std::string ToWebViewString(const json& dump_options) const override;
};

struct IterOptLegacyZV : public InternalIterator {
  Slice  user_value_;
  static constexpr bool is_legacy_zv = true;
};
struct IterOptTagRS : public InternalIterator {
  static constexpr bool is_legacy_zv = false;
};
template<class Base>
struct MultiValuePerUK : public Base {
  uint32_t  value_count_ = 0;
  uint32_t  value_index_ = 0;
  static constexpr bool is_one_value_per_uk = false;
};
template<class Base>
struct OneValuePerUK : public Base {
  static constexpr uint32_t value_count_ = 1;
  static constexpr uint32_t value_index_ = 0;
  static constexpr bool is_one_value_per_uk = true;
};
template<class IterOpt>
class ToplingZipTableIterBase : public IterOpt {
public:
  using IterBase = ToplingZipTableIterBase;
  using IterOpt::is_legacy_zv;
  using IterOpt::value_count_;
  using IterOpt::value_index_;
  const TooZipTableReaderBase* table_reader_;
  const ToplingZipSubReader* subReader_;
  PinnedIteratorsManager*   pinned_iters_mgr_;
  COIndex::Iterator*        iter_;
  uint32_t                  min_prefault_pages_;
  SequenceNumber            global_tag_;
  Status                    status_;
  size_t                    seek_cnt_ = 0;
  size_t                    next_cnt_ = 0;
  size_t                    prev_cnt_ = 0;
  size_t                    iter_key_len_ = 0;
  size_t                    iter_val_len_ = 0;
  union {
    uint32_t                rs_bitpos_; // just for RS
    ZipValueType            zip_value_type_; // just for LegacyZvType
  };
  TagRsKind                 tag_rs_kind_;
  bool is_value_loaded_ = false;
  bool is_seqscan_ = false;
  PinIterMgrBase dummy_pin_mgr_;
  static_assert(sizeof(dummy_pin_mgr_) == 1);

  BlobStore::CacheOffsets* cache_offsets() const {
    return (BlobStore::CacheOffsets*)(this + 1); // IterZO::rb_
  }
  valvec<byte_t>& ValueBuf() const { return cache_offsets()->recData; }

  void inc_seqscan(TooZipTableReaderBase* r) {
    if (g_tls_is_seqscan) {
      is_seqscan_ = true;
      std::lock_guard<std::mutex> lock(r->seqscan_iter_mtx_);
      if (1 == ++r->seqscan_iter_cnt_)
        r->SetupForCompaction();
    }
  }
  void dec_seqscan() {
    if (is_seqscan_) {
      auto r = const_cast<TooZipTableReaderBase*>(table_reader_);
      std::lock_guard<std::mutex> lock(r->seqscan_iter_mtx_);
      if (0 == --r->seqscan_iter_cnt_)
        r->SetupForRandomRead();
    }
  }

  ~ToplingZipTableIterBase() override {
    ROCKSDB_VERIFY(nullptr != subReader_);
    ROCKSDB_VERIFY(nullptr != table_reader_);
    auto r = static_cast<const TooZipTableReaderBase*>(table_reader_);
    auto f = r->table_factory_;
    as_atomic(f->live_iter_num).fetch_sub(1, std::memory_order_relaxed);
    as_atomic(f->iter_seek_cnt).fetch_add(seek_cnt_, std::memory_order_relaxed);
    as_atomic(f->iter_next_cnt).fetch_add(next_cnt_, std::memory_order_relaxed);
    as_atomic(f->iter_prev_cnt).fetch_add(prev_cnt_, std::memory_order_relaxed);
    as_atomic(f->iter_key_len).fetch_add(iter_key_len_, std::memory_order_relaxed);
    as_atomic(f->iter_val_len).fetch_add(iter_val_len_, std::memory_order_relaxed);
    dec_seqscan();
    iter_->Delete();
    auto old_live_num = as_atomic(r->live_iter_num_).fetch_sub(1, std::memory_order_relaxed);
    TERARK_VERIFY_GE(old_live_num, 1);
  }
  ToplingZipTableIterBase(const ToplingZipSubReader* subReader,
                          TooZipTableReaderBase* r,
                          const ReadOptions& ro, SequenceNumber global_seqno)
    : global_tag_(PackSequenceAndType(global_seqno, kTypeValue))
  {
    table_reader_ = r;
    subReader_ = subReader;
    iter_ = subReader_->index_->NewIterator();
    iter_->SetInvalid();
    min_prefault_pages_ = ro.min_prefault_pages;
    tag_rs_kind_ = subReader->tag_rs_kind_;
    pinned_iters_mgr_ = static_cast<PinnedIteratorsManager*>(&dummy_pin_mgr_);
    rs_bitpos_ = -1;
//  zip_value_type_ = ZipValueType(255); // NOLINT
    inc_seqscan(r);
    auto* f = r->table_factory_;
    as_atomic(r->live_iter_num_).fetch_add(1, std::memory_order_relaxed);
    as_atomic(f->cumu_iter_num).fetch_add(1, std::memory_order_relaxed);
    as_atomic(f->live_iter_num).fetch_add(1, std::memory_order_relaxed);
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) final {
    pinned_iters_mgr_ = pinned_iters_mgr ? :
        static_cast<PinnedIteratorsManager*>(&dummy_pin_mgr_);
  }

  bool Valid() const final { return UINT32_MAX != rs_bitpos_; }

  void SeekToFirst() override {
    if (UnzipIterRecord(IndexIterSeekToFirst())) {
      DecodeCurrKeyTag();
    }
  }

  void SeekToLast() override {
    if (UnzipIterRecord(IndexIterSeekToLast())) {
      if constexpr (!this->is_one_value_per_uk) {
        value_index_ = value_count_ - 1;
      }
      DecodeCurrKeyTag();
    }
  }

  void Seek(const Slice& target) override {
    ROCKSDB_ASSERT_GE(target.size(), kNumInternalBytes);
    ParsedInternalKey pikey(target);
    seek_cnt_++;
    SeekInternal(pikey);
  }

  Slice key() const noexcept final {
    assert(nullptr != iter_);
    assert(iter_->Valid());
    fstring uk = iter_->key();
    return Slice(uk.p, uk.n + 8);
  }

  Slice user_key() const noexcept final {
    assert(nullptr != iter_);
    assert(iter_->Valid());
    fstring uk = iter_->key();
    return Slice(uk.p, uk.n);
  }

  Slice value() const noexcept final {
    assert(nullptr != iter_);
    assert(iter_->Valid());
  #if 0
    assert(is_value_loaded_);
  #else
    // NewIterator() has param `allow_unprepared_value`, which is used for
    // decide whether call PrepareValue() in Next(), thus can check status
    // when Valid() returns false.
    //
    // 1. `allow_unprepared_value` is true  in DBIter
    // 2. `allow_unprepared_value` is false in Flush and Compaction
    //
    // The `allow_unprepared_value` increases non-essential complexity,
    // So we ignore `allow_unprepared_value` and handle lazy load here:
    //
    // 1. In DBIter, this lazy load will not be triggered
    // 2. In Flush and Compaction, this lazy load will be triggered, when
    //    PrepareValue() failed, it is a fatal error and data is not
    //    recoverable in most cases, we just die in such cases.
    //
    if (UNLIKELY(!is_value_loaded_)) {
      if (UNLIKELY(!const_cast<IterBase*>(this)->PrepareValue())) {
        TERARK_DIE_S("PrepareValue = %s", status_.ToString());
      }
    }
  #endif
    if constexpr (is_legacy_zv) {
      return this->user_value_;
    } else {
      return SliceOf(ValueBuf());
    }
  }

  Status status() const noexcept final { return status_; }

  bool IsKeyPinned() const final { return false; }
  bool IsValuePinned() const final {
    return pinned_iters_mgr_->PinningEnabled();
  }

  void SetIterInvalid() noexcept {
    iter_->SetInvalid();
    rs_bitpos_ = UINT32_MAX;
    invalidate_offsets_cache();
    if constexpr (!this->is_one_value_per_uk) {
      value_index_ = 0;
      value_count_ = 0;
    }
  }
  virtual void invalidate_offsets_cache() noexcept = 0;

  void TryPinBuffer(valvec<byte_t>& buf) {
    if (buf.capacity()) { // check capacity first for speed zero copy
      if (!buf.empty()) {
        if (pinned_iters_mgr_->PinningEnabled()) {
          pinned_iters_mgr_->PinPtr(buf.data(), free);
          buf.risk_release_ownership();
        } else {
          buf.risk_set_size(0);
        }
      }
    }
  }
  bool UnzipIterRecord(bool hasRecord) {
    if (hasRecord) {
      size_t recId = iter_->id();
      auto& r = *subReader_;
      if constexpr (!this->is_one_value_per_uk) {
        value_index_ = 0;
      }
      switch (tag_rs_kind_) {
      case LegacyZvType:
       if constexpr (is_legacy_zv) {
        is_value_loaded_ = false;
        zip_value_type_ = r.type_.size()
          ? ZipValueType(r.type_[recId])
          : ZipValueType::kZeroSeq;
        if (ZipValueType::kZeroSeq == zip_value_type_) {
          // do not fetch value
          if constexpr (!this->is_one_value_per_uk) {
            value_count_ = 1;
          }
          return true;
        } else { // must fetch values because seqnum is stored in value
          return FetchValueZV();
        }
       } else {
        ROCKSDB_DIE("tag_rs_kind_ must not be LegacyZvType");
       }
      case RS_Key0_TagN:
      case RS_Key0_Tag0:
      case RS_Key0_Tag1:
        rs_bitpos_   = r.krs0_.krs_.select0(recId);
        if constexpr (!this->is_one_value_per_uk) {
          value_count_ = r.krs0_.krs_.one_seq_len(rs_bitpos_ + 1) + 1;
        }
        return true;
      case RS_KeyN_TagN:
        rs_bitpos_   = r.krsx_.select0(recId);
        if constexpr (!this->is_one_value_per_uk) {
          value_count_ = r.krsx_.one_seq_len(rs_bitpos_ + 1) + 1;
        }
        return true;
      case RS_KeyN_Tag1:
        rs_bitpos_   = r.krsn_.krs_.select0(recId);
        if constexpr (!this->is_one_value_per_uk) {
          value_count_ = r.krsn_.krs_.one_seq_len(rs_bitpos_ + 1) + 1;
        }
        return true;
      }
      ROCKSDB_DIE("Bad tag_rs_kind_ = %d", tag_rs_kind_);
    }
    else {
      SetIterInvalid();
      return false;
    }
  }
  bool FetchValue() {
    if constexpr (is_legacy_zv) {
      if (LegacyZvType == tag_rs_kind_) {
        return FetchValueZV();
      } else {
        return FetchValueRS();
      }
    } else {
      ROCKSDB_ASSERT_NE(LegacyZvType, tag_rs_kind_);
      return FetchValueRS();
    }
  }
  inline bool FetchValueRS() {
    assert(!is_value_loaded_);
    auto op = ValueType(iter_->key().end()[0]); // ValueType byte
    auto& value_buf = ValueBuf();
    if (kTypeDeletion == op || kTypeSingleDeletion == op) {
      if constexpr (is_legacy_zv) {
        this->user_value_ = "";
      } else {
        value_buf.risk_set_size(0);
      }
    } else {
      size_t valueId = rs_bitpos_ + value_index_;
      TryPinBuffer(value_buf);
     #if defined(TOP_ZIP_TABLE_KEEP_EXCEPTION)
      subReader_->IterGetRecordAppend(valueId, cache_offsets());
      TryWarmupZeroCopy(value_buf, min_prefault_pages_);
     #else
      try {
        subReader_->IterGetRecordAppend(valueId, cache_offsets());
        TryWarmupZeroCopy(value_buf, min_prefault_pages_);
      }
      catch (const std::exception& ex) { // crc checksum error
        if (table_reader_->table_factory_->table_options_.debugLevel > 0) {
          ROCKSDB_DIE("ex.what = %s", ex.what());
        }
        SetIterInvalid();
        status_ = Status::Corruption("IterGetRecordAppend()", ex.what());
        return false;
      }
     #endif
      if constexpr (is_legacy_zv) {
        this->user_value_ = SliceOf(value_buf);
      }
    }
    is_value_loaded_ = true;
    return true;
  }
  inline bool FetchValueZV() {
    assert(!is_value_loaded_);
    auto& value_buf = ValueBuf();
    TryPinBuffer(value_buf);
    size_t recId = iter_->id();
    size_t mulnum_size = 0;
    if (ZipValueType::kMulti == zip_value_type_) {
      mulnum_size = sizeof(uint32_t); // for offsets[valnum_]
    }
    if (subReader_->zeroCopy_) {
      if (value_buf.capacity()) // recent accessed value is kMulti
        valvec<byte_t>().swap(value_buf);
      else
        value_buf.risk_release_ownership();
    } else {
      value_buf.ensure_capacity(subReader_->estimateUnzipCap_ + mulnum_size);
      value_buf.resize_no_init(mulnum_size);
    }
   #if defined(TOP_ZIP_TABLE_KEEP_EXCEPTION)
    subReader_->IterGetRecordAppend(recId, cache_offsets());
    TryWarmupZeroCopy(value_buf, min_prefault_pages_);
   #else
    try {
      subReader_->IterGetRecordAppend(recId, cache_offsets());
      TryWarmupZeroCopy(value_buf, min_prefault_pages_);
    }
    catch (const std::exception& ex) { // crc checksum error
      if (table_reader_->table_factory_->table_options_.debugLevel > 0) {
        ROCKSDB_DIE("ex.what = %s", ex.what());
      }
      SetIterInvalid();
      status_ = Status::Corruption("IterGetRecordAppend()", ex.what());
      return false;
    }
   #endif
    if (!this->is_one_value_per_uk && ZipValueType::kMulti == zip_value_type_) {
      if (value_buf.capacity() == 0) {
        TERARK_VERIFY(subReader_->zeroCopy_);
        byte_t* buf_copy = (byte_t*)malloc(sizeof(uint32_t) + value_buf.size());
        memcpy(buf_copy + sizeof(uint32_t), value_buf.data(), value_buf.size());
        value_buf.risk_set_data(buf_copy);
        value_buf.risk_set_size(sizeof(uint32_t) + value_buf.size());
        value_buf.risk_set_capacity(value_buf.size());
      }
      TERARK_ASSERT_GE(value_buf.size(), sizeof(uint32_t) + 8);
      if constexpr (!this->is_one_value_per_uk) {
        // ^^^^^^^ to avoid compile error
        ZipValueMultiValue::decode(value_buf, &value_count_);
      }
      TERARK_ASSERT_GE(value_count_, 1);
      iter_key_len_ += (iter_->key().size() + 8) * value_count_;
    } else {
      if constexpr (!this->is_one_value_per_uk) {
        value_count_ = 1;
      }
      iter_key_len_ += (iter_->key().size() + 8);
    }
    iter_val_len_ += value_buf.size() - mulnum_size;
    if constexpr (!this->is_one_value_per_uk) {
      value_index_ = 0;
    }
    is_value_loaded_ = true;
    return true;
  }
  bool PrepareValue() final {
    if (LIKELY(!is_value_loaded_)) {
      if (UNLIKELY(!FetchValue())) return false;
    }
    if constexpr (is_legacy_zv) {
      if (UNLIKELY(LegacyZvType == tag_rs_kind_)) {
        LegacyDecodeCurrTagAndValue();
      } else {
        // all works were done in FetchValue
      }
    }
    return true;
  }
  bool PrepareAndGetValue(Slice* v) final {
    if (LIKELY(!is_value_loaded_)) {
      if (UNLIKELY(!FetchValue())) return false;
    }
    if constexpr (is_legacy_zv) {
      if (UNLIKELY(LegacyZvType == tag_rs_kind_)) {
        LegacyDecodeCurrTagAndValue();
      } else {
        // all works were done in FetchValue
      }
      *v = this->user_value_;
    } else {
      *v = SliceOf(ValueBuf());
    }
    return true;
  }
  terark_flatten void DecodeCurrKeyTag() {
    TERARK_ASSERT_S(status_.ok(), "%s", status_.ToString());
    TERARK_ASSERT_LT(iter_->id(), subReader_->index_->NumKeys());
    const auto& r = *subReader_;
    switch (tag_rs_kind_) {
      case LegacyZvType: return DecodeCurrKeyTagZV();
      case RS_Key0_TagN: return DecodeCurrKeyTagRS(r.krs0_.trsn_);
      case RS_Key0_Tag0: return DecodeCurrKeyTagRS(r.krs0_.trs0_);
      case RS_Key0_Tag1: return DecodeCurrKeyTagRS(r.krs0_.trs1_);
      case RS_KeyN_TagN: return DecodeCurrKeyTagRS(r.krsx_.trs());
      case RS_KeyN_Tag1: return DecodeCurrKeyTagRS(r.krsn_.trs1_);
    }
    // should not goes here
    ROCKSDB_DIE("Bad tag_rs_kind_ = %d", tag_rs_kind_);
  }
  template<class SeqRS>
  inline void DecodeCurrKeyTagRS(const SeqRS& trs) {
    TERARK_ASSERT_EQ(trs.size(), subReader_->store_->num_records());
    TERARK_ASSERT_LT(value_index_, value_count_);
    is_value_loaded_ = false;
    size_t bitpos = rs_bitpos_ + value_index_;
    TERARK_ASSERT_LT(bitpos, subReader_->store_->num_records());
    uint64_t key_tag;
    if (trs.is1(bitpos)) {
      size_t idx = trs.rank1(bitpos);
      key_tag = subReader_->tags_array_[idx];
    } else {
      key_tag = global_tag_;
    }
    EncodeFixed64((char*)iter_->key().end(), key_tag); // use extra 8 bytes
  }
  void DecodeCurrKeyTagZV() {
    if constexpr (!is_legacy_zv) {
      return;
    }
    if (ZipValueType::kZeroSeq == zip_value_type_) {
      TERARK_ASSERT_EQ(0, value_index_);
      TERARK_ASSERT_EQ(1, value_count_);
      uint64_t key_tag = global_tag_;
      EncodeFixed64((char*)iter_->key().end(), key_tag); // use extra 8 bytes
      // just decode key_tag, value may be not fetched until now
    } else { // decode value together
      LegacyDecodeCurrTagAndValue();
    }
  }
  void LegacyDecodeCurrTagAndValue() {
   if constexpr (is_legacy_zv) {
    assert(is_value_loaded_);
    assert(status_.ok());
    assert(iter_->id() < subReader_->index_->NumKeys());
    auto& value_buf = ValueBuf();
    uint64_t key_tag_;
    switch (zip_value_type_) {
    default:
      TERARK_DIE("bad zip_value_type_ = %d", int(zip_value_type_));
      break;
    case ZipValueType::kZeroSeq:
      assert(0 == value_index_);
      assert(1 == value_count_);
      key_tag_ = global_tag_;
      this->user_value_ = SliceOf(value_buf);
      break;
    case ZipValueType::kValue: // should be a kTypeValue, the normal case
      assert(0 == value_index_);
      assert(1 == value_count_);
      // little endian uint64_t
      key_tag_ = PackSequenceAndType(*(uint64_t*)value_buf.data() & kMaxSequenceNumber,
                                     kTypeValue);
      this->user_value_ = SubSlice(value_buf, 7);
      break;
    case ZipValueType::kDelete:
      assert(0 == value_index_);
      assert(1 == value_count_);
      // little endian uint64_t
      key_tag_ = PackSequenceAndType(*(uint64_t*)value_buf.data() & kMaxSequenceNumber,
                                     kTypeDeletion);
      this->user_value_ = SubSlice(value_buf, 7);
      break;
    case ZipValueType::kMulti: { // more than one value
      auto zmValue = (const ZipValueMultiValue*)value_buf.data();
      assert(0 != value_count_);
      assert(value_index_ < value_count_);
      TERARK_ASSERT_EQ(4*(value_count_ + 1) + zmValue->offsets[value_count_],
                       value_buf.size());
      Slice d = zmValue->getValueData(value_index_, value_count_);
      TERARK_ASSERT_GE(d.size(), sizeof(SequenceNumber));
      key_tag_ = unaligned_load<SequenceNumber>(d.data());
      d.remove_prefix(sizeof(SequenceNumber));
      this->user_value_ = d;
      break; }
    }
    // COIndex::Iterator::key() has 8+ bytes extra space for KeyTag
    EncodeFixed64((char*)iter_->key().end(), key_tag_); // use extra bytes
   }
  }

  bool PointGet(const Slice& ikey, bool fetch_val) override {
    return DoPointGet(subReader_, ikey, fetch_val);
  }
  inline bool DoPointGet(const ToplingZipSubReader* sub, const Slice& ikey, bool fetch_val) {
    if (fetch_val) {
      auto& buf = ValueBuf(); // reuse value buf
      buf.risk_set_size(0);
      if (sub->PointGet(ikey, &buf)) {
        if constexpr (is_legacy_zv) {
          this->user_value_ = SliceOf(buf);
        }
        is_value_loaded_ = true;
        return true;
      } else {
        return false;
      }
    } else {
      return sub->PointGet(ikey, nullptr);
    }
  }

  virtual bool IndexIterSeekToFirst() = 0;
  virtual bool IndexIterSeekToLast() = 0;
  virtual void SeekInternal(const ParsedInternalKey& pikey) = 0;
};

template<bool reverse, class IterOpt>
class ToplingZipTableIterator : public ToplingZipTableIterBase<IterOpt> {
public:
  using super = ToplingZipTableIterBase<IterOpt>;
  using super::super;
  using super::is_legacy_zv;
  using super::Next;
  using super::SeekForPrevImpl;
  using super::DecodeCurrKeyTag;
  using super::UnzipIterRecord;
  using super::iter_;
  using super::value_index_;
  using super::value_count_;

  void SeekForPrevAux(const Slice& target, const InternalKeyComparator& c) {
    SeekForPrevImpl(target, &c);
  }
  void SeekForPrev(const Slice& target) override {
    if (reverse)
      SeekForPrevAux(target, InternalKeyComparator(ReverseBytewiseComparator()));
    else
      SeekForPrevAux(target, InternalKeyComparator(BytewiseComparator()));
  }

protected:
  void SeekInternal(const ParsedInternalKey& pikey) final {
    // Damn MySQL-rocksdb may use "rev:" comparator
    fstring seek_key = pikey.user_key;
    bool ok = iter_->Seek(seek_key);
    int cmp; // fstring::compare3(iterKey, searchKey)
    if (reverse) {
      if (!ok) {
        // searchKey is reverse_bytewise less than all keys in database
        iter_->SeekToLast();
        assert(iter_->Valid()); // COIndex should not empty
        assert(iter_->key() < seek_key);
        ok = true;
        cmp = -1;
      }
      else {
        cmp = fstring_func::compare3()(iter_->key(), seek_key);
        if (cmp != 0) {
          assert(cmp > 0);
          iter_->Prev();
          ok = iter_->Valid();
        }
      }
    }
    else {
      if (ok)
        cmp = fstring_func::compare3()(iter_->key(), seek_key);
      else
        cmp = 0; // seek_key is larger than all iter_ key
    }
    if (UnzipIterRecord(ok)) {
      if (0 == cmp) {
        if constexpr (this->is_one_value_per_uk) {
          DecodeCurrKeyTag();
          uint64_t key_tag_ = unaligned_load<uint64_t>(iter_->key().end());
          if ((key_tag_ >> 8u) <= pikey.sequence) {
            return; // done
          }
        }
        else {
          value_index_ = -1;
          do {
            value_index_++;
            DecodeCurrKeyTag();
            uint64_t key_tag_ = unaligned_load<uint64_t>(iter_->key().end());
            if ((key_tag_ >> 8u) <= pikey.sequence) {
              return; // done
            }
          } while (value_index_ + 1 < value_count_);
        }
        // no visible version/sequence for target, use Next();
        // if using Next(), version check is not needed
        Next();
      }
      else {
        DecodeCurrKeyTag();
      }
    }
  }
  bool IndexIterSeekToFirst() override {
    if (reverse)
      return iter_->SeekToLast();
    else
      return iter_->SeekToFirst();
  }
  bool IndexIterSeekToLast() override {
    if (reverse)
      return iter_->SeekToFirst();
    else
      return iter_->SeekToLast();
  }
  inline bool IndexIterPrev() {
    if (reverse)
      return iter_->Next();
    else
      return iter_->Prev();
  }
  inline bool IndexIterNext() {
    if (reverse)
      return iter_->Prev();
    else
      return iter_->Next();
  }
};

template<bool reverse, class IterOpt>
class ToplingZipTableMultiIterator : public ToplingZipTableIterator<reverse, IterOpt> {
public:
  ToplingZipTableMultiIterator(ToplingZipTableMultiReader& table,
                               const ReadOptions& ro, SequenceNumber global_seqno)
    : ToplingZipTableIterator<reverse, IterOpt>
      (&table.subReader_[0], &table, ro, global_seqno)
  {}
protected:
  const ToplingZipTableMultiReader* reader() const {
    return static_cast<const ToplingZipTableMultiReader*>(this->table_reader_);
  }
  typedef ToplingZipTableIterator<reverse, IterOpt> base_t;
  using base_t::invalidate_offsets_cache;
  using base_t::iter_;
  using base_t::subReader_;
  using base_t::SeekInternal;
  using base_t::SetIterInvalid;

public:
  void Seek(const Slice& target) override {
    ROCKSDB_ASSERT_GE(target.size(), kNumInternalBytes);
    ParsedInternalKey pikey(target);
    this->seek_cnt_++;
    auto subReader = reader()->LowerBoundSubReader(pikey.user_key);
    if (subReader == nullptr) {
      SetIterInvalid();
      return;
    }
    ResetIter(subReader);
    if (!fstring(pikey.user_key).startsWith(subReader->prefix())) {
      if (reverse)
        SeekToAscendingLast();
      else
        SeekToAscendingFirst();
    }
    else {
      SeekInternal(pikey);
      assert(nullptr != iter_);
      if (!iter_->Valid()) {
        if (reverse) {
          if (subReader != reader()->GetSubReader(0)) {
            ResetIter(subReader - 1);
            SeekToAscendingLast();
          }
        }
        else {
          if (subReader != reader()->LastSubReader()) {
            ResetIter(subReader + 1);
            SeekToAscendingFirst();
          }
        }
      }
    }
  }

  bool PointGet(const Slice& ikey, bool fetch_val) override {
    auto sub = reader()->LowerBoundSubReader(ikey);
    if (sub == nullptr) {
      return false;
    }
    return this->DoPointGet(sub, ikey, fetch_val);
  }

protected:
  void SeekToAscendingFirst() {
    if (this->UnzipIterRecord(iter_->SeekToFirst())) {
      if constexpr (reverse && !this->is_one_value_per_uk)
        this->value_index_ = this->value_count_ - 1;
      this->DecodeCurrKeyTag();
    }
  }
  void SeekToAscendingLast() {
    if (this->UnzipIterRecord(iter_->SeekToLast())) {
      if constexpr (!reverse && !this->is_one_value_per_uk)
        this->value_index_ = this->value_count_ - 1;
      this->DecodeCurrKeyTag();
    }
  }
  void ResetIter(const ToplingZipSubReader* subReader) {
    if (subReader_ == subReader) {
      return;
    }
    this->TryPinBuffer(this->ValueBuf());
    if (this->ValueBuf().capacity())
      valvec<byte_t>().swap(this->ValueBuf());
    else
      this->ValueBuf().risk_release_ownership();
    subReader_ = subReader;
    this->tag_rs_kind_ = subReader->tag_rs_kind_;
    if constexpr (!reverse && !this->is_one_value_per_uk) {
      this->value_count_ = 0; // set invalid
    }
    iter_->Delete();
    iter_ = nullptr; //< for exception by NewIterator
    iter_ = subReader->index_->NewIterator();
    invalidate_offsets_cache();
  }
  bool IndexIterSeekToFirst() override {
    if (reverse) {
      ResetIter(reader()->LastSubReader());
      return iter_->SeekToLast();
    }
    else {
      ResetIter(reader()->GetSubReader(0));
      return iter_->SeekToFirst();
    }
  }
  bool IndexIterSeekToLast() override {
    if (reverse) {
      ResetIter(reader()->GetSubReader(0));
      return iter_->SeekToFirst();
    }
    else {
      ResetIter(reader()->LastSubReader());
      return iter_->SeekToLast();
    }
  }
  inline bool IndexIterPrev() {
    if (reverse) {
      if (iter_->Next())
        return true;
      if (subReader_ == reader()->LastSubReader())
        return false;
      ResetIter(subReader_ + 1);
      return iter_->SeekToFirst();
    }
    else {
      if (iter_->Prev())
        return true;
      if (subReader_ == reader()->GetSubReader(0))
        return false;
      ResetIter(subReader_ - 1);
      return iter_->SeekToLast();
    }
  }
  inline bool IndexIterNext() {
    if (reverse) {
      if (iter_->Prev())
        return true;
      if (subReader_ == reader()->GetSubReader(0))
        return false;
      ResetIter(subReader_ - 1);
      return iter_->SeekToLast();
    }
    else {
      if (iter_->Next())
        return true;
      if (subReader_ == reader()->LastSubReader())
        return false;
      ResetIter(subReader_ + 1);
      return iter_->SeekToFirst();
    }
  }
};

template<class Base>
class BaseIterZO : public Base {
protected:
  using Base::Base;
  using Base::iter_;
  using Base::next_cnt_;
  using Base::prev_cnt_;
  using Base::value_index_;
  using Base::value_count_;
  using Base::UnzipIterRecord;
  using Base::DecodeCurrKeyTag;
  terark_flatten
  void Next() final {
    assert(iter_->Valid());
    next_cnt_++;
    if constexpr (this->is_one_value_per_uk) {
      if (UnzipIterRecord(this->IndexIterNext())) {
        DecodeCurrKeyTag();
      }
    }
    else {
      value_index_++;
      if (value_index_ < value_count_) {
        DecodeCurrKeyTag();
      }
      else {
        if (UnzipIterRecord(this->IndexIterNext())) {
          DecodeCurrKeyTag();
        }
      }
    }
  }
  void Prev() final {
    assert(iter_->Valid());
    prev_cnt_++;
    if constexpr (this->is_one_value_per_uk) {
      if (UnzipIterRecord(this->IndexIterPrev())) {
        DecodeCurrKeyTag();
      }
    }
    else {
      if (value_index_ > 0) {
        value_index_--;
        DecodeCurrKeyTag();
      }
      else {
        if (UnzipIterRecord(this->IndexIterPrev())) {
          value_index_ = value_count_ - 1;
          DecodeCurrKeyTag();
        }
      }
    }
  }
  terark_flatten
  bool NextAndGetResult(IterateResult* result) noexcept final {
    this->Next();
    if (LIKELY(this->Valid())) {
      result->SetKey(this->key());
      result->bound_check_result = IterBoundCheck::kUnknown;
      if constexpr (Base::is_legacy_zv) {
        result->value_prepared = this->is_value_loaded_;
      } else {
        assert(!this->is_value_loaded_);
        result->value_prepared = false;
      }
      result->is_valid = true;
      return true;
    }
    result->is_valid = false;
    return false;
  }
};
template<class Base, bool ZipOffset>
class IterZO : public BaseIterZO<Base> {
  BlobStoreRecBuffer<ZipOffset> rb_;
public:
  using super = BaseIterZO<Base>;
  using super::super;
  void invalidate_offsets_cache() noexcept override {
    static_assert(offsetof(IterZO, rb_) == sizeof(typename Base::IterBase));
    rb_.invalidate_offsets_cache();
  }
};

void ToplingZipSubReader::InitKeyRankCache
(const void* mem, const KeyRankCacheEntry& krce) {
  TERARK_VERIFY_AL(size_t(mem), 64);
  krce_ = krce;
  if (0 == krce.cache_bytes) {
    //TODO: now we skip varlen key index's RankCache, but
    //      it has no FastApproximateRank
    //ROCKSDB_VERIFY_F(index_->HasFastApproximateRank(), "%s", index_->Name());
    return;
  }
  ROCKSDB_VERIFY_F(!index_->HasFastApproximateRank(), "%s", index_->Name());
  ROCKSDB_VERIFY(krce.num > 0);
  if (krce.fixed_suffix_len) {
    ROCKSDB_VERIFY_F(krce.cache_bytes == krce.num * krce.fixed_suffix_len,
                     "%s : %u %u %u", index_->Name(),
                     krce.cache_bytes ,  krce.num , krce.fixed_suffix_len);
    fixCacheKeys_.m_size = krce.num;
    fixCacheKeys_.m_fixlen = krce.fixed_suffix_len;
    fixCacheKeys_.m_strpool.risk_set_data((byte_t*)mem, krce.cache_bytes);
    fixCacheKeys_.m_strpool_mem_type = MemType::User;
    fixCacheKeys_.optimize_func();
  } else {
    new(&varCacheKeys_)decltype(varCacheKeys_)(valvec_no_init());
    varCacheKeys_.risk_set_data(krce.num, (void*)mem, krce.cache_bytes);
  }
  keyRankCacheStep_ = index_->NumKeys() / krce.num;
}

size_t ToplingZipSubReader::StoreDataSize() const {
  size_t dsize = 0;
  for (auto& block : store_->get_data_blocks()) {
    dsize += block.data.size();
  }
  return dsize;
}

void ToplingZipSubReader::InitUsePread(int minPreadLen) {
  if (minPreadLen < 0) {
    storeUsePread_ = false;
  }
  else if (minPreadLen == 0) {
    storeUsePread_ = true;
  }
  else {
    size_t numRecords = store_->num_records();
    size_t memSize = store_->get_mmap().size();
    storeUsePread_ = memSize > minPreadLen * numRecords &&
                     StoreDataSize() > rawReaderSize_ / 2;
  }
  this->zeroCopy_ = store_->support_zero_copy();
  double sumUnzipSize = store_->total_data_size();
  double avgUnzipSize = sumUnzipSize / store_->num_records();
  estimateUnzipCap_ = size_t(avgUnzipSize * 1.62); // a bit larger than 1.618
}

#define PREAD_CHECK_RESULT(len2) \
  if (UNLIKELY(size_t(len2) != len)) { \
    ROCKSDB_DIE("pread(%s : fd=%d, len=%zd, offset=%zd) = %zd : %s", \
      self->fileReader_->file_name().c_str(), \
      fd, len, offset, len2, strerror(errno)); \
  }
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

static const byte_t*
FsPread(void* vself, size_t offset, size_t len, valvec<byte_t>* buf) {
  auto self = (ToplingZipSubReader*)vself;
  buf->TERARK_IF_DEBUG(resize_fill(len, 0xCC), resize_no_init(len));
  auto bufdata = buf->data();
  if (UNLIKELY(0 == len)) {
    return bufdata;
  }
 #if defined(TOPLING_ZIP_TABLE_USE_ROCKSDB_FS_READ)
  IOOptions ioopt;
  IODebugContext dctx;
  Slice unused;
  char* scratch = (char*)bufdata;
  IOStatus s;
  if (auto stats = self->stats_) {
    auto t0 = qtime::now();
    s = self->storeFileObj_->Read(offset, len, ioopt, &unused, scratch, &dctx);
    auto t1 = qtime::now();
    stats->recordInHistogram(SST_READ_MICROS, t0.us(t1));
  }
  else {
    s = self->storeFileObj_->Read(offset, len, ioopt, &unused, scratch, &dctx);
  }
  if (unused.size_) {
    ROCKSDB_VERIFY_EQ(unused.size_, len);
    memcpy(scratch, unused.data_, len);
  }
  if (terark_unlikely(!s.ok())) {
    // to be catched by ToplingZipSubReader::Get()
    throw std::logic_error(s.ToString());
  }
 #else
  // `IOOptions::property_bag` is an std::unordered_map, although it is empty
  // in most cases, it incurs considerable overheads
  int fd = (int)self->storeFD_;
  if (auto stats = self->stats_) {
    auto t0 = qtime::now();
    auto rd = pread(fd, bufdata, len, offset);
    auto t1 = qtime::now();
    stats->recordInHistogram(SST_READ_MICROS, t0.us(t1));
    PREAD_CHECK_RESULT(rd);
  }
  else {
    auto rd = pread(fd, bufdata, len, offset);
    PREAD_CHECK_RESULT(rd);
  }
 #endif
  return bufdata;
}

static const byte_t*
FiberAsyncRead(void* vself, size_t offset, size_t len, valvec<byte_t>* buf) {
  buf->TERARK_IF_DEBUG(resize_fill(len, 0xCC), resize_no_init(len));
  auto bufdata = buf->data();
  if (UNLIKELY(0 == len)) {
    return bufdata;
  }
  auto self = (ToplingZipSubReader*)vself;
  int fd = (int)self->storeFD_;
  if (auto stats = self->stats_) {
    auto t0 = qtime::now();
    auto rd = fiber_aio_read(fd, bufdata, len, offset);
    auto t1 = qtime::now();
    stats->recordInHistogram(SST_READ_MICROS, t0.us(t1));
    PREAD_CHECK_RESULT(rd);
  }
  else {
    auto rd = fiber_aio_read(fd, bufdata, len, offset);
    PREAD_CHECK_RESULT(rd);
  }
  return bufdata;
}

void ToplingZipSubReader::GetRecordAppend(size_t recId, valvec<byte_t>* tbuf, bool async)
const {
  auto stats = stats_;
  qtime t0;
  if (stats) {
    t0 = qtime::now();
  }
  if (async) {
    // ignore storeUsePread_ on async.
    // we may use MADV_POPULATE_READ for mmap in the future.
    //   - MADV_POPULATE_READ by io uring does not seperate io submit
    //     and io wait, so call MADV_POPULATE_READ by io uring does not
    //     improve performance if not using io uring kernel polling
    //   - pread/preadv by io uring does seperate io submit and io wait,
    //     thus io can exec in parallel
    //   - if MADV_POPULATE_READ by io uring does seperate io submit and
    //     io wait in the future, we should prefer MADV_POPULATE_READ
    store_->fspread_record_append(&FiberAsyncRead, (void*)this,
                                  storeOffset_, recId, tbuf);
  } else if (storeUsePread_) {
    store_->fspread_record_append(&FsPread, (void*)this, storeOffset_, recId, tbuf);
  } else {
    store_->get_record_append(recId, tbuf);
  }
  if (stats) {
    auto t1 = qtime::now();
    auto ns = t0.ns(t1);
    perf_context.block_read_time += ns; // use this metric as zbs read nanos
    stats->recordInHistogram(READ_ZBS_RECORD_MICROS, ns / 1000);
  }
}

void ToplingZipSubReader::IterGetRecordAppend(size_t recId, BlobStore::CacheOffsets* co)
const {
  store_->get_record_append(recId, co);
}

Status ToplingZipSubReader::Get(const ReadOptions& read_options,
                               SequenceNumber global_seqno, const Slice& ikey,
                               GetContext* get_context)
const {
  switch (tag_rs_kind_) {
  case LegacyZvType: return GetZV(read_options, global_seqno, ikey, get_context);
  case RS_Key0_TagN: return GetRS(read_options, global_seqno, ikey, get_context, krs0_.krs_, krs0_.trsn_);
  case RS_Key0_Tag0: return GetRS(read_options, global_seqno, ikey, get_context, krs0_.krs_, krs0_.trs0_);
  case RS_Key0_Tag1: return GetRS(read_options, global_seqno, ikey, get_context, krs0_.krs_, krs0_.trs1_);
  case RS_KeyN_TagN: return GetRS(read_options, global_seqno, ikey, get_context, krsx_     , krsx_.trs());
  case RS_KeyN_Tag1: return GetRS(read_options, global_seqno, ikey, get_context, krsn_.krs_, krsn_.trs1_);
  }
  // should not goes here
  ROCKSDB_DIE("Bad tag_rs_kind_ = %d", tag_rs_kind_);
}

struct DeferFree : public Cleanable {
  explicit DeferFree(valvec<byte_t>&& buf) {
    if (buf.capacity()) {
      // trick: extra param arg2 passed to free will be ignored
      auto clean = (Cleanable::CleanupFunction)(free);
      this->RegisterCleanup(clean, buf.data(), nullptr);
      buf.risk_set_capacity(0); // instruct valvec::~valvec() do not free mem
    }
  }
};

template<class KeyRS, class TagRS>
Status ToplingZipSubReader::GetRS(const ReadOptions& read_options,
                                  SequenceNumber global_seqno, const Slice& ikey,
                                  GetContext* get_context,
                                  const KeyRS& krs, const TagRS& trs)
const {
  ROCKSDB_ASSERT_GE(ikey.size(), kNumInternalBytes);
  ParsedInternalKey pikey(ikey);
  size_t keyId = index_->Find(pikey.user_key);
  if (size_t(-1) == keyId) {
    return Status::OK();
  }
  const uint64_t seqno = pikey.sequence;
  size_t rsBitpos = krs.select0(keyId);
  size_t valueNum = krs.one_seq_len(rsBitpos + 1) + 1;
  for (size_t i = 0; i < valueNum; ++i) {
    size_t valueId = rsBitpos + i;
    if (trs.is1(valueId)) {
      size_t tagId = trs.rank1(valueId);
      tags_array_.GetSequenceAndType(tagId, &pikey.sequence, &pikey.type);
    } else {
      pikey.sequence = global_seqno;
      pikey.type = kTypeValue;
    }
    if (pikey.sequence <= seqno) {
      valvec<byte_t> buf;
      if (read_options.just_check_key_exists) {
        if (kTypeMerge == pikey.type)
          pikey.type = kTypeValue; // instruct SaveValue to stop earlier
      }
      else if (kTypeDeletion == pikey.type || kTypeSingleDeletion == pikey.type) {
        // do nothing
      }
      else { // fetch value
        if (read_options.async_io || storeUsePread_ || !zeroCopy_) {
          buf.reserve(estimateUnzipCap_);
        }
        GetRecordAppend(valueId, &buf, read_options.async_io);
        TryWarmupZeroCopy(buf, read_options.min_prefault_pages);
      }
      Slice val((const char*)buf.data(), buf.size());
      if (read_options.pinning_tls || buf.capacity()) {
        // pinning_tls indicate SST is pinned by pinned SuperVersion, thus it
        // is safe to expose mmap memory to user code
        if (!get_context->SaveValue(pikey, val, DeferFree(std::move(buf)))) {
          break;
        }
      } else {
        bool ignore = false;
        if (!get_context->SaveValue(pikey, val, &ignore)) // do not pin
          break;
      }
    }
  }
  return Status::OK();
}

Status ToplingZipSubReader::GetZV(const ReadOptions& read_options,
                                  SequenceNumber global_seqno, const Slice& ikey,
                                  GetContext* get_context)
const {
  ROCKSDB_ASSERT_GE(ikey.size(), kNumInternalBytes);
  ParsedInternalKey pikey(ikey);
  //auto t0 = qtime::now(); // qtime is fast enough, always timming
  size_t recId = index_->Find(pikey.user_key);
  //auto t1 = qtime::now();
  //perf_context.read_index_block_nanos += t0.ns(t1);
  if (size_t(-1) == recId) {
    return Status::OK();
  }
  const uint64_t seqno = pikey.sequence;
  valvec<byte_t> buf;
  if (read_options.async_io || storeUsePread_ || !zeroCopy_) {
    buf.reserve(128 + estimateUnzipCap_);
  }
  Cleanable pinnerObj; // ok local object
  Cleanable* pinner = nullptr;
  auto PinBuf = [&]() {
    if (buf.capacity()) {
      // trick: extra param arg2 passed to free will be ignored
      auto clean = (Cleanable::CleanupFunction)(free);
      pinnerObj.RegisterCleanup(clean, buf.data(), nullptr);
      pinner = &pinnerObj;
      buf.risk_set_capacity(0); // instruct valvec::~valvec() do not free mem
    } else if (read_options.pinning_tls) {
      pinner = &pinnerObj;
    }
  };
  auto zvType = type_.size()
    ? ZipValueType(type_[recId])
    : ZipValueType::kZeroSeq;
  switch (zvType) {
  default:
    return Status::Aborted("ToplingZipSubReader::GetZV", "Bad ZipValueType");
  case ZipValueType::kZeroSeq:
    if (global_seqno <= seqno) {
      pikey.sequence = global_seqno;
      pikey.type = kTypeValue;
      if (read_options.just_check_key_exists) {
        get_context->SaveValue(pikey, Slice(), Cleanable());
      } else {
        GetRecordAppend(recId, &buf, read_options.async_io);
        TryWarmupZeroCopy(buf, read_options.min_prefault_pages);
        auto val = SliceOf(buf);
        PinBuf();
        get_context->SaveValue(pikey, val, pinner);
      }
    }
    break;
  case ZipValueType::kValue: { // should be a kTypeValue, the normal case
    GetRecordAppend(recId, &buf, read_options.async_io);
    TryWarmupZeroCopy(buf, read_options.min_prefault_pages);
    pikey.sequence = *(uint64_t*)buf.data() & kMaxSequenceNumber;
    pikey.type = kTypeValue;
    if (pikey.sequence <= seqno) {
      if (read_options.just_check_key_exists) {
        get_context->SaveValue(pikey, Slice(), Cleanable());
      }
      else {
        auto val = SubSlice(buf, 7);
        PinBuf();
        get_context->SaveValue(pikey, val, pinner);
      }
    }
    break; }
  case ZipValueType::kDelete: {
    GetRecordAppend(recId, &buf, read_options.async_io);
    pikey.sequence = *(uint64_t*)buf.data() & kMaxSequenceNumber;
    pikey.type = kTypeDeletion;
    if (pikey.sequence <= seqno) {
      auto val = SubSlice(buf, 7);
      PinBuf();
      get_context->SaveValue(pikey, val, pinner);
    }
    break; }
  case ZipValueType::kMulti: { // more than one value
    if (buf.capacity()) {
      buf.resize_no_init(sizeof(uint32_t));
      GetRecordAppend(recId, &buf, read_options.async_io);
    } else {
      GetRecordAppend(recId, &buf, read_options.async_io);
      TryWarmupZeroCopy(buf, read_options.min_prefault_pages);
      TERARK_VERIFY(this->zeroCopy_);
      byte_t* buf_copy = (byte_t*)malloc(sizeof(uint32_t) + buf.size());
      memcpy(buf_copy + sizeof(uint32_t), buf.data(), buf.size());
      buf.risk_set_data(buf_copy, sizeof(uint32_t) + buf.size());
    }
    TERARK_ASSERT_GE(buf.size(), sizeof(uint32_t) + sizeof(SequenceNumber));
    uint32_t num = 0;
    auto mVal = ZipValueMultiValue::decode(buf, &num);
    TERARK_ASSERT_GE(num, 1);
    PinBuf();
    for (size_t i = 0; i < num; ++i) {
      Slice val = mVal->getValueData(i, num);
      TERARK_ASSERT_GE(val.size(), sizeof(SequenceNumber));
      auto tag = unaligned_load<SequenceNumber>(val.data());
      UnPackSequenceAndType(tag, &pikey.sequence, &pikey.type);
      if (pikey.sequence <= seqno) {
        if (read_options.just_check_key_exists) {
          if (kTypeMerge == pikey.type) {
            pikey.type = kTypeValue; // instruct SaveValue to stop earlier
          }
          val = Slice(); // empty
        }
        else {
          val.remove_prefix(sizeof(SequenceNumber));
        }
        if (!get_context->SaveValue(pikey, val, pinner)) {
          break;
        }
      }
    }
    break; }
  }
  return Status::OK();
}

bool ToplingZipSubReader::PointGet(const Slice& ikey, valvec<byte_t>* val) const {
  switch (tag_rs_kind_) {
  case LegacyZvType: return PointGetZV(ikey, val);
  case RS_Key0_TagN: return PointGetRS(ikey, val, krs0_.krs_, krs0_.trsn_);
  case RS_Key0_Tag0: return PointGetRS(ikey, val, krs0_.krs_, krs0_.trs0_);
  case RS_Key0_Tag1: return PointGetRS(ikey, val, krs0_.krs_, krs0_.trs1_);
  case RS_KeyN_TagN: return PointGetRS(ikey, val, krsx_     , krsx_.trs());
  case RS_KeyN_Tag1: return PointGetRS(ikey, val, krsn_.krs_, krsn_.trs1_);
  }
  // should not goes here
  ROCKSDB_DIE("Bad tag_rs_kind_ = %d", tag_rs_kind_);
}
template<class KeyRS, class TagRS>
bool ToplingZipSubReader::PointGetRS(const Slice& ikey, valvec<byte_t>* val,
                                     const KeyRS& krs, const TagRS& trs)
const {
  ROCKSDB_ASSERT_GE(ikey.size(), kNumInternalBytes);
  Slice user_key = ExtractUserKey(ikey);
  size_t keyId = index_->Find(user_key);
  if (size_t(-1) == keyId) {
    return false;
  }
  size_t rsBitpos = krs.select0(keyId);
  size_t valueNum = krs.one_seq_len(rsBitpos + 1) + 1;
  for (size_t i = 0; i < valueNum; ++i) {
    size_t valueId = rsBitpos + i;
    bool found = true;
    if (trs.is1(valueId)) {
      size_t tagId = trs.rank1(valueId);
      uint64_t tag = tags_array_[tagId];
      found = GetUnaligned<uint64_t>(user_key.end()) == tag;
    }
    if (found) {
      if (val) store_->get_record(valueId, val);
      return true;
    }
  }
  return false;
}
bool ToplingZipSubReader::PointGetZV(const Slice& ikey, valvec<byte_t>* val) const {
  ROCKSDB_ASSERT_GE(ikey.size(), kNumInternalBytes);
  Slice user_key = ExtractUserKey(ikey);
  size_t recId = index_->Find(user_key);
  if (size_t(-1) == recId) {
    return false;
  }
  if (val) {
    // be simply, just fetch packed tag and value, do not decode multi....
    store_->get_record(recId, val);
  }
  return true;
}

size_t ToplingZipSubReader::ApproximateRank(fstring key) const {
  if (krce_.cache_bytes) {
    size_t min_len = std::min<size_t>(key.size(), krce_.cache_prefix_len);
    size_t common_pref_len = commonPrefixLen(krce_.cache_prefix(), key);
    if (common_pref_len < min_len) {
      if (key.uch(common_pref_len) < krce_.cache_prefix_data[common_pref_len])
        return 0;
      else
        return index_->NumKeys();
    }
    else if (key.size() <= krce_.cache_prefix_len) {
      ROCKSDB_ASSERT_EQ(key.size(), krce_.cache_prefix_len);
      return 0;
    }
    else {
      size_t lo;
      if (krce_.fixed_suffix_len) {
        // lower_bound_prefix is much faster than lower_bound, if there is no
        // dup keys(this is the case here), lower_bound_prefix is lower_bound
        // or (lower_bound - 1), we prefer speed to accurate, ignore the small
        // difference.
        lo = fixCacheKeys_.lower_bound_prefix(key.substr(krce_.cache_prefix_len));
      } else {
        lo = varCacheKeys_.lower_bound(key.substr(krce_.cache_prefix_len));
      }
      size_t rank = keyRankCacheStep_ * lo;
      return std::min(rank, index_->NumKeys());
    }
  }
  return index_->ApproximateRank(key);
}

ToplingZipSubReader::~ToplingZipSubReader() {
  type_.risk_release_ownership();
}

AbstractBlobStore::Dictionary getVerifyDict(Slice dictData) {
  if (isChecksumVerifyEnabled()) {
    return AbstractBlobStore::Dictionary(dictData);
  } else {
    return AbstractBlobStore::Dictionary(dictData, 0); // NOLINT
  }
}

TooZipTableReaderBase::TooZipTableReaderBase(const ToplingZipTableFactory* fac)
 : table_factory_(fac) {
  dict_zip_size_ = 0;
  this->debugLevel_ = fac->table_options_.debugLevel;
  enableApproximateKeyAnchors_ = fac->table_options_.enableApproximateKeyAnchors;
}

TooZipTableReaderBase::~TooZipTableReaderBase() {
  if (dict_zip_size_ && hugepage_mem_.capacity() == 0) {
    free((char*)dict_.data_);
  }
}

void TooZipTableReaderBase::ReadGlobalDict(const TableReaderOptions& tro) {
  BlockContents valueDictBlock;
  try {
    const ImmutableOptions& ioptions = tro.ioptions;
    valueDictBlock = ReadMetaBlockE(file_.get(), file_data_.size_,
        kToplingZipTableMagicNumber, ioptions, kToplingZipTableValueDictBlock);
    TERARK_VERIFY(!valueDictBlock.data.empty());
  }
  catch (const Status&) {
    // when not found, Status is Corruption, it is confused with
    // real error, we ignore it
  }
  BlockContents dictInfoBlock; // dict compression name, such as zstd
  try {
    const ImmutableOptions& ioptions = tro.ioptions;
    dictInfoBlock = ReadMetaBlockE(file_.get(), file_data_.size_,
        kToplingZipTableMagicNumber, ioptions, kToplingZipTableDictInfo);
    TERARK_VERIFY(!dictInfoBlock.data.empty());
  }
  catch (const Status&) {
    // when not found, Status is Corruption, it is confused with
    // real error, we ignore it
  }
  valvec<byte_t> dict;
  Status s = DecompressDict(dictInfoBlock.data, valueDictBlock.data, &dict);
  if (!s.ok()) {
    throw s; // NOLINT
  }
  if (dict.empty()) { // dict is not compressed
    dict_zip_size_ = 0;
    dict_ = valueDictBlock.data;
  }
  else {
    dict_zip_size_ = valueDictBlock.data.size_;
    dict_ = Slice((char*)dict.data(), dict.size());
    dict.risk_release_ownership();
  }
  if (!dict_.empty()) { // PlainBlobStore & MixedLenBlobStore has no dict
    table_properties_->user_collected_properties
      .emplace(kToplingZipTableDictSize, lcast(dict_.size_));
  }
  table_properties_->gdic_size = dict_.size_;
}

void TooZipTableReaderBase::SetupForCompaction() {
  MmapAdvSeq(file_data_.data_, file_data_.size_);
}

Status TooZipTableReaderBase::DumpTable(WritableFile* out_file) {
  std::string meta = ToWebViewString({{"indent",4}, {"html",0}});
  out_file->Append(meta);
  out_file->Append("\n\n");
  return TableReader::DumpTable(out_file);
}

template<class T>
static T* unchecked_append(valvec<T>& vec, const T* src, size_t len) {
  TERARK_VERIFY_LE(len, vec.unused());
  vec.unchecked_append(src, len);
  return vec.end() - len;
}

static void StoreMetaUseHugePage(BlobStore* store, valvec<byte_t>* huge) {
  auto meta_blocks = store->get_meta_blocks();
  for (auto& block : meta_blocks) {
    TERARK_VERIFY_LE(block.data.size(), huge->unused());
    auto mem = unchecked_append(*huge, block.data.udata(), block.data.size());
    while (huge->size() % 64 != 0) {
      huge->push_back(0);
    }
    MmapColdize(block.data);
    block.data.p = (const char*)mem;
  }
  store->detach_meta_blocks(meta_blocks);
}

void TooZipTableReaderBase::LoadStore(ToplingZipSubReader* parts,
            TableMultiPartInfo& offsetInfo,
            const AbstractBlobStore::Dictionary& dict) {
  size_t partCount = offsetInfo.prefixSet_.size();
  auto baseAddress = (const byte_t*)file_data_.data();
  for (size_t i = 0; i < partCount; i++) {
    auto& curr = offsetInfo.offset_[i];
    TERARK_VERIFY_AL(curr.offset, 64);
    TERARK_VERIFY_AL(curr.key   , 64);
    TERARK_VERIFY_AL(curr.value , 64);
    TERARK_VERIFY_AL(curr.type  , 64);
    const fstring storeMem(baseAddress + curr.offset + curr.key, curr.value);
    parts[i].store_.reset(AbstractBlobStore::load_from_user_memory(storeMem, dict));
  }
}

size_t TooZipTableReaderBase::InitPart(
          size_t nth, size_t offset, ToplingZipSubReader& part,
          TableMultiPartInfo& offsetInfo, const TableReaderOptions& tro,
          const AbstractBlobStore::Dictionary& dict) {
  auto& curr = offsetInfo.offset_[nth];
  auto tf = table_factory_;
  auto fileObj = dynamic_cast<MmapReadWrapper*>(file_->target())->target_;
  auto baseAddress = (const byte_t*)file_data_.data();
  const size_t prefixLen = offsetInfo.prefixSet_.m_fixlen;
  const size_t type_offset = offset + curr.key + curr.value;
  TERARK_VERIFY_EQ(curr.offset, offset);
  TERARK_VERIFY_AL(curr.offset, 64);
  TERARK_VERIFY_AL(curr.key   , 64);
  TERARK_VERIFY_AL(curr.value , 64);
  TERARK_VERIFY_AL(curr.type  , 64);
  if (tf->table_options_.indexMemAsResident) {
    MlockMem(baseAddress + offset, curr.key);
  }
  else if (WarmupLevel::kIndex == tf->table_options_.warmupLevel) {
    MmapAdvSeq(baseAddress + offset, curr.key);
    MmapWarmUp(baseAddress + offset, curr.key);
    MmapAdvSeq(baseAddress + type_offset, curr.type);
    MmapWarmUp(baseAddress + type_offset, curr.type);
  }
  part.debugLevel_ = this->debugLevel_;
  part.storeFD_ = fileObj->FileDescriptor();
  part.storeFileObj_ = fileObj;
  part.fileReader_ = file_.get();
  if (tf->table_options_.enableStatistics) {
    part.stats_ = tro.ioptions.stats;
  } else {
    part.stats_ = nullptr;
  }
  part.rawReaderOffset_ = offset;
  part.rawReaderSize_ = curr.key + curr.value + curr.type;
  memcpy(part.prefix_data_, offsetInfo.prefixSet_.data() + nth * prefixLen, prefixLen);
  part.prefix_len_ = prefixLen;
  if (hugepage_mem_.capacity() == 0) {
    part.index_ = COIndex::LoadMemory(baseAddress + offset, curr.key);
  } else {
    auto mem = unchecked_append(hugepage_mem_, baseAddress + offset, curr.key);
    part.index_ = COIndex::LoadMemory(mem, curr.key);
    MmapColdize(baseAddress + offset, curr.key);
  }
  part.storeOffset_ = offset += curr.key;
  if (hugepage_mem_.capacity() != 0) {
    StoreMetaUseHugePage(part.store_.get(), &hugepage_mem_);
  }
  part.InitUsePread(tf->table_options_.minPreadLen);
  part.store_->set_min_prefetch_pages(tf->table_options_.minPrefetchPages);
  offset += curr.value;
  size_t keyNum = part.index_->NumKeys();
  size_t valueNum = part.store_->num_records();
  auto tag_rs_base = (byte_t*)baseAddress + type_offset;
  if (hugepage_mem_.capacity() == 0) {
    part.InitKeyTags(tag_rs_base, keyNum, valueNum, curr);
  } else {
    auto mem = unchecked_append(hugepage_mem_, tag_rs_base, curr.type);
    part.InitKeyTags(mem, keyNum, valueNum, curr);
    MmapColdize(tag_rs_base, curr.type);
  }
  offset += curr.type;
  if (!part.IsWireSeqNumAllZero()) {
    global_seqno_ = 0;
  }
  return offset;
}

void
ToplingZipTableReader::Open(RandomAccessFileReader* file, Slice file_data,
                            const TableReaderOptions& tro,
                            TableMultiPartInfo& info) {
  TERARK_VERIFY(info.prefixSet_.size() == 1);
  TERARK_SCOPE_EXIT(info.risk_release_ownership());
  LoadCommonPart(file, tro, file_data, kToplingZipTableMagicNumber);
  const auto& ioptions = tro.ioptions;
  TableProperties* props = table_properties_.get();
  uint64_t file_size = file_data.size_;
  ReadGlobalDict(tro);
  LoadStore(&subReader_, info, getVerifyDict(dict_));
  ConfigureHugePage(&subReader_, info, tro);
  size_t offset = InitPart(0, 0, subReader_, info, tro, getVerifyDict(dict_));
  if (info.krceVec_) {
    subReader_.InitKeyRankCache(file_data.data_ + offset, *info.krceVec_);
  }
  SetCompressionOptions(&subReader_, 1);
  long long t0 = g_pf.now();
  SetupForRandomRead();
  long long t1 = g_pf.now();
  subReader_.index_->BuildCache(table_factory_->table_options_.indexCacheRatio);
  if (debugLevel_ >= 3) {
    // In ToplingZipTableMultiReader, this is included in build cache time
    subReader_.index_->VerifyDictRank();
  }
  long long t2 = g_pf.now();
  ROCKS_LOG_DEBUG(ioptions.info_log
    , "ToplingZipTableReader::Open(%s): fsize = %zd, entries = %zd keys = %zd indexSize = %zd valueSize=%zd, warm up time = %6.3f'sec, build cache time = %6.3f'sec, pref_len = %d\n"
    , file->file_name().c_str()
    , size_t(file_size), size_t(props->num_entries)
    , subReader_.index_->NumKeys()
    , size_t(props->index_size)
    , size_t(props->data_size)
    , g_pf.sf(t0, t1)
    , g_pf.sf(t1, t2)
    , info.prefixSet_.m_fixlen
  );
}

void ToplingZipTableReader::SetupForRandomRead() {
  const size_t indexSize = subReader_.index_->Memory().size();
  const auto& tzto = table_factory_->table_options_;
  if (advise_random_on_open_ && tzto.warmupLevel < WarmupLevel::kValue) {
    if (subReader_.StoreDataSize() > file_data_.size() / 2) {
      for (auto& block : subReader_.store_->get_data_blocks())
        MmapAdvRnd(block.data);
    }
  }
  if (hugepage_mem_.capacity() != 0) {
    return;
  }
  if (tzto.indexMemAsResident) {
    MlockMem(file_data_.data_, indexSize);
    for (auto& block : subReader_.store_->get_meta_blocks()) {
      MlockMem(block.data);
    }
  }
  else if (WarmupLevel::kIndex == tzto.warmupLevel) {
    MmapAdvSeq(file_data_.data_, indexSize);
    MmapWarmUp(file_data_.data_, indexSize);
    for (auto& block : subReader_.store_->get_meta_blocks()) {
      MmapAdvSeq(block.data);
      MmapWarmUp(block.data);
    }
  }
}

template<class Reader>
static InternalIterator*
NewIter(Reader* r, const ReadOptions& ro, Arena* arena,
        bool ZipOffset, bool IsLegacyZV, bool IsOneValuePerUK) {
  const bool reverse = !!r->isReverseBytewiseOrder_;
#define ForTemplateArg(a, b) \
  do { \
    if (a == reverse && b == ZipOffset) { \
      if (IsLegacyZV) \
        if (IsOneValuePerUK) \
          return r->template NewIteratorImp<a,b,OneValuePerUK<IterOptLegacyZV> >(ro, arena); \
        else \
          return r->template NewIteratorImp<a,b,MultiValuePerUK<IterOptLegacyZV> >(ro, arena); \
      else \
        if (IsOneValuePerUK) \
          return r->template NewIteratorImp<a,b,OneValuePerUK<IterOptTagRS> >(ro, arena); \
        else \
          return r->template NewIteratorImp<a,b,MultiValuePerUK<IterOptTagRS> >(ro, arena); \
    } \
  } while (0)
  ForTemplateArg(0,0);
  ForTemplateArg(0,1);
  ForTemplateArg(1,0);
  ForTemplateArg(1,1);
  TERARK_DIE("Unexpected");
}

InternalIterator*
ToplingZipTableReader::
NewIterator(const ReadOptions& ro, const SliceTransform* prefix_extractor,
            Arena* arena, bool skip_filters, TableReaderCaller caller,
            size_t compaction_readahead_size,
            bool allow_unprepared_value) {
  TERARK_UNUSED_VAR(skip_filters); // unused
  const bool ZipOffset = !!subReader_.store_->is_offsets_zipped();
  const bool IsLegacyZV = LegacyZvType == subReader_.tag_rs_kind_;
  const bool IsOneValuePerUK = subReader_.IsOneValuePerUK();
  return NewIter(this, ro, arena, ZipOffset, IsLegacyZV, IsOneValuePerUK);
}

template<bool reverse, bool ZipOffset, class IterOpt>
InternalIterator*
ToplingZipTableReader::NewIteratorImp(const ReadOptions& ro, Arena* a) {
  typedef IterZO<ToplingZipTableIterator<reverse, IterOpt>, ZipOffset> IterType;
  if (a) {
    return new(a->AllocateAligned(sizeof(IterType)))
               IterType(&subReader_, this, ro, global_seqno_);
  } else {
    return new IterType(&subReader_, this, ro, global_seqno_);
  }
}

Status
ToplingZipTableReader::Get(const ReadOptions& ro, const Slice& ikey,
                          GetContext* get_context, const SliceTransform* prefix_extractor,
                          bool skip_filters) {
  TERARK_ASSERT_GE(ikey.size(), 8);
  if (IF_TOP_ZIP_TABLE_KEEP_EXCEPTION(0, LIKELY(0 == debugLevel_))) {
    try {
      return subReader_.Get(ro, global_seqno_, ikey, get_context);
    } catch (const std::exception& ex) {
      return Status::Corruption("ToplingZipTableReader::Get()", ex.what());
    }
  }
  else { // let C++ runtime handle exception, will trap at throw in debugger
    return subReader_.Get(ro, global_seqno_, ikey, get_context);
  }
}

uint64_t ToplingZipTableReader::ApproximateOffsetOf(
      ROCKSDB_8_X_COMMA(const ReadOptions& readopt)
      const Slice& ikey, TableReaderCaller) {
  size_t numRecords = subReader_.index_->NumKeys();
  size_t rank = subReader_.ApproximateRank(ExtractUserKey(ikey));
  TERARK_VERIFY_LE(rank, numRecords);
  auto offset = uint64_t(file_data_.size_ * 1.0 * rank / numRecords);
  if (isReverseBytewiseOrder_) return file_data_.size_ - offset;
  return offset;
}

uint64_t
ToplingZipTableReader::ApproximateSize(ROCKSDB_8_X_COMMA(const ReadOptions& readopt)
                                      const Slice& start,
                                      const Slice& end,
                                      TableReaderCaller caller) {
  size_t rankBeg = subReader_.ApproximateRank(ExtractUserKey(start));
  size_t rankEnd = subReader_.ApproximateRank(ExtractUserKey(end));
  size_t numRecords = subReader_.index_->NumKeys();
  TERARK_VERIFY_LE(rankBeg, numRecords);
  TERARK_VERIFY_LE(rankEnd, numRecords);
  auto avgLen = double(file_data_.size_) / numRecords;
  if (rankBeg == rankEnd) {
    if (subReader_.krce_.num)
      return 1 + uint64_t(avgLen * 0.618 * subReader_.keyRankCacheStep_);
    else
      return 0;
  }
  auto offsetBeg = uint64_t(avgLen * rankBeg);
  auto offsetEnd = uint64_t(avgLen * rankEnd);
  if (isReverseBytewiseOrder_) {
    TERARK_VERIFY_GE(offsetBeg, offsetEnd);
    return offsetBeg - offsetEnd;
  } else {
    TERARK_VERIFY_GE(offsetEnd, offsetBeg);
    return offsetEnd - offsetBeg;
  }
}

Status ToplingZipTableReader::ApproximateKeyAnchors(const ReadOptions&, std::vector<Anchor>& anchors) {
  if (!enableApproximateKeyAnchors_) {
    return Status::NotSupported("ToplingZipTable: enableApproximateKeyAnchors is false");
  }
  auto index = subReader_.index_.get();
  size_t sum = index->NumKeys();
  size_t num = std::min(sum, kMaxNumAnchors);
  size_t size = file_data_.size_ / num;
  double step = double(sum) / num;
  for (size_t i = 0; i < num; i++) {
    size_t nth = std::min(size_t(step * (i + 1)), sum) - 1;
    anchors.emplace_back(index->NthKey(nth), size);
  }
  return Status::OK();
}

Status ToplingZipTableMultiReader::ApproximateKeyAnchors(const ReadOptions&, std::vector<Anchor>& anchors) {
  if (!enableApproximateKeyAnchors_) {
    return Status::NotSupported("ToplingZipTable: enableApproximateKeyAnchors is false");
  }
  size_t sum = 0, parts = subReader_.size();
  size_t* keynums = (size_t*)alloca(sizeof(size_t) * (parts + 1));
  for (size_t i = 0; i < parts; i++) {
    keynums[i] = sum;
    sum += subReader_[i].index_->NumKeys();
  }
  keynums[parts] = sum;
  size_t num = std::min(sum, kMaxNumAnchors);
  size_t size = file_data_.size_ / num;
  double step = double(sum) / num;
  for (size_t i = 0; i < num; i++) {
    size_t nth = std::min(size_t(step * (i + 1)), sum) - 1;
    auto ipart = upper_bound_0(keynums, parts, nth) - 1;
    auto index = subReader_[ipart].index_.get();
    auto keyth = nth - keynums[ipart];
    TERARK_ASSERT_LT(keyth, index->NumKeys());
    anchors.emplace_back(index->NthKey(keyth), size);
  }
  return Status::OK();
}

ToplingZipTableReader::~ToplingZipTableReader() {
  TERARK_VERIFY_F(0 == live_iter_num_, "real: %zd", live_iter_num_);
}

ToplingZipTableReader::ToplingZipTableReader(const ToplingZipTableFactory* table_factory)
  : TooZipTableReaderBase(table_factory)
{
}

#define ToStr(...) json(std::string(buf, snprintf(buf, sizeof(buf), __VA_ARGS__)))

template<class KeyRS, class TagRS>
static json JTagRS(const char* krs_name, const KeyRS& krs,
                   const char* trs_name, const TagRS& trs, bool html) {
  json js = {
{{"Kind",krs_name},{"Bits",krs.size()},{"Num0",krs.max_rank0()},{"Num1",krs.max_rank1()},{"MemSize",krs.mem_size()}},
{{"Kind",trs_name},{"Bits",trs.size()},{"Num0",trs.max_rank0()},{"Num1",trs.max_rank1()},{"MemSize",trs.mem_size()}},
  };
  if (html) {
    js[0]["<htmltab:col>"] = json::array({
      "Kind", "Bits", "Num0", "Num1", "MemSize"
    });
  }
  return js;
}

static void JsonWebViewSubReader(const char* base, const ToplingZipSubReader& sub,
          json& djs, bool html, size_t hugepageMem) {
  djs["Index"];  // insert
  djs["Store"];  // insert
  djs["TagRsKind"]; // insert
  auto& idx = djs["Index"];
  std::string Type = sub.index_->Name();
  size_t NumKeys = sub.index_->NumKeys();
  size_t TotalRawKeySize = sub.index_->TotalKeySize();
  size_t TotalZipKeySize = sub.index_->Memory().size();
  size_t PartSize = sub.rawReaderSize_;
  char buf[64];
  auto& ibase = idx["Common"];
  ibase["Type"] = sub.index_->Name();
  ROCKSDB_JSON_SET_SIZE(ibase, NumKeys);
  ROCKSDB_JSON_SET_SIZE(ibase, TotalRawKeySize);
  ROCKSDB_JSON_SET_SIZE(ibase, TotalZipKeySize);
  ibase["AvgZipKey"] = ToStr("%.3f", double(TotalZipKeySize) / NumKeys);
  ibase["AvgRawKey"] = ToStr("%.3f", double(TotalRawKeySize) / NumKeys);
  idx["Specific"] = sub.index_->MetaToJson();
  idx["SizeRatio"] = ToStr("%.3f", double(TotalZipKeySize) / PartSize);
  json& sto = djs["Store"];
  sto["Type"] = sub.store_->name();
  if (auto dz = dynamic_cast<DictZipBlobStore*>(sub.store_.get())) {
    sto["EntropyAlgo"] = ::enum_stdstr(dz->entropyAlgo());
  }
  sto["AvgZipVal"] = ToStr("%.3f", double(sub.store_->mem_size()) / NumKeys);
  sto["AvgRawVal"] = ToStr("%.3f", double(sub.store_->total_data_size()) / NumKeys);
  sto["SizeRatio"] = ToStr("%.3f", double(sub.store_->mem_size()) / PartSize);
  size_t SumZipBlocks = 0;
  for (auto& blk : sub.store_->get_data_blocks()) {
    sto["Data"].push_back(json::object({
		    { "name", blk.name.p },
        { "offset", blk.data.p - base },
        { "size", blk.data.size() },
        { "avg", double(blk.data.size()) / NumKeys },
        { "ratio", double(blk.data.size()) / sub.store_->mem_size() },
    }));
    SumZipBlocks += blk.data.size();
  }
  size_t MetaSum = 0;
  for (auto& blk : sub.store_->get_meta_blocks()) {
    sto["Meta"].push_back(json::object({
		    { "name", blk.name.p },
        { "offset", hugepageMem ? -1 : blk.data.p - base },
        { "size", blk.data.size() },
        { "avg", double(blk.data.size()) / NumKeys },
        { "ratio", double(blk.data.size()) / sub.store_->mem_size() },
    }));
    MetaSum += blk.data.size();
  }
  sto["MetaSum"] = MetaSum;
  sto["MetaAvg"] = double(MetaSum) / NumKeys;
  if (html) {
   if (SumZipBlocks)
    sto["Data"][0]["<htmltab:col>"] = json::array({
      "name", "offset", "size", "avg", "ratio"
    });
   if (MetaSum)
    sto["Meta"][0]["<htmltab:col>"] = json::array({
      "name", "offset", "size", "avg", "ratio"
    });
  }
json& rjs = djs["TagRsKind"];
switch (sub.tag_rs_kind_) {
case LegacyZvType: {
  json& zvt_js = rjs["ZvType"];
  zvt_js["ItemCount"] = sub.type_.size();
  zvt_js["MemSize"] = sub.type_.mem_size();
  zvt_js["SizeRatio"] = double(sub.type_.mem_size()) / PartSize;
  json& zvt_hist_js = zvt_js["Historgram"];
  for (int ev = 0; ev < 4; ++ev) {
    Slice name = enum_name(ZipValueType(ev));
    zvt_hist_js.push_back(json{
      { "enum", ev },
      { "name", name.ToString() },
      { "count", sub.type_histogram_[ev]},
    });
  }
  zvt_hist_js[0]["<htmltab:col>"] = json::array({
    "enum", "name", "count",
  });
  break; }
case RS_Key0_TagN: rjs = JTagRS("Key0", sub.krs0_.krs_, "TagN", sub.krs0_.trsn_, html); break;
case RS_Key0_Tag0: rjs = JTagRS("Key0", sub.krs0_.krs_, "Tag0", sub.krs0_.trs0_, html); break;
case RS_Key0_Tag1: rjs = JTagRS("Key0", sub.krs0_.krs_, "Tag1", sub.krs0_.trs1_, html); break;
case RS_KeyN_TagN: rjs = JTagRS("KeyX", sub.krsx_     , "TagX", sub.krsx_.trs(), html); break;
case RS_KeyN_Tag1: rjs = JTagRS("KeyN", sub.krsn_.krs_, "Tag1", sub.krsn_.trs1_, html); break;
}
if (LegacyZvType != sub.tag_rs_kind_ && RS_Key0_Tag0 != sub.tag_rs_kind_) {
  rjs = {{"RankSelect", std::move(rjs)}};
  const char* vtr_width = "VTR Bits";
  if (html) {
    vtr_width = "<span title='Zipped ValueType Bits'>VTR Bits</span>";
  }
  std::string vtlist;
  for (size_t vtr = 0; vtr < sub.tags_array_.vtr_num(); vtr++) {
    Slice name = enum_name(sub.tags_array_.vtr_to_vt(vtr));
    vtlist.append(name.data(), name.size());
    vtlist.push_back(',');
  }
  vtlist.pop_back(); // pop last ','

  rjs["TagArray"] = {
    {"Seq Min" , sub.tags_array_.min_seq()},
    {"Seq Bits", sub.tags_array_.seq_width()},
    {vtr_width , sub.tags_array_.vtr_width()},
    {"VTR List", std::move(vtlist)},
    {"ArrayLen", sub.tags_array_.size()},
    {"MemSize" , SizeToString(sub.tags_array_.mem_size())},
    {"ZipRatio", sub.tags_array_.uintbits() / 64.0},
  };
}
  if (sub.krce_.cache_bytes) {
    djs["RankCache"] = {
      { "Bytes", sub.krce_.cache_bytes },
      { "Num"  , sub.krce_.num },
      { "CachePrefixLen", sub.krce_.cache_prefix_len },
      { "FixedSuffixLen", sub.krce_.fixed_suffix_len },
    };
    if (0 == sub.krce_.fixed_suffix_len) {
      djs["RankCache"]["AvgKeyLen"] = sub.varCacheKeys_.avg_size();
      djs["RankCache"]["SumKeyLen"] = sub.varCacheKeys_.str_size();
    }
  } else {
    djs["RankCache"] = "EMPTY";
  }
}

std::string ToplingZipTableReader::ToWebViewString(const json& dump_options) const {
  bool html = JsonSmartBool(dump_options, "html", true);
  json djs;
  djs["Props.User"] = TableUserPropsToJson(
    table_properties_->user_collected_properties, dump_options);

  djs["PrefixLen"] = size_t(subReader_.prefix_len_);
  djs["fixed_key_len"] = (long long)(table_properties_->fixed_key_len);
  djs["fixed_value_len"] = (long long)(table_properties_->fixed_value_len);
  size_t DictSize = subReader_.store_->get_dict().memory.size();
  if (DictSize) {
    auto& dict_js = djs["Dict"];
    size_t RawSize = DictSize;
    size_t ZipSize = dict_zip_size_;
    ROCKSDB_JSON_SET_SIZE(dict_js, RawSize);
    ROCKSDB_JSON_SET_SIZE(dict_js, ZipSize);
    dict_js["Zip/Raw"] = double(ZipSize) / RawSize;
    dict_js["RawDict/RawData"] = double(DictSize) / subReader_.store_->total_data_size();
    dict_js["RawDict/ZipData"] = double(DictSize) / subReader_.store_->mem_size();
  }
  size_t HugePageMem = hugepage_mem_.size();
  ROCKSDB_JSON_SET_SIZE(djs, HugePageMem);
  JsonWebViewSubReader(file_data_.data_, subReader_, djs, html, HugePageMem);
  return JsonToString(djs, dump_options);
}

void ToplingZipTableMultiReader::Init(TableMultiPartInfo& offsetInfo, const TableReaderOptions& tro) {
  const byte_t* baseAddress = (const byte_t*)file_data_.data();
  AbstractBlobStore::Dictionary dict = getVerifyDict(dict_);
  const size_t partCount = offsetInfo.prefixSet_.size();
  subReader_.resize(partCount);

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
  // bytewise copy prefixSet_
  memcpy(&prefixSet_, &offsetInfo.prefixSet_, sizeof(prefixSet_));
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif

  prefixSet_.optimize_func();

  size_t offset = 0;
  LoadStore(subReader_.data(), offsetInfo, dict);
  ConfigureHugePage(subReader_.data(), offsetInfo, tro);
  for (size_t i = 0; i < partCount; ++i) {
    auto& part = subReader_[i];
    offset = InitPart(i, offset, part, offsetInfo, tro, dict);
  }
  hasAnyZipOffset_ = false;
  for (size_t i = 0; i < partCount; ++i) {
    auto& part = subReader_[i];
    if (part.store_->is_offsets_zipped())
      hasAnyZipOffset_ = true;
  }
  if (offsetInfo.krceVec_) {
    offset = pow2_align_up(offset, 64);
    for (size_t i = 0; i < partCount; ++i) {
      auto& part = subReader_[i];
      part.InitKeyRankCache(baseAddress + offset, offsetInfo.krceVec_[i]);
      offset += pow2_align_up(offsetInfo.krceVec_[i].cache_bytes, 64);
    }
  }
}

bool ToplingZipTableMultiReader::IterIsLegacyZV() const {
  for (auto& sub : subReader_) {
    if (LegacyZvType != sub.tag_rs_kind_)
      return false;
  }
  return true;
}

const ToplingZipSubReader*
ToplingZipTableMultiReader::LowerBoundSubReader(fstring key) const {
  if (isReverseBytewiseOrder_) {
  //TODO: the semantic of upper_bound_prefix is ill, clarify or delete it later
  //auto index = prefixSet_.upper_bound_prefix(key);
  //key.n = std::min<ptrdiff_t>(key.n, PrefixLen_);
    auto index = prefixSet_.upper_bound(key);
    if (index == 0) {
      return nullptr;
    }
    assert(prefixSet_[index - 1] <= key);
    return &subReader_[index - 1];
  }
  else {
  // set key.n is not needed because use lower_bound_prefix
  // key.n = std::min<ptrdiff_t>(key.n, PrefixLen_);
    size_t index = prefixSet_.lower_bound_prefix(key);
    if (index == prefixSet_.size()) {
      return nullptr;
    }
    assert(key.size() < prefixSet_.m_fixlen ||
           prefixSet_[index] >= key.prefix(prefixSet_.m_fixlen));
    return &subReader_[index];
  }
}

InternalIterator*
ToplingZipTableMultiReader::
NewIterator(const ReadOptions& ro, const SliceTransform* prefix_extractor,
            Arena* arena, bool skip_filters, TableReaderCaller caller,
            size_t compaction_readahead_size,
            bool allow_unprepared_value) {
  TERARK_UNUSED_VAR(skip_filters); // unused
  return NewIter(this, ro, arena, HasAnyZipOffset(), IterIsLegacyZV(), IsOneValuePerUK());
}

template<bool reverse, bool ZipOffset, class IterOpt>
InternalIterator*
ToplingZipTableMultiReader::NewIteratorImp(const ReadOptions& ro, Arena* a) {
  typedef IterZO<ToplingZipTableMultiIterator<reverse, IterOpt>, ZipOffset> IterType;
  if (a) {
    return new(a->AllocateAligned(sizeof(IterType)))
               IterType(*this, ro, global_seqno_);
  } else {
    return new IterType(*this, ro, global_seqno_);
  }
}

Status
ToplingZipTableMultiReader::Get(const ReadOptions& ro, const Slice& ikey, GetContext* get_context,
                               const SliceTransform* prefix_extractor, bool skip_filters) {
  size_t pref_len = prefixSet_.m_fixlen;
  if (UNLIKELY(ikey.size() < 8 + pref_len)) {
    return Status::OK(); // must not found, return OK
  }
  auto subReader = LowerBoundSubReader(fstring(ikey.data_, pref_len));
//if (!subReader || memcmp(subReader->prefix_.p, ikey.data_, pref_len) != 0)
  if (UNLIKELY(!subReader)) { // memcmp prefix fail is very unlikely, skip it
    return Status::OK();
  }
  if (IF_TOP_ZIP_TABLE_KEEP_EXCEPTION(0, LIKELY(0 == debugLevel_))) {
    try {
      return subReader->Get(ro, global_seqno_, ikey, get_context);
    } catch (const std::exception& ex) {
      return Status::Corruption("ToplingZipTableMultiReader::Get()", ex.what());
    }
  }
  else { // let C++ runtime handle exception, will trap at throw in debugger
    return subReader->Get(ro, global_seqno_, ikey, get_context);
  }
}

uint64_t ToplingZipTableMultiReader::ApproximateOffsetOf(
      ROCKSDB_8_X_COMMA(const ReadOptions& readopt)
      const Slice& ikey, TableReaderCaller) {
  fstring key = ExtractUserKey(ikey);
  const ToplingZipSubReader* subReader = LowerBoundSubReader(key);
  size_t numRecords;
  size_t rank;
  if (subReader == nullptr) {
    if (isReverseBytewiseOrder_) {
      subReader = &subReader_.front();
      numRecords = subReader->index_->NumKeys();
      rank = 0;
    } else {
      subReader = &subReader_.back();
      numRecords = subReader->index_->NumKeys();
      rank = numRecords;
    }
  }
  else {
    numRecords = subReader->index_->NumKeys();
    rank = subReader->ApproximateRank(key);
    TERARK_ASSERT_LE(rank, numRecords);
  }
  auto offset = uint64_t(subReader->rawReaderOffset_ +
                         1.0 * subReader->rawReaderSize_ * rank / numRecords);
  if (isReverseBytewiseOrder_) {
    return file_data_.size_ - offset;
  }
  return offset;
}

uint64_t
ToplingZipTableMultiReader::ApproximateSize(ROCKSDB_8_X_COMMA(const ReadOptions& readopt)
                                           const Slice& start,
                                           const Slice& end,
                                           TableReaderCaller caller) {
  auto off0 = ApproximateOffsetOf(ROCKSDB_8_X_COMMA(readopt)start, caller);
  auto off1 = ApproximateOffsetOf(ROCKSDB_8_X_COMMA(readopt)end, caller);
  return off1 - off0;
}

ToplingZipTableMultiReader::~ToplingZipTableMultiReader() {
  prefixSet_.risk_release_ownership();
  TERARK_VERIFY_EZ(live_iter_num_);
}

ToplingZipTableMultiReader::ToplingZipTableMultiReader(
              const ToplingZipTableFactory* table_factory)
  : TooZipTableReaderBase(table_factory)
{
}

void
ToplingZipTableMultiReader::Open(RandomAccessFileReader* file, Slice file_data,
                                 const TableReaderOptions& tro,
                                 TableMultiPartInfo& info) {
  TERARK_VERIFY(info.prefixSet_.m_fixlen >= 1);
  TERARK_VERIFY(info.prefixSet_.size() >= 2);
  TERARK_SCOPE_EXIT(info.risk_release_ownership());
  LoadCommonPart(file, tro, file_data, kToplingZipTableMagicNumber);
  const auto& ioptions = tro.ioptions;
  TableProperties* props = table_properties_.get();
  uint64_t file_size = file_data.size_;
  BlockContents offsetBlock = ReadMetaBlockE(file, file_size,
      kToplingZipTableMagicNumber, ioptions, kToplingZipTableOffsetBlock);
  TERARK_VERIFY(!offsetBlock.data.empty());
  ReadGlobalDict(tro);
  Init(info, tro);
  long long t0 = g_pf.now();
  SetupForRandomRead();
  long long t1 = g_pf.now();
  size_t keyCount = 0;
  SetCompressionOptions(subReader_.data(), subReader_.size());
  for (size_t i = 0; i < subReader_.size(); ++i) {
    auto part = &subReader_[i];
    part->index_->BuildCache(table_factory_->table_options_.indexCacheRatio);
    if (this->debugLevel_ >= 3) {
      part->index_->VerifyDictRank();
    }
    keyCount += part->index_->NumKeys();
  }
  long long t2 = g_pf.now();
  ROCKS_LOG_DEBUG(ioptions.info_log
    , "ToplingZipTableMultiReader::Open(%s): fsize = %zd, entries = %zd keys = %zd indexSize = %zd valueSize=%zd, warm up time = %6.3f'sec, build cache time = %6.3f'sec, pref_len = %d, parts = %zd\n"
    , file->file_name().c_str()
    , size_t(file_size), size_t(props->num_entries)
    , keyCount
    , size_t(props->index_size)
    , size_t(props->data_size)
    , g_pf.sf(t0, t1)
    , g_pf.sf(t1, t2)
    , info.prefixSet_.m_fixlen, info.prefixSet_.size()
  );
  if (WarmupLevel::kNone == table_factory_->table_options_.warmupLevel) {
    MmapColdize(fstring(file_data_.data_, file_data_.size_));
  }
}

void ToplingZipTableMultiReader::SetupForRandomRead() {
  const auto& tzto = table_factory_->table_options_;
  if (0 == dict_zip_size_ && hugepage_mem_.capacity() == 0) {
    if (tzto.indexMemAsResident) {
      MlockMem(dict_);
    }
    else if (WarmupLevel::kIndex == tzto.warmupLevel) {
      MmapAdvSeq(dict_);
      MmapWarmUp(dict_);
    }
  }
  for (size_t i = 0; i < subReader_.size(); ++i) {
    auto part = &subReader_[i];
    if (hugepage_mem_.capacity() == 0) {
      fstring mem = subReader_[i].index_->Memory();
      if (tzto.indexMemAsResident) {
        MlockMem(mem.p, mem.n);
      }
      else if (WarmupLevel::kIndex == tzto.warmupLevel) {
        MmapAdvSeq(mem.p, mem.n);
        MmapWarmUp(mem.p, mem.n);
      }
    }
    if (advise_random_on_open_ && tzto.warmupLevel < WarmupLevel::kValue) {
      if (part->StoreDataSize() > part->rawReaderSize_ / 2) {
        for (auto& block : part->store_->get_data_blocks())
          MmapAdvRnd(block.data);
      }
    }
    if (hugepage_mem_.capacity() == 0) {
      for (auto& block : part->store_->get_meta_blocks()) {
        if (tzto.indexMemAsResident) {
          MlockMem(block.data);
        }
        else if (WarmupLevel::kIndex == tzto.warmupLevel) {
          MmapAdvSeq(block.data);
          MmapWarmUp(block.data);
        }
      }
    }
  }
}

json ToplingZipTableMultiReader::ToWebViewJson(const json& dump_options) const {
  bool html = JsonSmartBool(dump_options, "html", true);
  json djs;
  if (!dump_options.contains("part")) {
    djs["Props.User"] = TableUserPropsToJson(
      table_properties_->user_collected_properties, dump_options);
  }
  djs["PrefixLen"] = prefixSet_.m_fixlen;
  djs["fixed_key_len"] = (long long)(table_properties_->fixed_key_len);
  djs["fixed_value_len"] = (long long)(table_properties_->fixed_value_len);
  uint64_t StoreSize = 0;
  uint64_t RawValueSize = 0;
  for (const auto& sub : subReader_) {
    StoreSize += sub.store_->mem_size();
    RawValueSize += sub.store_->total_data_size();
  }
  if (size_t DictSize = dict_.size()) {
    auto& dict_js = djs["Dict"];
    size_t RawSize = DictSize;
    size_t ZipSize = dict_zip_size_;
    ROCKSDB_JSON_SET_SIZE(dict_js, RawSize);
    ROCKSDB_JSON_SET_SIZE(dict_js, ZipSize);
    dict_js["Zip/Raw"] = double(ZipSize) / RawSize;
    dict_js["RawDict/RawData"] = double(DictSize) / RawValueSize;
    dict_js["RawDict/ZipData"] = double(DictSize) / StoreSize;
  }
  size_t HugePageMem = hugepage_mem_.size();
  ROCKSDB_JSON_SET_SIZE(djs, HugePageMem);
  {
    int part = JsonSmartInt(dump_options, "part", -1);
    if (part >= 0 && part < (int)subReader_.size()) {
      auto base = file_data_.data_ + subReader_[part].rawReaderOffset_;
      JsonWebViewSubReader(base, subReader_[part], djs, html, HugePageMem);
      return djs;
    }
  }
  auto& parts_js = djs["Parts"];
  std::string strhtml;
  char buf[128];
  strhtml.append(R"EOS(
  <script>
    function subpart(part) {
      location.href = location.href + "&part=" + part;
    }
  </script>
  <style>
    div * td {
      text-align: right;
      font-family: monospace;
    }
    div * td.left {
      text-align: left;
      font-family: Sans-serif;
    }
    div * td.monoleft {
      text-align: left;
      font-family: monospace;
    }
    div * td.center {
      text-align: center;
      font-family: Sans-serif;
    }
    .bghighlight {
      background-color: AntiqueWhite;
    }
    .emoji {
      font-family: "Apple Color Emoji","Segoe UI Emoji",NotoColorEmoji,"Segoe UI Symbol","Android Emoji",EmojiSymbols;
    }
  </style>)EOS");
  strhtml.append("<div>\n");
  //strhtml.append("");
  strhtml.append("<table border=1>\n");
  strhtml.append("<thead>");
  strhtml.append("<tr>");
  strhtml.append("<th rowspan=2>Part</th>");
  strhtml.append("<th rowspan=2>Prefix</th>");
  strhtml.append("<th colspan=2>DataSize(MB)</th>");
  strhtml.append("<th rowspan=2>Rows</th>");
  strhtml.append("<th colspan=3>ZipSize(MB)</th>");
  strhtml.append("<th colspan=3>RawSize(MB)</th>");
  strhtml.append("<th colspan=3>ZipRatio(Zip/Raw)</th>");
  strhtml.append("<th colspan=2>AvgKey</th>");
  strhtml.append("<th colspan=2>AvgValue</th>");
  strhtml.append("<th rowspan=2>IndexClass</th>");
  strhtml.append("<th rowspan=2>BlobStoreClass</th>");
  strhtml.append("</tr>");

  strhtml.append("<tr>");
  strhtml.append("<th>Zip</th>");
  strhtml.append("<th>Raw</th>");

  strhtml.append("<th>Key</th>");
  strhtml.append("<th>Value</th>");
  strhtml.append("<th title='size ratio of Key/(Key + Value)'>K/KV</th>");
  strhtml.append("<th>Key</th>");
  strhtml.append("<th>Value</th>");
  strhtml.append("<th title='size ratio of Key/(Key + Value)'>K/KV</th>");

  strhtml.append("<th>Key</th>");     // Zip Ratio
  strhtml.append("<th>Value</th>");
  strhtml.append("<th>K+V</th>");

  strhtml.append("<th>Zip</th>");  // AvgLen
  strhtml.append("<th>Raw</th>");
  strhtml.append("<th>Zip</th>");
  strhtml.append("<th>Raw</th>");

  strhtml.append("</tr>\n");
  strhtml.append("</thead>");

  strhtml.append("<tbody>");
#define AppendFmt(...) strhtml.append(buf, snprintf(buf, sizeof(buf), __VA_ARGS__))
  for (size_t i = 0; i < subReader_.size(); i++) {
    const auto& sub = subReader_[i];
    strhtml.append("<tr>");
    AppendFmt("<th><a href='javascript:subpart(%zd)'>%zd</a></th>", i, i);
    strhtml.append("<td>");
    if (sub.prefix_len_ <= 8 && 0) {
      // TODO:
    } else {
      strhtml.append(SliceOf(sub.prefix()).hex());
    }
    strhtml.append("</td>");
    double raw_key_size = sub.index_->TotalKeySize();
    double raw_val_size = sub.store_->total_data_size();
    double raw_kv_size = sub.index_->TotalKeySize() + sub.store_->total_data_size();
    double zip_kv_size = sub.rawReaderSize_;
    size_t rows = sub.index_->NumKeys();
    AppendFmt("<td>%.3f</td>", zip_kv_size/1e6);
    AppendFmt("<td>%.3f</td>", raw_kv_size/1e6);
    AppendFmt("<td>%zd</td>", rows);

    size_t zip_val_size = sub.store_->mem_size() - sub.store_->get_dict().memory.size();
    size_t zip_key_size = sub.index_->Memory().size();
    AppendFmt("<td>%.3f</td>", zip_key_size/1e6);
    AppendFmt("<td>%.3f</td>", zip_val_size/1e6);
    AppendFmt("<td>%.3f</td>", zip_key_size*1.0/zip_kv_size);

    AppendFmt("<td>%.3f</td>", raw_key_size/1e6);
    AppendFmt("<td>%.3f</td>", raw_val_size/1e6);
    AppendFmt("<td>%.3f</td>", raw_key_size*1.0/raw_kv_size);

    AppendFmt("<td>%.3f</td>", zip_key_size*1.0/raw_key_size);
    AppendFmt("<td>%.3f</td>", zip_val_size*1.0/raw_val_size);
    AppendFmt("<td>%.3f</td>", zip_kv_size*1.0/raw_kv_size);

    AppendFmt("<td>%.3f</td>", zip_key_size*1.0/rows);
    AppendFmt("<td>%.3f</td>", raw_key_size*1.0/rows);
    AppendFmt("<td>%.3f</td>", zip_val_size*1.0/rows);
    AppendFmt("<td>%.3f</td>", raw_val_size*1.0/rows);

    strhtml.append("<td class='left'>");
    strhtml.append(sub.index_->Name());
    strhtml.append("</td>");
    strhtml.append("<td class='left'>");
    strhtml.append(sub.store_->name());
    strhtml.append("</td>");
    strhtml.append("</tr>\n");
  }
  strhtml.append("</tbody>\n");
  strhtml.append("</table>\n");
  strhtml.append("</div>\n");
  parts_js = std::move(strhtml);
  djs["stat"] = json{
    {"cumu_iter_num", cumu_iter_num_},
    {"live_iter_num", live_iter_num_},
    {"iter_seek_cnt", iter_seek_cnt},
    {"iter_next_cnt", iter_next_cnt},
    {"iter_prev_cnt", iter_prev_cnt},
    {"iter_key_len", iter_key_len},
    {"iter_val_len", iter_val_len},
  };
  return djs;
}

std::string ToplingZipTableMultiReader::ToWebViewString(const json& dump_options) const {
  auto djs = ToWebViewJson(dump_options);
  return JsonToString(djs, dump_options);
}

///////////////////////////////////////////////////////////////////////////
Status
ToplingZipTableFactory::NewTableReader(
              const ReadOptions& ro,
              const TableReaderOptions& tro,
              unique_ptr<RandomAccessFileReader>&& file_up,
              uint64_t file_size, unique_ptr<TableReader>* table,
              bool prefetch_index_and_filter_in_cache)
const try {
  void PrintVersionHashInfo(rocksdb::Logger*);
  void PrintVersionHashInfo(const std::shared_ptr<rocksdb::Logger>&);
  PrintVersionHashInfo(tro.ioptions.info_log);
  Footer footer;
  Slice file_data;
  auto file = file_up.get();
  file->exchange(new MmapReadWrapper(file));
  Status s = TopMmapReadAll(*file, file_size, &file_data);
  if (!s.ok()) {
    return s;
  }
  if (WarmupLevel::kValue == table_options_.warmupLevel) {
    MmapAdvSeq(file_data);
    MmapWarmUp(file_data);
  }
  s = ReadFooterFromFile(IOOptions(), file, nullptr, file_size, &footer);
  if (!s.ok()) {
    return s;
  }
  if (footer.table_magic_number() != kToplingZipTableMagicNumber) {
    return Status::InvalidArgument(
      "ToplingZipTableFactory::NewTableReader()",
      "fallback_factory is null and magic_number is not kToplingZipTable"
    );
  }
  if (footer.format_version() > 2) {
    auto ver = std::to_string(footer.format_version());
    return Status::NotSupported("SST format version = " + ver + " > 2");
  }
  try {
    BlockContents emptyTableBC = ReadMetaBlockE(file, file_size,
        kToplingZipTableMagicNumber, tro.ioptions, kTopEmptyTableKey);
    TERARK_VERIFY(!emptyTableBC.data.empty());
    std::unique_ptr<TopEmptyTableReader> t(new TopEmptyTableReader());
    t->Open(file, file_data, tro);
    *table = std::move(t);
    file_up.release();
    return Status::OK();
  }
  catch (const Status&) {
    // ignore
  }
  BlockContents offsetBC  = ReadMetaBlockE(file, file_size,
      kToplingZipTableMagicNumber, tro.ioptions, kToplingZipTableOffsetBlock);
  TERARK_VERIFY(!offsetBC.data.empty());
  TableMultiPartInfo info;
  TERARK_SCOPE_EXIT(info.risk_release_ownership());
  if (info.risk_set_memory(offsetBC.data)) {
    BlockContents krceBlock;
    try {
      krceBlock = ReadMetaBlockE(file, file_size, kToplingZipTableMagicNumber,
          tro.ioptions, kZipTableKeyRankCacheBlock);
      info.krceVec_ = (const KeyRankCacheEntry*)krceBlock.data.data_;
      ROCKSDB_VERIFY_EQ(krceBlock.data.size(),
                        sizeof(KeyRankCacheEntry) * info.prefixSet_.size());
      MmapAdvSeq(krceBlock.data);
      MmapWarmUp(krceBlock.data);
    } catch (const Status&) {
      // ignore
    }
    if (info.prefixSet_.size() > 1) {
      TERARK_VERIFY(info.prefixSet_.m_fixlen >= 1);
      auto t = std::make_unique<ToplingZipTableMultiReader>(this);
      t->Open(file, file_data, tro, info);
      *table = std::move(t);
    } else {
      auto t = std::make_unique<ToplingZipTableReader>(this);
      t->Open(file, file_data, tro, info);
      *table = std::move(t);
    }
    file_up.release();
    return Status::OK();
  }
  return Status::InvalidArgument("ToplingZipTableFactory::NewTableReader()",
                                 "bad TableMultiPartInfo");
}
catch (const IOStatus& es) {
  return Status::IOError(ROCKSDB_FUNC, es.ToString());
}
catch (const Status& es) {
  return es;
}
catch (const std::exception& ex) {
  return Status::Corruption(ROCKSDB_FUNC, ex.what());
}

} // namespace rocksdb
