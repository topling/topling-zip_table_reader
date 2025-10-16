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
  #pragma warning(disable: 4458) // index_ hide member(intentional)
#endif

namespace rocksdb {
using namespace terark;

fstring COIndexGetClassName(const std::type_info&);
void VerifyClassName(const char* class_name, const std::type_info&);

struct UintIndexBase : public COIndex {
  static const char index_name[10];
  struct MyBaseFileHeader : public ToplingIndexHeader
  {
    uint64_t min_value;
    uint64_t max_value;
    uint64_t index_mem_size;
    uint32_t key_length;
    uint32_t common_prefix_length;

    MyBaseFileHeader(size_t body_size, const std::type_info& ti) {
      memset(this, 0, sizeof *this);
      magic_len = strlen(index_name);
      strncpy(magic, index_name, sizeof magic);
      fstring clazz = COIndexGetClassName(ti);
      TERARK_VERIFY_LT(clazz.size(), sizeof class_name);
      memcpy(class_name, clazz.c_str(), clazz.size());
      header_size = sizeof(*this);
      version = 1;
      file_size = sizeof(*this) + body_size;
    }
  };
  class MyBaseIterator : public COIndex::FastIter {
  public:
   #if defined(_MSC_VER)
    #define buffer_ this->get_buffer()
   #endif
    #define max_pos_ m_num
    MyBaseIterator(const UintIndexBase& index, const char* commonPrefix) : index_(index) {
      minValue_ = index_.minValue_;
      max_pos_ = index.maxValue_ - index.minValue_;
      pos_ = size_t(-1);
      full_len_ = index_.commonPrefixLen_ + index_.keyLength_;
      pref_len_ = index_.commonPrefixLen_;
      suff_len_ = index_.keyLength_;
      memcpy(buffer_, commonPrefix, index_.commonPrefixLen_);
      m_key = fstring(buffer_, full_len_); // will not change in the life time
    }

    bool SeekToFirst() override {
      m_id = 0;
      pos_ = 0;
      UpdateBuffer();
      return true;
    }
    size_t DictRank() const override {
      assert(m_id != size_t(-1));
      return m_id;
    }
  protected:
    void UpdateBuffer() {
      SaveAsBigEndianUint64(buffer_ + pref_len_, suff_len_, pos_ + minValue_);
    }
    void Delete() override final {
      // no need to call destructor
      free(this);
    }
    const UintIndexBase& index_;
    uint64_t minValue_;
    ushort full_len_;
    ushort pref_len_;
    ushort suff_len_;
    //size_t max_pos_;
    size_t pos_;
   #if defined(_MSC_VER)
    byte_t* get_buffer() { return (byte_t*)(this + 1); }
    const byte_t* get_buffer() const { return (const byte_t*)(this + 1); }
   #else
    byte_t buffer_[0];
   #endif
  };

  class MyBaseFactory : public COIndex::Factory {
  public:
    size_t MemSizeForBuild(const KeyStat& ks) const override {
      assert(ks.minKeyLen == ks.maxKeyLen);
      size_t length = ks.maxKeyLen - commonPrefixLen(ks.minKey, ks.maxKey);
      auto minValue = ReadBigEndianUint64(ks.minKey.begin(), length);
      auto maxValue = ReadBigEndianUint64(ks.maxKey.begin(), length);
      if (minValue > maxValue) {
        std::swap(minValue, maxValue);
      }
      uint64_t diff = maxValue - minValue + 1;
      return size_t(std::ceil(diff * 1.25 / 8));
    }
    void loadCommonPart(UintIndexBase* ptr, size_t obj_size, fstring& mem, fstring fpath) const {
      ptr->workingState_ = WorkingState::UserMemory;
      if (mem.data() == nullptr) {
        MmapWholeFile mmapFile(fpath);
        mem = mmapFile.memory();
        //MmapWholeFile().swap(mmapFile);
        mmapFile.base = nullptr;
        ptr->workingState_ = WorkingState::MmapFile;
      }
      using FormatError = std::logic_error;
      auto header = ptr->header_ = (const MyBaseFileHeader*)mem.data();
      TERARK_EXPECT_GT(mem.size(), sizeof(MyBaseFileHeader), FormatError);
      TERARK_EXPECT_EQ(align_up(header->file_size, 64), align_up(mem.size(), 64), FormatError);
      TERARK_EXPECT_LE(header->file_size, mem.size(), FormatError);
      TERARK_EXPECT_EQ(header->header_size, sizeof(MyBaseFileHeader), FormatError);
      TERARK_EXPECT_EQ(header->magic_len  , strlen(index_name), FormatError);
      TERARK_EXPECT_F(strcmp(header->magic, index_name) == 0, FormatError,
                    "%s %s", header->magic, index_name);
      TERARK_EXPECT_EQ(header->version, 1, FormatError);
      ptr->minValue_ = header->min_value;
      ptr->maxValue_ = header->max_value;
      ptr->keyLength_ = header->key_length;
      const size_t cplen = header->common_prefix_length;
      if (cplen > 0) {
        auto cpsrc = mem.data() + sizeof(MyBaseFileHeader);
        auto cpdst = (char*)ptr + obj_size;
        memcpy(cpdst, cpsrc, cplen);
      }
      ptr->commonPrefixLen_ = uint32_t(cplen);
    }
  };

  virtual ~UintIndexBase() {
    if (workingState_ == WorkingState::Building) {
      delete header_;
    }
    else {
      if (workingState_ == WorkingState::MmapFile) {
        terark::mmap_close((void*)header_, header_->file_size);
      }
    }
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
    js["CommonPrefixLen"] = commonPrefixLen_;
    js["KeyLen"] = keyLength_;
    js["Version"] = header_->version;
    js["HeaderSize"] = header_->header_size;
  }

  const MyBaseFileHeader* header_;
  uint64_t          minValue_;
  uint64_t          maxValue_;
  uint32_t          commonPrefixLen_;
  uint16_t          keyLength_; // must <= 8
  WorkingState      workingState_;
};
const char UintIndexBase::index_name[10] = "UintIndex";

template<class RankSelect>
class UintIndex : public UintIndexBase {
  const char* commonPrefixData() const { return (const char*)(this + 1); }
  void* AllocIterMem() const {
    // extra 8 bytes for KeyTag(SeqNum & ValueType)
    // 64u is for speed up read key access: callers may read beyond ikey mem
    auto ikey_cap = std::max(commonPrefixLen_ + keyLength_ + 8, 64u);
   #if defined(_MSC_VER)
    auto mem = (char*)malloc(sizeof(MyBaseIterator) + ikey_cap);
   #else
    auto mem = (char*)aligned_alloc(64, pow2_align_up(sizeof(MyBaseIterator) + ikey_cap, 64));
   #endif
    memset(mem + sizeof(MyBaseIterator), 0, ikey_cap);
    return mem;
  }
public:
  struct FileHeader : public MyBaseFileHeader {
    FileHeader(size_t body_size)
      : MyBaseFileHeader(body_size, typeid(UintIndex)) {}
  };
  bool SeekImpl(fstring target, size_t& id, size_t& pos) const {
    auto cpdata = commonPrefixData();
    size_t cplen = target.commonPrefixLen(cpdata, commonPrefixLen_);
    if (cplen != commonPrefixLen_) {
      assert(target.size() >= cplen);
      assert(target.size() == cplen
        || byte_t(target[cplen]) != byte_t(cpdata[cplen]));
      if (target.size() == cplen
        || byte_t(target[cplen]) < byte_t(cpdata[cplen])) {
        id = 0;
        pos = 0;
        return true;
      }
      else {
        id = size_t(-1);
        return false;
      }
    }
    target = target.substr(cplen);
    /*
     *    target.size()     == 4;
     *    index_.keyLength_ == 6;
     *    | - - - - - - - - |  <- buffer
     *        | - - - - - - |  <- index
     *        | - - - - |      <- target
     */
    byte_t targetBuffer[8] = {};
    memcpy(targetBuffer + (8 - keyLength_),
      target.data(), std::min<size_t>(keyLength_, target.size()));
    uint64_t targetValue = ReadBigEndianUint64Aligned(targetBuffer, 8);
    if (UNLIKELY(targetValue > maxValue_)) {
      id = size_t(-1);
      return false;
    }
    if (UNLIKELY(targetValue < minValue_)) {
      id = 0;
      pos = 0;
      return true;
    }
    pos = targetValue - minValue_;
    id = indexSeq_.rank1(pos);
    if (!indexSeq_[pos]) {
      pos += indexSeq_.zero_seq_len(pos);
    }
    else if (UNLIKELY(target.size() > keyLength_)) {
      // minValue:  target
      // targetVal: targetvalue.1
      // maxValue:  targetvau
      if (pos == indexSeq_.size() - 1) {
        id = size_t(-1);
        return false;
      }
      ++id;
      pos = pos + indexSeq_.zero_seq_len(pos + 1) + 1;
    }
    return true;
  }
  class UintIndexIterator : public MyBaseIterator {
  public:
    UintIndexIterator(const UintIndex& index) : MyBaseIterator(index, index.commonPrefixData()) {}
    bool SeekToLast() override {
      auto& index_ = static_cast<const UintIndex&>(this->index_);
      m_id = index_.indexSeq_.max_rank1() - 1;
      pos_ = index_.indexSeq_.size() - 1;
      UpdateBuffer();
      return true;
    }
    bool Seek(fstring target) override {
      auto& index = static_cast<const UintIndex&>(this->index_);
      if (index.SeekImpl(target, m_id, pos_)) {
        UpdateBuffer();
        return true;
      }
      return false;
    }
  };
  template<int SuffixLen>
  class UintIndexIterTmpl : public UintIndexIterator {
  public:
    using UintIndexIterator::UintIndexIterator;
   #if !defined(_MSC_VER)
    using UintIndexIterator::buffer_;
   #endif
    using UintIndexIterator::pref_len_;
    using UintIndexIterator::suff_len_;
    using UintIndexIterator::pos_;
    using UintIndexIterator::m_id;
    using UintIndexIterator::max_pos_;
    using UintIndexIterator::minValue_;
    void UpdateBufferConstLen() {
      assert(SuffixLen == suff_len_);
      SaveAsBigEndianUint64(buffer_ + pref_len_, SuffixLen, pos_ + minValue_);
    }
    bool Next() override {
      auto& index_ = static_cast<const UintIndex&>(this->index_);
      assert(m_id != size_t(-1));
      assert(index_.indexSeq_[pos_]);
      assert(index_.indexSeq_.rank1(pos_) == m_id);
      if (UNLIKELY(pos_ >= max_pos_)) {
        m_id = size_t(-1);
        return false;
      }
      else {
        ++m_id;
        pos_ = pos_ + index_.indexSeq_.zero_seq_len(pos_ + 1) + 1;
        UpdateBufferConstLen();
        return true;
      }
    }
    bool Prev() override {
      auto& index_ = static_cast<const UintIndex&>(this->index_);
      assert(m_id != size_t(-1));
      assert(index_.indexSeq_[pos_]);
      assert(index_.indexSeq_.rank1(pos_) == m_id);
      if (m_id == 0) {
        m_id = size_t(-1);
        return false;
      }
      else {
        --m_id;
        pos_ = pos_ - index_.indexSeq_.zero_seq_revlen(pos_) - 1;
        UpdateBufferConstLen();
        return true;
      }
    }
  };
  class MyFactory : public MyBaseFactory {
  public:
    COIndex* Build(const std::string& keyFilePath,
                       const ToplingZipTableOptions& tzopt,
                       const KeyStat& ks,
                       const ImmutableOptions* ioptions) const override {
    /* can be rank_select_allone
      if (std::is_same<RankSelect, rank_select_allone>::value) {
        abort(); // will not happen
        return NULL; // let compiler to eliminate dead code
      }
    */
      const size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
      const size_t reclen = ks.minKeyLen;
      const size_t effectiveLen = ks.minKeyLen - cplen;
      TERARK_VERIFY_EQ(ks.maxKeyLen, ks.minKeyLen);
      TERARK_VERIFY_LE(ks.maxKeyLen, cplen + sizeof(uint64_t));
      TERARK_VERIFY_EQ(ks.sumKeyLen, ks.minKeyLen * ks.numKeys);
      auto minValue = ReadBigEndianUint64(ks.minKey.begin() + cplen, ks.minKey.end());
      auto maxValue = ReadBigEndianUint64(ks.maxKey.begin() + cplen, ks.maxKey.end());
      if (minValue > maxValue) {
        std::swap(minValue, maxValue);
      }
      assert(maxValue - minValue < UINT64_MAX); // must not overflow
      RankSelect indexSeq;
      indexSeq.resize(maxValue - minValue + 1);
      if (!std::is_same<RankSelect, terark::rank_select_allone>::value) {
        // not 'all one' case
        MmapWholeFile fm(keyFilePath, false, true); // readonly populate
        TERARK_VERIFY_EQ(fm.size, reclen * ks.numKeys);
        auto p = (const byte_t*)fm.base + cplen;
        for (size_t seq_id = 0, n = ks.numKeys; seq_id < n; ++seq_id) {
          auto cur = ReadBigEndianUint64(p, effectiveLen);
          indexSeq.set1(cur - minValue);
          p += reclen;
        }
        indexSeq.build_cache(false, false);
      }

      auto full_cplen = ks.prefix.size() + cplen;
      auto raw = (char*)malloc(align_up(sizeof(UintIndex<RankSelect>) + full_cplen, 16));
      auto ptr = new(raw)UintIndex<RankSelect>();
      COIndexUP ptr_up(ptr); // guard
      ptr->workingState_ = WorkingState::Building;
      MinMemIO mio(raw + sizeof(UintIndex<RankSelect>));
      mio.ensureWrite(ks.prefix.data(), ks.prefix.size());
      mio.ensureWrite(ks.minKey.data(), cplen);
      ptr->commonPrefixLen_ = uint32_t(full_cplen);

      FileHeader *header = new FileHeader(indexSeq.mem_size());
      header->min_value = minValue;
      header->max_value = maxValue;
      header->index_mem_size = indexSeq.mem_size();
      header->key_length = uint32_t(ks.minKeyLen - cplen);
      header->common_prefix_length = uint32_t(full_cplen);
      header->file_size = align_up(header->file_size + full_cplen, 8);
      ptr->header_ = header;
      ptr->indexSeq_.swap(indexSeq);
      ptr->minValue_ = minValue;
      ptr->maxValue_ = maxValue;
      ptr->keyLength_ = header->key_length;
      ptr->m_num_keys = ptr->indexSeq_.max_rank1();
      ptr_up.release(); // must release
      return ptr;
    }
    COIndexUP LoadMemory(fstring mem) const override {
      return COIndexUP(loadImpl(mem, {}));
    }
    COIndexUP LoadFile(fstring fpath) const override {
      return COIndexUP(loadImpl({}, fpath));
    }
    COIndex* loadImpl(fstring mem, fstring fpath) const {
      auto header = (const FileHeader*)(mem.data());
     #if defined(_MSC_VER)
      auto raw = malloc(align_up(sizeof(UintIndex) + header->common_prefix_length, 16));
     #else
      auto raw = aligned_alloc(64, align_up(sizeof(UintIndex) + header->common_prefix_length, 64));
     #endif
      auto ptr = new(raw)UintIndex();
      COIndexUP ptr_up(ptr); // guard
      loadCommonPart(ptr, sizeof(UintIndex), mem, fpath);
      VerifyClassName(header->class_name, typeid(UintIndex));
      ptr->indexSeq_.risk_mmap_from((unsigned char*)mem.data() + header->header_size
        + align_up(header->common_prefix_length, 8), header->index_mem_size);
      ptr->m_num_keys = ptr->indexSeq_.max_rank1();
      ptr_up.release(); // must release
      return ptr;
    }
  };
  using COIndex::FactoryPtr;
private:
  virtual ~UintIndex() {
    if (workingState_ == WorkingState::Building) {
      // do nothing
    }
    else {
      indexSeq_.risk_release_ownership();
    }
  }
  void Delete() override {
    this->~UintIndex();
    free(this);
  }
public:
  void SaveMmap(std::function<void(const void *, size_t)> write) const override {
    write(header_, sizeof *header_);
    if (commonPrefixLen_) {
      write(commonPrefixData(), commonPrefixLen_);
      Padzero<8>(write, commonPrefixLen_);
    }
    write(indexSeq_.data(), indexSeq_.mem_size());
  }
  size_t Find(fstring key) const override {
    const size_t cplen = commonPrefixLen_;
    if (UNLIKELY(key.size() != cplen + keyLength_)) {
      return size_t(-1);
    }
    // ToplingZipTableOptions.prefixLen <= 16 but there may be more
    // common prefix after prefixLen, so max cplen may > 16 but must <= 32
    TERARK_ASSERT_LE(cplen, 32);
    if (UNLIKELY(!BytesEqualMaxLen32(key.data(), commonPrefixData(), cplen))) {
      return size_t(-1);
    }
    uint64_t findValue = ReadBigEndianUint64(key.data() + cplen, keyLength_);
    if (UNLIKELY(findValue < minValue_ || findValue > maxValue_)) {
      return size_t(-1);
    }
    uint64_t findPos = findValue - minValue_;
    if (UNLIKELY(!indexSeq_[findPos])) {
      return size_t(-1);
    }
    return indexSeq_.rank1(findPos);
  }
  size_t AccurateRank(fstring key) const final {
    size_t id, pos;
    if (this->SeekImpl(key, id, pos)) {
      return id;
    }
    return indexSeq_.max_rank1();
  }
  size_t ApproximateRank(fstring key) const final {
    return AccurateRank(key);
  }
  size_t TotalKeySize() const override final {
    return (commonPrefixLen_ + keyLength_) * indexSeq_.max_rank1();
  }
  std::string NthKey(size_t nth) const final {
    TERARK_VERIFY_LT(nth, m_num_keys);
    const size_t pref_len = commonPrefixLen_;
    const size_t suff_len = keyLength_;
    std::string key;
    key.reserve(pref_len + suff_len);
    key.append(commonPrefixData(), pref_len);
    key.resize(pref_len + suff_len);
    size_t val = indexSeq_.select1(nth);
    SaveAsBigEndianUint64((byte_t*)key.data() + pref_len, suff_len, minValue_ + val);
    TERARK_ASSERT_EQ(Find(key), nth);
    return key;
  }
  Iterator* NewIterator() const final {
    void* mem = this->AllocIterMem();
    switch (keyLength_) {
      case 0: return new(mem)UintIndexIterTmpl<0>(*this);
      case 1: return new(mem)UintIndexIterTmpl<1>(*this);
      case 2: return new(mem)UintIndexIterTmpl<2>(*this);
      case 3: return new(mem)UintIndexIterTmpl<3>(*this);
      case 4: return new(mem)UintIndexIterTmpl<4>(*this);
      case 5: return new(mem)UintIndexIterTmpl<5>(*this);
      case 6: return new(mem)UintIndexIterTmpl<6>(*this);
      case 7: return new(mem)UintIndexIterTmpl<7>(*this);
      case 8: return new(mem)UintIndexIterTmpl<8>(*this);
    }
    ROCKSDB_DIE("should not goes here");
  }
  json MetaToJson() const final {
    json js;
    js["CommonPrefix"] = Slice(commonPrefixData(), commonPrefixLen_).hex();
    MetaToJsonCommon(js);
    js["RankSelect.NumBits"] = indexSeq_.size();
    js["RankSelect.MemSize"] = indexSeq_.mem_size();
    js["RankSelect.MaxRank0"] = indexSeq_.max_rank0();
    js["RankSelect.MaxRank1"] = indexSeq_.max_rank1();
    return js;
  }
protected:
  RankSelect        indexSeq_;
};

#define RegisterUintIndex(RankSelect) \
  typedef UintIndex<RankSelect> UintIndex_##RankSelect; \
            COIndexRegister(UintIndex_##RankSelect)
RegisterUintIndex(IL_256_32);
RegisterUintIndex(SE_512_64);
RegisterUintIndex(AllOne);

} // namespace rocksdb
