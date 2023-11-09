#include "co_index.h"
#include "top_zip_table.h"
#include <table/top_table_common.h>
#include <terark/fsa/dfa_mmap_header.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/zbs/zip_reorder_map.hpp>
#include <topling/json.h>
#include "rs_index_common.h"

namespace rocksdb {

using namespace terark;

fstring COIndexGetClassName(const std::type_info&);
void MmapRead_FixedLenStrVec(fstring, const COIndex::KeyStat&, FixedLenStrVec&);
void MmapRead_ReadSortedStrVec(fstring, const COIndex::KeyStat&, SortedStrVec&);

struct CheckOffset : StringLexIterator {
  CheckOffset() {
    constexpr auto nlt_key_offset = sizeof(COIndex::Iterator) + offsetof(CheckOffset, m_word);
    static_assert(offsetof(COIndex::FastIter, m_key) == nlt_key_offset);
  }
};
struct Check_valvec_fstring_layout : valvec<byte_t> {
  Check_valvec_fstring_layout() {
    static_assert(offsetof(Check_valvec_fstring_layout, p) == offsetof(fstring, p));
    static_assert(offsetof(Check_valvec_fstring_layout, n) == offsetof(fstring, n));
  }
};

// 1. Use template just to inline dawg->state_to_word_id().
// 2. If we prefer smaller code size, pay a virtual call to
//    dawg->v_state_to_word_id(), NestLoudsTrieIter can be non-template
template<class NLTrie>
class NestLoudsTrieIter : public COIndex::Iterator {
protected:
  using COIndex::Iterator::m_id;
  // 1. For minimum dependency, NLTrie::Iterator can be ADFA_LexIterator
  // 2. For better debug show object structure, use NLTrie::Iterator
  auto& dawg_iter() const { return *(typename NLTrie::Iterator*)(this + 1); }
  bool Done(bool ok) {
    auto dawg = static_cast<const NLTrie*>(dawg_iter().get_dfa());
    if (ok)
      m_id = dawg->state_to_word_id(dawg_iter().word_state());
    else
      m_id = size_t(-1);
    return ok;
  }
  NestLoudsTrieIter() = default;

public:
  static NestLoudsTrieIter* new_iter(const NLTrie* trie) {
    // layout:
    //    1. NestLoudsTrieIter
    //    2. TrieDAWG Iterator
    size_t iter_mem_size = sizeof(NestLoudsTrieIter) + trie->iter_mem_size();
    auto iter = (NestLoudsTrieIter*)malloc(iter_mem_size);
    new(iter)NestLoudsTrieIter(); // init m_id and v-table
    trie->cons_iter(iter + 1); // cons TrieDAWG Iterator
    return iter;
  }
  void Delete() override {
    // no destruction needed, just free memory
    free(this);
  }
  bool SeekToFirst() override { return Done(dawg_iter().seek_begin()); }
  bool SeekToLast()  override { return Done(dawg_iter().seek_end()); }
  bool Seek(fstring key) override { return Done(dawg_iter().seek_lower_bound(key)); }
  bool Next() override { return Done(dawg_iter().incr()); }
  bool Prev() override { return Done(dawg_iter().decr()); }
  size_t DictRank() const override {
	  auto dawg = static_cast<const NLTrie*>(dawg_iter().get_dfa());
    assert(m_id != size_t(-1));
    return dawg->state_to_dict_rank(dawg_iter().word_state());
  }
};

template<class NLTrie>
void NestLoudsTrieBuildCache(NLTrie* trie, double cacheRatio) {
  trie->build_fsa_cache(cacheRatio, NULL);
}

template<class NLTrie>
void NestLoudsTrieGetOrderMap(const NLTrie* trie, UintVecMin0& newToOld) {
  NonRecursiveDictionaryOrderToStateMapGenerator gen;
  gen(*trie, [&](size_t dictOrderOldId, size_t state) {
    size_t newId = trie->state_to_word_id(state);
    //assert(trie->state_to_dict_index(state) == dictOrderOldId);
    //assert(trie->dict_index_to_state(dictOrderOldId) == state);
    newToOld.set_wire(newId, dictOrderOldId);
  });
}

struct NestLoudsTrieIndexBase : public COIndex {
  const BaseDAWG* m_dawg;
  unique_ptr<BaseDFA> m_trie;

  struct MyBaseFactory : public Factory {
    template<class StrVec>
    static
    void Set_NestLoudsTrieConfig(NestLoudsTrieConfig& conf,
                                 const StrVec& keyVec,
                                 const ToplingZipTableOptions& tzopt) {
      Set_NestLoudsTrieConfig(conf, keyVec.mem_size(), keyVec.avg_size(), tzopt);
    }
    // non-template, common func of template Set_NestLoudsTrieConfig
    terark_no_inline static
    void Set_NestLoudsTrieConfig(NestLoudsTrieConfig& conf,
                                 size_t mem_size, double avglen,
                                 const ToplingZipTableOptions& tzopt) {
      conf.nestLevel = tzopt.indexNestLevel;
      conf.nestScale = tzopt.indexNestScale;
      if (tzopt.indexTempLevel >= 0 && tzopt.indexTempLevel < 5) {
        if (mem_size > tzopt.smallTaskMemory) {
          // use tmp files during index building
          conf.tmpDir = tzopt.localTempDir;
          if (0 == tzopt.indexTempLevel) {
            // adjust tmpLevel for linkVec, wihch is proportional to num of keys
            if (mem_size > tzopt.smallTaskMemory*2 && avglen <= 50) {
              // not need any mem in BFS, instead 8G file of 4G mem (linkVec)
              // this reduce 10% peak mem when avg keylen is 24 bytes
              if (avglen <= 30) {
                // write str data(each len+data) of nestStrVec to tmpfile
                conf.tmpLevel = 4;
              } else {
                // write offset+len of nestStrVec to tmpfile
                // which offset is ref to outer StrVec's data
                conf.tmpLevel = 3;
              }
            }
            else if (mem_size > tzopt.smallTaskMemory*3/2) {
              // for example:
              // 1G mem in BFS, swap to 1G file after BFS and before build nextStrVec
              conf.tmpLevel = 2;
            }
          }
          else {
            conf.tmpLevel = tzopt.indexTempLevel;
          }
        }
      }
      if (tzopt.indexTempLevel >= 5) {
        // always use max tmpLevel 4
        conf.tmpDir = tzopt.localTempDir;
        conf.tmpLevel = 4;
      }
      conf.isInputSorted = true;
    }

    template<class StrVec>
    static void AssertSorted(const StrVec& keyVec) {
#if !defined(NDEBUG)
      for (size_t i = 1; i < keyVec.size(); ++i) {
        fstring prev = keyVec[i - 1];
        fstring curr = keyVec[i];
        assert(prev < curr);
      }
#endif
    }

    size_t MemSizeForBuild(const KeyStat& ks) const override {
      size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
      size_t sumRealKeyLen = ks.sumKeyLen - cplen * ks.numKeys;
      if (ks.minKeyLen == ks.maxKeyLen) {
        return sumRealKeyLen;
      }
      size_t indexSize = UintVecMin0::compute_mem_size_by_max_val(ks.numKeys + 1, sumRealKeyLen);
      return indexSize + sumRealKeyLen;
    }
    COIndex* Build(const std::string& keyFilePath,
                       const ToplingZipTableOptions& tzopt,
                       const KeyStat& ks,
                       const ImmutableOptions* iopt) const override {
      NestLoudsTrieConfig conf;
      conf.initFromEnv();
      conf.speedupNestTrieBuild = tzopt.speedupNestTrieBuild;
      size_t cplen = commonPrefixLen(ks.minKey, ks.maxKey);
      conf.commonPrefix = ks.prefix + fstring(ks.minKey).prefix(cplen);
      if (ks.minKeyLen == ks.maxKeyLen) {
        COIndex* index = nullptr;
        {
          FixedLenStrVec keyVec;
          MmapRead_FixedLenStrVec(keyFilePath, ks, keyVec);
          index = VirtBuild(tzopt, keyVec, conf);
        }
        size_t nltMemSize = 0;
        auto getSize = [&](const void*, size_t len) {
          nltMemSize += len;
        };
        index->SaveMmap(std::ref(getSize));
        size_t holeLen = ks.ComputeMiddleHoleLen(cplen);
        size_t fixlen = ks.minKeyLen - cplen - holeLen;
        size_t fixIndexMemSize = fixlen * ks.numKeys;
        if (nltMemSize > fixIndexMemSize * tzopt.nltAcceptCompressionRatio) {
          if (tzopt.debugLevel >= 2) {
            if (0 == holeLen)
              STD_INFO("File(%s): fallback to FixedLenKeyIndex for speed", keyFilePath.c_str());
            else
              STD_INFO("File(%s): use FixedLenHoleIndex(%zd) for speed", keyFilePath.c_str(), holeLen);
          }
          auto fac = GetFactory(holeLen ? "FixedLenHoleIndex" : "FixedLenKeyIndex");
          TERARK_VERIFY_F(nullptr != fac, "holeLen = %zd", holeLen);
          index->Delete();
          index = fac->Build(keyFilePath, tzopt, ks, iopt);
        }
        return index;
      }
      else {
        SortedStrVec keyVec;
        TERARK_VERIFY_EQ(ks.keyOffsets.size(), ks.numKeys + 1);
        MmapRead_ReadSortedStrVec(keyFilePath, ks, keyVec);
        return VirtBuild(tzopt, keyVec, conf);
      }
    }
    virtual COIndex* VirtBuild(const ToplingZipTableOptions&,
                           FixedLenStrVec& keyVec,
                           NestLoudsTrieConfig&) const = 0;
    virtual COIndex* VirtBuild(const ToplingZipTableOptions&,
                           SortedStrVec& keyVec,
                           NestLoudsTrieConfig&) const = 0;
  };

  explicit NestLoudsTrieIndexBase(BaseDFA* trie) : m_trie(trie) {
    m_dawg = trie->get_dawg();
    m_num_keys = m_dawg->num_words();
  }

  const char* Name() const override {
    if (m_trie->is_mmap()) {
      auto header = (const ToplingIndexHeader*)m_trie->get_mmap().data();
      return header->class_name;
    }
    else {
      return COIndexGetClassName(typeid(*this)).c_str();
    }
  }

  json MetaToJson() const final {
    auto trie = m_trie.get();
    DFA_MmapHeader header = {};
    trie->get_stat(&header);
    json js;
    js["NestLevel"] = trie->zp_nest_level();
    js["NumStates"] = trie->v_total_states();
    //js["NumKeys"] = trie->num_words();
    //js["MemSize"] = trie->mem_size();
    js["MaxKeyLen"] = trie->max_strlen();
    //js["SumKeyLen"] = trie->adfa_total_words_len();
    js["ZpathRatioN"] = (trie->num_zpath_states() + 0.01)/(trie->v_total_states() + 0.01);
    js["ZpathRatioL"] = (trie->total_zpath_len() + 0.01)/(trie->adfa_total_words_len() + 0.01);
    js["ZpathStates"] = trie->num_zpath_states();
    js["ZpathSumLen"] = trie->total_zpath_len();
    js["Version"] = header.version;
    //js["Type"] = header.dfa_class_name;
    return js;
  }

  void SaveMmap(std::function<void(const void *, size_t)> write) const final {
    m_trie->save_mmap(write);
  }
  size_t Find(fstring key) const final { return m_dawg->index(key); }
  size_t AccurateRank(fstring key) const final {
    MatchContext ctx;
    size_t index = -1;
    size_t dict_rank = -1;
    m_dawg->lower_bound(ctx, key, &index, &dict_rank);
    return dict_rank;
  }
  size_t ApproximateRank(fstring key) const override {
    return AccurateRank(key);
  }
  size_t TotalKeySize() const final { return m_trie->adfa_total_words_len(); }
  std::string NthKey(size_t nth) const final {
    TERARK_VERIFY_LT(nth, m_num_keys);
    return m_dawg->nth_word(nth);
  }
  fstring Memory()      const final { return m_trie->get_mmap(); }
  bool NeedsReorder()   const final { return true; }
  bool HasFastApproximateRank() const noexcept final { return false; }
};

template<class NLTrie>
class NestLoudsTrieIndex : public NestLoudsTrieIndexBase {
  typedef NestLoudsTrieIter<NLTrie> MyIterator;
public:
  explicit NestLoudsTrieIndex(NLTrie* trie) : NestLoudsTrieIndexBase(trie) {}
  Iterator* NewIterator() const final {
    auto trie = static_cast<const NLTrie*>(m_trie.get());
    return MyIterator::new_iter(trie);
  }
  void GetOrderMap(UintVecMin0& newToOld) const final {
    auto trie = static_cast<const NLTrie*>(m_trie.get());
    NestLoudsTrieGetOrderMap(trie, newToOld);
  }
  void BuildCache(double cacheRatio) final {
    if (cacheRatio > 1e-8) {
      auto trie = static_cast<NLTrie*>(m_trie.get());
      NestLoudsTrieBuildCache(trie, cacheRatio);
    }
  }

  class MyFactory : public MyBaseFactory {
  public:
  private:
    template<class StrVec>
    COIndex*
    BuildImpl(const ToplingZipTableOptions& tzopt, StrVec& keyVec,
              NestLoudsTrieConfig& conf) const {
      AssertSorted(keyVec);
      Set_NestLoudsTrieConfig(conf, keyVec, tzopt);
      std::unique_ptr<NLTrie> trie(new NLTrie());
      trie->build_from(keyVec, conf);
      return new NestLoudsTrieIndex(trie.release());
    }
    COIndex* VirtBuild(const ToplingZipTableOptions& tzopt,
                           FixedLenStrVec& keyVec,
                           NestLoudsTrieConfig& conf) const override {
      return BuildImpl(tzopt, keyVec, conf);
    }
    COIndex* VirtBuild(const ToplingZipTableOptions& tzopt,
                           SortedStrVec& keyVec,
                           NestLoudsTrieConfig& conf) const override {
      return BuildImpl(tzopt, keyVec, conf);
    }
  public:
    COIndexUP LoadMemory(fstring mem) const override {
      unique_ptr<BaseDFA> dfa(BaseDFA::load_mmap_user_mem(mem));
      auto trie = dynamic_cast<NLTrie*>(dfa.get());
      if (NULL == trie) {
        throw std::invalid_argument("Bad trie class: " + ClassName(*dfa)
            + ", should be " + ClassName<NLTrie>());
      }
      auto index = new NestLoudsTrieIndex(trie);
      dfa.release();
      return COIndexUP(index);
    }
    COIndexUP LoadFile(fstring fpath) const override {
      unique_ptr<BaseDFA> dfa(BaseDFA::load_mmap(fpath));
      auto trie = dynamic_cast<NLTrie*>(dfa.get());
      if (NULL == trie) {
        throw std::invalid_argument(
            "File: " + fpath + ", Bad trie class: " + ClassName(*dfa)
            + ", should be " + ClassName<NLTrie>());
      }
      auto index = new NestLoudsTrieIndex(trie);
      dfa.release();
      return COIndexUP(index);
    }
  };
};

typedef NestLoudsTrieDAWG_IL_256 NestLoudsTrieDAWG_IL_256_32;
typedef NestLoudsTrieDAWG_SE_512 NestLoudsTrieDAWG_SE_512_32;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_IL_256_32> TrieDAWG_IL_256_32;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_SE_512_64> TrieDAWG_SE_512_64;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_IL_256> TrieDAWG_Mixed_IL_256;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_SE_512> TrieDAWG_Mixed_SE_512;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_XL_256> TrieDAWG_Mixed_XL_256;

typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_IL_256_32_FL> TrieDAWG_IL_256_32_FL;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_SE_512_64_FL> TrieDAWG_SE_512_64_FL;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_IL_256_32_FL> TrieDAWG_Mixed_IL_256_32_FL;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_SE_512_32_FL> TrieDAWG_Mixed_SE_512_32_FL;
typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_XL_256_32_FL> TrieDAWG_Mixed_XL_256_32_FL;

#define COIndexRegisterNLT(clazzSuffix, ...)                        \
  COIndexRegisterImp(TrieDAWG_##clazzSuffix,                        \
    BOOST_STRINGIZE(BOOST_PP_CAT(NestLoudsTrieDAWG_, clazzSuffix)), \
    BOOST_STRINGIZE(clazzSuffix), \
    ##__VA_ARGS__)

COIndexRegisterImp(TrieDAWG_IL_256_32, "NestLoudsTrieDAWG_IL", "IL_256_32", "IL_256", "NestLoudsTrieDAWG_IL_256");
COIndexRegisterNLT(SE_512_64);
COIndexRegisterNLT(Mixed_SE_512);
COIndexRegisterNLT(Mixed_IL_256);
COIndexRegisterNLT(Mixed_XL_256);

COIndexRegisterNLT(IL_256_32_FL);
COIndexRegisterNLT(SE_512_64_FL);
COIndexRegisterNLT(Mixed_SE_512_32_FL);
COIndexRegisterNLT(Mixed_IL_256_32_FL);
COIndexRegisterNLT(Mixed_XL_256_32_FL);

} // namespace rocksdb
