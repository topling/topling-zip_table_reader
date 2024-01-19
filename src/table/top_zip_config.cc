#include "top_zip_table.h"
#include <table/top_table_common.h>
#include <terark/hash_strmap.hpp>
#include <terark/util/throw.hpp>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <options/options_helper.h>
#include <rocksdb/convenience.h> // for GetMemTableRepFactoryFromString
#include <rocksdb/memtablerep.h>
#include <rocksdb/table.h>
#ifdef _MSC_VER
# include <Windows.h>
# define strcasecmp _stricmp
# define strncasecmp _strnicmp
# undef DeleteFile
#else
# include <unistd.h>
#endif
#include <mutex>

namespace terark {
  TERARK_DLL_EXPORT void DictZipBlobStore_setZipThreads(int zipThreads);
}

namespace rocksdb {

void ToplingZipDeleteTempFiles(const std::string& tmpPath) {
  Env* env = Env::Default();
  std::vector<std::string> files;
  env->GetChildren(tmpPath, &files);
  std::string fpath;
  for (const std::string& f : files) {
    if (false
        || fstring(f).startsWith("Topling-")
        || fstring(f).startsWith("q1-")
        || fstring(f).startsWith("q2-")
        || fstring(f).startsWith("linkSeqVec-")
        || fstring(f).startsWith("linkVec-")
        || fstring(f).startsWith("label-")
        || fstring(f).startsWith("nextStrVec-")
        || fstring(f).startsWith("nestStrVec-")
        || fstring(f).startsWith("nestStrPool-")
        ) {
      fpath.resize(0);
      fpath.append(tmpPath);
      fpath.push_back('/');
      fpath.append(f);
      env->DeleteFile(fpath);
    }
  }
}

void ToplingZipAutoConfigForBulkLoad(struct ToplingZipTableOptions& tzo,
                                    struct DBOptions& dbo,
                                    struct ColumnFamilyOptions& cfo,
                                    size_t cpuNum,
                                    size_t memBytesLimit,
                                    size_t diskBytesLimit)
{
  using namespace std; // max, min
  int iCpuNum = int(cpuNum);
  if (cpuNum > 0) {
    terark::DictZipBlobStore_setZipThreads(max(iCpuNum-1, 0));
  }
  if (0 == memBytesLimit) {
#ifdef _MSC_VER
    MEMORYSTATUSEX statex;
    statex.dwLength = sizeof(statex);
    GlobalMemoryStatusEx(&statex);
    memBytesLimit = statex.ullTotalPhys;
#else
    size_t page_num  = sysconf(_SC_PHYS_PAGES);
    size_t page_size = sysconf(_SC_PAGE_SIZE);
    memBytesLimit = page_num * page_size;
#endif
  }
  tzo.softZipWorkingMemLimit = memBytesLimit * 7 / 8;
  tzo.hardZipWorkingMemLimit = tzo.softZipWorkingMemLimit;
  tzo.smallTaskMemory = memBytesLimit / 16;
  tzo.indexNestLevel = 3;

  cfo.table_factory = SingleToplingZipTableFactory(tzo);
  cfo.write_buffer_size = tzo.smallTaskMemory;
  cfo.num_levels = 7;
  cfo.max_write_buffer_number = 6;
  cfo.min_write_buffer_number_to_merge = 1;
  cfo.target_file_size_base = cfo.write_buffer_size;
  cfo.target_file_size_multiplier = 2;
  cfo.compaction_style = rocksdb::kCompactionStyleUniversal;
  cfo.compaction_options_universal.allow_trivial_move = true;

  cfo.max_compaction_bytes = (static_cast<uint64_t>(1) << 60);
  cfo.disable_auto_compactions = true;
  cfo.level0_file_num_compaction_trigger = (1<<30);
  cfo.level0_slowdown_writes_trigger = (1<<30);
  cfo.level0_stop_writes_trigger = (1<<30);
  cfo.soft_pending_compaction_bytes_limit = 0;
  cfo.hard_pending_compaction_bytes_limit = 0;

  dbo.create_if_missing = true;
  dbo.allow_concurrent_memtable_write = false;
  //dbo.allow_mmap_populate = false;
  dbo.max_background_flushes = 2;
  dbo.max_subcompactions = 1; // no sub compactions
#if ROCKSDB_MAJOR < 7
  dbo.new_table_reader_for_compaction_inputs = false;
#endif
  dbo.max_open_files = -1;

  dbo.env->SetBackgroundThreads(max(1,min(4,iCpuNum/2)), rocksdb::Env::HIGH);
}

void ToplingZipAutoConfigForOnlineDB(struct ToplingZipTableOptions& tzo,
                                    struct DBOptions& dbo,
                                    struct ColumnFamilyOptions& cfo,
                                    size_t cpuNum,
                                    size_t memBytesLimit,
                                    size_t diskBytesLimit)
{
  ToplingZipAutoConfigForOnlineDB_CFOptions(tzo, cfo, memBytesLimit, diskBytesLimit);
  ToplingZipAutoConfigForOnlineDB_DBOptions(dbo, cpuNum);
}

void
ToplingZipAutoConfigForOnlineDB_DBOptions(struct DBOptions& dbo, size_t cpuNum)
{
  using namespace std; // max, min
  int iCpuNum = int(cpuNum);
  if (cpuNum > 0) {
    terark::DictZipBlobStore_setZipThreads((iCpuNum * 3 + 1) / 5);
  }
  dbo.create_if_missing = true;
  dbo.max_background_flushes = 2;
  dbo.max_subcompactions = 1; // no sub compactions
#if ROCKSDB_MAJOR < 7
  dbo.base_background_compactions = 3;
#endif
  dbo.max_background_compactions = 5;
  dbo.allow_concurrent_memtable_write = false;

  dbo.env->SetBackgroundThreads(max(1,min(3,iCpuNum*3/8)), rocksdb::Env::LOW);
  dbo.env->SetBackgroundThreads(max(1,min(2,iCpuNum*2/8)), rocksdb::Env::HIGH);
}

void ToplingZipAutoConfigForOnlineDB_CFOptions(struct ToplingZipTableOptions& tzo,
                                    struct ColumnFamilyOptions& cfo,
                                    size_t memBytesLimit,
                                    size_t diskBytesLimit)
{
  using namespace std; // max, min
  if (0 == memBytesLimit) {
#ifdef _MSC_VER
    MEMORYSTATUSEX statex;
    statex.dwLength = sizeof(statex);
    GlobalMemoryStatusEx(&statex);
    memBytesLimit = statex.ullTotalPhys;
#else
    size_t page_num  = sysconf(_SC_PHYS_PAGES);
    size_t page_size = sysconf(_SC_PAGE_SIZE);
    memBytesLimit = page_num * page_size;
#endif
  }
  tzo.softZipWorkingMemLimit = memBytesLimit * 1 / 8;
  tzo.hardZipWorkingMemLimit = tzo.softZipWorkingMemLimit * 2;
  tzo.smallTaskMemory = memBytesLimit / 64;

  cfo.write_buffer_size = memBytesLimit / 32;
  cfo.num_levels = 7;
  cfo.max_write_buffer_number = 3;
  cfo.target_file_size_base = memBytesLimit / 8;
  cfo.target_file_size_multiplier = 1;
  cfo.compaction_style = rocksdb::kCompactionStyleUniversal;
  cfo.compaction_options_universal.allow_trivial_move = true;

  // intended: less than target_file_size_base
  cfo.max_bytes_for_level_base = cfo.write_buffer_size * 8;
  cfo.max_bytes_for_level_multiplier = 2;
}

bool ToplingZipConfigFromEnv(DBOptions& dbo, ColumnFamilyOptions& cfo) {
  if (ToplingZipCFOptionsFromEnv(cfo)) {
    ToplingZipDBOptionsFromEnv(dbo);
    return true;
  } else {
    return false;
  }
}

static void ToplingZipCFOptionsFromEnv_tzo(ColumnFamilyOptions& cfo,
                                          ToplingZipTableOptions& tzo);
bool ToplingZipCFOptionsFromEnv(ColumnFamilyOptions& cfo) {
  struct ToplingZipTableOptions tzo;
  ToplingZipAutoConfigForOnlineDB_CFOptions(tzo, cfo, 0, 0);
  if (!ToplingZipTableOptionsFromEnv(tzo)) {
    return false;
  }
  ToplingZipCFOptionsFromEnv_tzo(cfo, tzo);
  return true;
}

bool ToplingZipTableOptionsFromEnv(ToplingZipTableOptions& tzo) {
  const char* localTempDir = getenv("ToplingZipTable_localTempDir");
  if (localTempDir) {
    tzo.localTempDir = localTempDir;
  }
  if (const char* algo = getenv("ToplingZipTable_entropyAlgo")) {
    if (strcasecmp(algo, "NoEntropy") == 0) {
      tzo.entropyAlgo = tzo.kNoEntropy;
    } else if (strcasecmp(algo, "FSE") == 0) {
      tzo.entropyAlgo = tzo.kFSE;
    } else if (strcasecmp(algo, "huf") == 0) {
      tzo.entropyAlgo = tzo.kHuffman;
    } else if (strcasecmp(algo, "huff") == 0) {
      tzo.entropyAlgo = tzo.kHuffman;
    } else if (strcasecmp(algo, "huffman") == 0) {
      tzo.entropyAlgo = tzo.kHuffman;
    } else {
      tzo.entropyAlgo = tzo.kNoEntropy;
      STD_WARN(
        "bad env ToplingZipTable_entropyAlgo=%s, must be one of {NoEntropy, FSE, huf}, reset to default 'NoEntropy'\n"
        , algo);
    }
  }
  if (const char* env = getenv("ToplingZipTable_indexType")) {
    tzo.indexType = env;
  }

// semantic is override
#define MyGetInt(obj, name) \
    obj.name = (int)terark::getEnvLong("ToplingZipTable_" #name, obj.name)
#define MyGetBool(obj, name) \
    obj.name = terark::getEnvBool("ToplingZipTable_" #name, obj.name)
#define MyGetDouble(obj, name) \
    obj.name = terark::getEnvDouble("ToplingZipTable_" #name, obj.name)
#define MyGetXiB(obj, name) \
  if (const char* env = getenv("ToplingZipTable_" #name))\
    obj.name = terark::ParseSizeXiB(env)

  MyGetInt   (tzo, checksumLevel        );
  MyGetInt   (tzo, indexNestLevel       );
  MyGetInt   (tzo, debugLevel           );
  MyGetInt   (tzo, keyPrefixLen         );
  MyGetInt   (tzo, offsetArrayBlockUnits);
  MyGetInt   (tzo, indexNestScale       );
  MyGetBool  (tzo, forceLegacyZvType    );
  MyGetBool  (tzo, indexMemAsHugePage   );

  terark::minimize(tzo.keyPrefixLen, tzo.MAX_PREFIX_LEN);

  if (true
      &&   0 != tzo.offsetArrayBlockUnits
      &&  64 != tzo.offsetArrayBlockUnits
      && 128 != tzo.offsetArrayBlockUnits
  ) {
    STD_WARN(
      "ToplingZipConfigFromEnv: bad offsetArrayBlockUnits = %d, must be one of {0,64,128}, reset to 128\n"
      , tzo.offsetArrayBlockUnits
    );
    tzo.offsetArrayBlockUnits = 128;
  }
  if (tzo.indexNestScale == 0) {
    STD_WARN(
      "ToplingZipConfigFromEnv: bad indexNestScale = %d, must be in [1,255], reset to 8\n"
      , int(tzo.indexNestScale)
    );
    tzo.indexNestScale = 8;
  }

  if (const char* env = getenv("ToplingZipTable_warmupLevel")) {
    enum_value(env, &tzo.warmupLevel);
  }

  MyGetBool  (tzo, useSuffixArrayLocalMatch);
  MyGetBool  (tzo, compressGlobalDict      );

  MyGetDouble(tzo, estimateCompressionRatio);
  if (tzo.sampleRatio.size() == 1) {
    tzo.sampleRatio[0] = terark::getEnvDouble("ToplingZipTable_sampleRatio", tzo.sampleRatio[0]);
  } else {
    STD_WARN("existing sampleRatio.size() must be 1, but is %zd, ignored", tzo.sampleRatio.size());
  }
  MyGetDouble(tzo, indexCacheRatio         );

  MyGetInt(tzo, minPreadLen);

  MyGetXiB(tzo, softZipWorkingMemLimit);
  MyGetXiB(tzo, hardZipWorkingMemLimit);
  MyGetXiB(tzo, smallTaskMemory);
  MyGetInt(tzo, minDictZipValueSize);

  MyGetBool(tzo, speedupNestTrieBuild);
  MyGetInt (tzo, bytesPerBatch       );
  MyGetInt (tzo, recordsPerBatch     );
  //MyGetXiB (tzo, maxRawKeyValueBytes );

  MyGetBool(tzo, enableApproximateKeyAnchors);
  MyGetBool(tzo, enableCompositeUintIndex);
  MyGetBool(tzo, enableFixedLenHoleIndex);
  MyGetBool(tzo, enableUintIndex);
  MyGetBool(tzo, preadUseRocksdbFS);
  MyGetInt (tzo, needCompactMaxLevel);

  return true;
}


static void ToplingZipCFOptionsFromEnv_tzo(ColumnFamilyOptions& cfo,
                                          ToplingZipTableOptions& tzo) {
  if (const char* env = getenv("ToplingZipTable_memTable")) {
    // grammar: memTableName:key1=val1;key2=val2
    // example: patricia:virtual_alloc=1;concurrent_write=1
    std::unique_ptr<MemTableRepFactory> fac;
    Status s = GetMemTableRepFactoryFromString(env, &fac);
    if (s.ok() && fac)
      cfo.memtable_factory.reset(fac.release());
  }
  cfo.table_factory = SingleToplingZipTableFactory(tzo);
  const char* compaction_style = "Universal";
  if (const char* env = getenv("ToplingZipTable_compaction_style")) {
    compaction_style = env;
  }
  if (strcasecmp(compaction_style, "Level") == 0) {
    cfo.compaction_style = kCompactionStyleLevel;
  } else {
    if (strcasecmp(compaction_style, "Universal") != 0) {
      STD_WARN(
        "bad env ToplingZipTable_compaction_style=%s, use default 'universal'",
        compaction_style);
    }
    cfo.compaction_style = kCompactionStyleUniversal;
    cfo.compaction_options_universal.allow_trivial_move = true;
#define MyGetUniversal_uint(name, Default) \
    cfo.compaction_options_universal.name = \
        (unsigned)terark::getEnvLong("ToplingZipTable_" #name, Default)
    MyGetUniversal_uint(min_merge_width, 3);
    MyGetUniversal_uint(max_merge_width, 7);
    cfo.compaction_options_universal.size_ratio = (unsigned)
        terark::getEnvLong("ToplingZipTable_universal_compaction_size_ratio",
                            cfo.compaction_options_universal.size_ratio);
    const char* env_stop_style =
        getenv("ToplingZipTable_universal_compaction_stop_style");
    if (env_stop_style) {
      auto& ou = cfo.compaction_options_universal;
      if (strncasecmp(env_stop_style, "Similar", 7) == 0) {
        ou.stop_style = kCompactionStopStyleSimilarSize;
      }
      else if (strncasecmp(env_stop_style, "Total", 5) == 0) {
        ou.stop_style = kCompactionStopStyleTotalSize;
      }
      else {
        STD_WARN(
          "bad env ToplingZipTable_universal_compaction_stop_style=%s, use rocksdb default 'TotalSize'",
          env_stop_style);
        ou.stop_style = kCompactionStopStyleTotalSize;
      }
    }
  }
  MyGetXiB(cfo, write_buffer_size);
  MyGetXiB(cfo, target_file_size_base);
  //MyGetBool(cfo, enable_lazy_compaction, true);
  MyGetInt(cfo, max_write_buffer_number);
  MyGetInt(cfo, target_file_size_multiplier);
  MyGetInt(cfo, num_levels                 );
  MyGetInt(cfo, level0_file_num_compaction_trigger);
  MyGetInt(cfo, level0_slowdown_writes_trigger);
  MyGetInt(cfo, level0_stop_writes_trigger);

  if (tzo.debugLevel) {
    STD_INFO("ToplingZipConfigFromEnv(dbo, cfo) successed\n");
  }
}

void ToplingZipDBOptionsFromEnv(DBOptions& dbo) {
  ToplingZipAutoConfigForOnlineDB_DBOptions(dbo, 0);

#if ROCKSDB_MAJOR < 7
  MyGetInt(dbo, base_background_compactions);
#endif
  MyGetInt(dbo,  max_background_compactions);
  MyGetInt(dbo,  max_background_flushes    );
  MyGetInt(dbo,  max_subcompactions        );
  //MyGetBool(dbo, allow_mmap_populate       , false);

  dbo.env->SetBackgroundThreads(dbo.max_background_compactions, rocksdb::Env::LOW);
  dbo.env->SetBackgroundThreads(dbo.max_background_flushes    , rocksdb::Env::HIGH);
#if ROCKSDB_MAJOR < 7
  dbo.new_table_reader_for_compaction_inputs = false;
#endif
  dbo.max_open_files = -1;
}

class ToplingBlackListCF : public terark::hash_strmap<> {
public:
  ToplingBlackListCF() {
    if (const char* env = getenv("ToplingZipTable_blackListColumnFamily")) {
      const char* end = env + strlen(env);
      for (auto  curr = env; curr < end; ) {
        auto next = std::find(curr, end, ',');
        this->insert_i(fstring(curr, next));
        curr = next + 1;
      }
    }
  }
};
static ToplingBlackListCF g_blacklistCF;

bool ToplingZipIsBlackListCF(const std::string& cfname) {
  return g_blacklistCF.exists(cfname);
}

template<class T>
T& auto_const_cast(const T& x) {
  return const_cast<T&>(x);
}

void
ToplingZipMultiCFOptionsFromEnv(const DBOptions& db_options,
      const std::vector<ColumnFamilyDescriptor>& cfvec) {
  size_t numIsBlackList = 0;
  for (auto& cf : auto_const_cast(cfvec)) {
    if (ToplingZipIsBlackListCF(cf.name)) {
      numIsBlackList++;
    } else {
      ToplingZipCFOptionsFromEnv(cf.options);
    }
  }
  if (numIsBlackList < cfvec.size()) {
    ToplingZipDBOptionsFromEnv(auto_const_cast(db_options));
  }
}

}
