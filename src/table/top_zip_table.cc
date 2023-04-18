/*
 * topling_zip_table.cc
 *
 *  Created on: 2016-08-09
 *      Author: leipeng
 */

// project headers
#include "top_zip_table.h"
#include "co_index.h"
#include <table/top_table_common.h>
#include "top_zip_internal.h"

// std headers
#include <future>
//#include <random>
#include <cstdlib>
#include <cstdint>
//#include <fstream>
//#include <memory/arena.h> // for #include <sys/mman.h>
#ifdef _MSC_VER
# include <io.h>
#else
//# include <sys/types.h>
//# include <sys/mman.h>
#endif

// boost headers
#include <boost/predef/other/endian.h>

// rocksdb headers
#include <logging/logging.h>
#include <table/meta_blocks.h>
#include <rocksdb/convenience.h>

// terark headers
#include <terark/lcast.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/DataIO.hpp>

// 3rd-party headers

static std::once_flag PrintVersionHashInfoFlag;

#ifndef _MSC_VER
const char* git_version_hash_info_core();
const char* git_version_hash_info_fsa();
const char* git_version_hash_info_zbs();
const char* git_version_hash_info_topling_zip_table_reader();
__attribute__((weak))
const char* git_version_hash_info_topling_rocks();
#endif

namespace rocksdb {

void PrintVersionHashInfo(rocksdb::Logger* info_log) {
  std::call_once(PrintVersionHashInfoFlag, [info_log] {
# ifndef _MSC_VER
    INFO(info_log, "core %s", git_version_hash_info_core());
    INFO(info_log, "fsa %s", git_version_hash_info_fsa());
    INFO(info_log, "zbs %s", git_version_hash_info_zbs());
    INFO(info_log, "topling-zip_table_reader: %s", git_version_hash_info_topling_zip_table_reader());
    if (git_version_hash_info_topling_rocks)
      INFO(info_log, "topling-rocks %s", git_version_hash_info_topling_rocks());
# endif
  });
}
void PrintVersionHashInfo(const std::shared_ptr<rocksdb::Logger>& log) {
  PrintVersionHashInfo(log.get());
}

const std::string kToplingZipTableValueDictBlock    = "TerarkZipTableValueDictBlock";
const std::string kToplingZipTableOffsetBlock       = "TerarkZipTableOffsetBlock";
const std::string kZipTableKeyRankCacheBlock       = "ZipTableKeyRankCacheBlock";

const std::string kToplingZipTableDictInfo = "terark.build.dict_info";
const std::string kToplingZipTableDictSize = "terark.build.dict_size";

// same as kTopEmptyTableMagicNumber
const uint64_t kToplingZipTableMagicNumber = 0x1122334455667788;

std::shared_ptr<class TableFactory>
NewToplingZipTableFactory(const ToplingZipTableOptions& tzto) {
  using namespace std;
  auto factory = make_shared<ToplingZipTableFactory>(tzto);
  if (tzto.debugLevel >= 4) {
    STD_INFO("NewToplingZipTableFactory(\n%s)\n",
      factory->GetPrintableOptions().c_str()
    );
  }
  return factory;
}

std::shared_ptr<TableFactory>
SingleToplingZipTableFactory(const ToplingZipTableOptions& tzto) {
  static std::shared_ptr<TableFactory> factory(NewToplingZipTableFactory(tzto));
  return factory;
}

// be KISS, do not play petty trick
#if 0
size_t GetFixedPrefixLen(const SliceTransform* tr) {
  if (tr == nullptr) {
    return 0;
  }
  fstring trName = tr->Name();
  fstring namePrefix = "rocksdb.FixedPrefix.";
  if (!trName.startsWith(namePrefix)) {
    return 0;
  }
  return terark::lcast(trName.substr(namePrefix.size()));
}
#endif

ToplingZipTableFactory::ToplingZipTableFactory(
    const ToplingZipTableOptions& tzto)
: table_options_(tzto) {
    if (tzto.minPreadLen >= 0 && tzto.cacheCapacityBytes) {
        bool aio = false;
        size_t maxFiles = -1;
        cache_.reset(LruReadonlyCache::create(
            tzto.cacheCapacityBytes, tzto.cacheShards, maxFiles, aio));
    }
    size_t fast_num = 1;
    if (tzto.keyPrefixLen > 1) {
        fast_num = 32*1024;
    }
    else if (1 == tzto.keyPrefixLen) {
        fast_num = 256;
    }
    extern bool IsCompactionWorker();
    level_zip_size_.resize_fill(fast_num);
    is_compaction_worker_ = IsCompactionWorker();
    if (is_compaction_worker_)
        level_zip_size_inc_.resize_fill(fast_num);
}

ToplingZipTableFactory::~ToplingZipTableFactory() {
}

// defined in topling_zip_table_builder.cc
__attribute__((weak))
extern
TableBuilder*
createToplingZipTableBuilder(const ToplingZipTableFactory*,
                             const TableBuilderOptions&,
                             WritableFileWriter*);
namespace tzb_detail {
extern long long g_startupTime;
}
using namespace tzb_detail;

TableBuilder*
ToplingZipTableFactory::NewTableBuilder(
        const TableBuilderOptions& tbo,
        WritableFileWriter* file)
const {
  if (!createToplingZipTableBuilder) {
    // if ToplingZipTableBuilder is not compiled in, weak symbol
    // createToplingZipTableBuilder is null, we just return null to notify
    // caller(DispatcherTable) fallback to default table builder.
    return nullptr;
  }
  if (!IsCompactionWorker()) {
    if (tbo.level_at_creation < table_options_.builderMinLevel) {
      // on DB side, if distributed compaction failed, the compaction can
      // fallback to local compaction, but local CPU is expensive, so we should
      // use fast tables such as SingleFastTable. We still hope to use this zip
      // table on lower levels(such as bottom level). return null to notifify
      // caller(DispatcherTable) fallback to default table builder.
      return nullptr;
    }
  }
  PrintVersionHashInfo(tbo.ioptions.info_log);
  auto userCmp = tbo.internal_comparator.user_comparator();
  TERARK_VERIFY_F(IsBytewiseComparator(userCmp), "%s", userCmp->Name());
  TERARK_VERIFY_EZ(userCmp->timestamp_size());
  TERARK_VERIFY_EQ(userCmp, tbo.ioptions.user_comparator);
  int curlevel = tbo.level_at_creation;
  int numlevel = tbo.ioptions.num_levels;
  if (table_options_.debugLevel >= 1) {
    INFO(tbo.ioptions.info_log
      , "num_new_table = %zd, curlevel = %d, numlevel = %d\n"
      , num_new_table, curlevel, numlevel
    );
  }
  if (0 == num_new_table) {
    if (!IsCompactionWorker()) {
      g_startupTime = g_pf.now();
    }
  }
  num_new_table++;

  return createToplingZipTableBuilder(this, tbo, file);
}

Status
ToplingZipTableFactory::ValidateOptions(const DBOptions& db_opts,
                                        const ColumnFamilyOptions& cf_opts)
const {
  if (!IsBytewiseComparator(cf_opts.comparator)) {
    return Status::InvalidArgument(ROCKSDB_FUNC, "comparator is not bytewise");
  }
  TERARK_VERIFY_EZ(cf_opts.comparator->timestamp_size());
  auto& tzto = this->table_options_;
  try {
    TempFileDeleteOnClose test;
    test.path = tzto.localTempDir + "/Topling-XXXXXX";
    test.open_temp();
    test.writer << "Some Data";
    test.complete_write();
  }
  catch (...) {
    std::string msg = "ERROR: bad localTempDir : " + tzto.localTempDir;
    fprintf(stderr , "%s\n" , msg.c_str());
    return Status::InvalidArgument("ToplingZipTableFactory::ValidateOptions()", msg);
  }
  if (!IsBytewiseComparator(cf_opts.comparator)) {
    return Status::InvalidArgument("ToplingZipTableFactory::ValidateOptions()",
      "user comparator must be 'leveldb.BytewiseComparator'");
  }
  auto indexFactory = COIndex::GetFactory(table_options_.indexType);
  if (!indexFactory) {
    std::string msg = "invalid indexType: " + table_options_.indexType;
    return Status::InvalidArgument(msg);
  }
  fstring wireName = indexFactory->WireName();
  if (!wireName.startsWith("NestLoudsTrieDAWG")) {
    std::string msg = "indexType is not a NestLoudsTrieDAWG: "
                      "WireName = " + wireName + " , ConfName = "
                    + table_options_.indexType;
    return Status::InvalidArgument(msg);
  }
  return Status::OK();
}

bool ToplingZipTablePrintCacheStat(TableFactory* factory, FILE* fp) {
  auto tztf = dynamic_cast<const ToplingZipTableFactory*>(factory);
  if (tztf) {
    if (tztf->cache()) {
      tztf->cache()->print_stat_cnt(fp);
      return true;
    } else {
      fprintf(fp, "PrintCacheStat: terark user cache == nullptr\n");
    }
  } else {
    fprintf(fp, "PrintCacheStat: factory is not ToplingZipTableFactory\n");
  }
  return false;
}

} // namespace rocksdb
