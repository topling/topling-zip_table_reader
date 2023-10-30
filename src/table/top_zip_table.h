/*
 * topling_zip_table.h
 *
 *  Created on: 2016-08-09
 *      Author: leipeng
 */

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <stdio.h>

#include "rocksdb/enum_reflection.h"
#include <table/top_table_reader.h> // for WarmupLevel

namespace rocksdb {

struct ToplingZipTableOptions {
  // copy of DictZipBlobStore::Options::EntropyAlgo
  ROCKSDB_ENUM_PLAIN_INCLASS(EntropyAlgo, unsigned char,
    kNoEntropy,
    kHuffman,
    kFSE
  );
  int indexNestLevel = 3;

  /// 0 : check sum nothing
  /// 1 : check sum meta data and index, check on file load
  /// 2 : check sum all data, not check on file load, checksum is for
  ///     each record, this incurs 4 bytes overhead for each record
  /// 3 : check sum all data with one checksum value, not checksum each record,
  ///     if checksum doesn't match, load will fail
  int checksumLevel = 1;

  EntropyAlgo entropyAlgo = kNoEntropy;

  /// please always set to 0
  /// unless you know what it really does for
  /// 0 : no debug
  /// 1 : enable infomation output
  /// 2 : verify 2nd pass iter keys & values
  /// 3 : dump 1st & 2nd pass data to file
  signed char   debugLevel               = 0;
  uint16_t      minPrefetchPages         = 0;

  unsigned char indexNestScale           = 8;
  bool          forceLegacyZvType        = false;
  bool          useSuffixArrayLocalMatch = false;
  bool          enableStatistics         = true;
  WarmupLevel   warmupLevel              = WarmupLevel::kIndex;
  int           builderMinLevel          = 0;
  bool          optimizeCpuL3Cache       = true;

  /// -1: dont use temp file for  any  index build
  ///  0: only use temp file for large index build, smart
  ///  1: only use temp file for large index build, same as NLT.tmpLevel
  ///  2: only use temp file for large index build, same as NLT.tmpLevel
  ///  3: only use temp file for large index build, same as NLT.tmpLevel
  ///  4: only use temp file for large index build, same as NLT.tmpLevel
  ///  5:      use temp file for  all  index build
  signed char    indexTempLevel           = 0;

  unsigned short offsetArrayBlockUnits    = 0;

  float          acceptCompressionRatio   = 0.8f;
  float          nltAcceptCompressionRatio = 0.4f;
  float          estimateCompressionRatio = 0.2f;
  float          keyRankCacheRatio        = 0.0f;
  double         sampleRatio              = 0.03;
  double         sampleRatioMultiplier    = 1.0;
  std::string    localTempDir             = "/tmp";
  std::string    indexType                = "Mixed_XL_256_32_FL";

  size_t softZipWorkingMemLimit = 16ull << 30;
  size_t hardZipWorkingMemLimit = 32ull << 30;
  size_t smallTaskMemory = 1200 << 20; // 1.2G
  // use dictZip for value when average value length >= minDictZipValueSize
  // otherwise do not use dictZip
  size_t minDictZipValueSize = 15;
  size_t keyPrefixLen = 0; // for IndexID

  // should be a small value, typically 0.001
  // default is to disable indexCache, because the improvement
  // is about only 10% when set to 0.001
  double indexCacheRatio = 0;//0.001;

  ///  < 0: do not use pread
  /// == 0: always use pread
  ///  > 0: use pread if BlobStore avg record len > minPreadLen
  int    minPreadLen         = 0;
  bool   compressGlobalDict = false;
  bool   indexMemAsHugePage = 0;
  bool   speedupNestTrieBuild = true;
  int    bytesPerBatch = 256*1024;
  int    recordsPerBatch = 500;
  int    fileWriterBufferSize = 128*1024;
  size_t fixedLenIndexCacheLeafSize = 512;
  //size_t maxRawKeyValueBytes = 0; // 0 indicate 5*target_file_size

  bool   enableApproximateKeyAnchors = true;
  bool   preadUseRocksdbFS = false;
  int    needCompactMaxLevel = -2; // disable by default

  static constexpr size_t MAX_PREFIX_LEN = 16;
};

void ToplingZipDeleteTempFiles(const std::string& tmpPath);

/// @memBytesLimit total memory can be used for the whole process
///   memBytesLimit == 0 indicate all physical memory can be used
void ToplingZipAutoConfigForBulkLoad(struct ToplingZipTableOptions&,
                         struct DBOptions&,
                         struct ColumnFamilyOptions&,
                         size_t cpuNum = 0,
                         size_t memBytesLimit = 0,
                         size_t diskBytesLimit = 0);
void ToplingZipAutoConfigForOnlineDB(struct ToplingZipTableOptions&,
                         struct DBOptions&,
                         struct ColumnFamilyOptions&,
                         size_t cpuNum = 0,
                         size_t memBytesLimit = 0,
                         size_t diskBytesLimit = 0);
void
ToplingZipAutoConfigForOnlineDB_DBOptions(struct DBOptions& dbo, size_t cpuNum = 0);

void
ToplingZipAutoConfigForOnlineDB_CFOptions(struct ToplingZipTableOptions& tzo,
                                    struct ColumnFamilyOptions& cfo,
                                    size_t memBytesLimit = 0,
                                    size_t diskBytesLimit = 0);

bool ToplingZipTableOptionsFromEnv(ToplingZipTableOptions& tzo);
bool ToplingZipConfigFromEnv(struct DBOptions&, struct ColumnFamilyOptions&);
bool ToplingZipCFOptionsFromEnv(struct ColumnFamilyOptions&);
void ToplingZipDBOptionsFromEnv(struct DBOptions&);
bool ToplingZipIsBlackListCF(const std::string& cfname);

/// will check column family black list in env
///@param db_options is const but will be modified in this function
///@param cfvec      is const but will be modified in this function
void
ToplingZipMultiCFOptionsFromEnv(const struct DBOptions& db_options,
      const std::vector<struct ColumnFamilyDescriptor>& cfvec);

std::shared_ptr<class TableFactory>
NewToplingZipTableFactory(const ToplingZipTableOptions&);

std::shared_ptr<class TableFactory>
SingleToplingZipTableFactory(const ToplingZipTableOptions&);

}  // namespace rocksdb
