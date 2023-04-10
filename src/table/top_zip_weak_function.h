/*
 * topling_zip_weak_function.h
 *
 *  Created on: 2017-04-01
 *      Author: leipeng
 */

#pragma once

#include "top_zip_table.h"

namespace rocksdb {

#if !defined(_MSC_VER)

void
__attribute__((weak))
ToplingZipAutoConfigForBulkLoad(struct ToplingZipTableOptions&,
                               struct DBOptions&,
                               struct ColumnFamilyOptions&,
                               size_t cpuNum,
                               size_t memBytesLimit,
                               size_t diskBytesLimit);

void
__attribute__((weak))
ToplingZipAutoConfigForOnlineDB(struct ToplingZipTableOptions&,
                               struct DBOptions&,
                               struct ColumnFamilyOptions&,
                               size_t cpuNum,
                               size_t memBytesLimit,
                               size_t diskBytesLimit);

void
__attribute__((weak))
ToplingZipAutoConfigForOnlineDB_DBOptions(struct DBOptions& dbo, size_t cpuNum);

void
__attribute__((weak))
ToplingZipAutoConfigForOnlineDB_CFOptions(struct ToplingZipTableOptions& tzo,
                                    struct ColumnFamilyOptions& cfo,
                                    size_t memBytesLimit,
                                    size_t diskBytesLimit);

const class WriteBatchEntryIndexFactory*
__attribute__((weak))
WriteBatchEntryPTrieIndexFactory(const WriteBatchEntryIndexFactory* fallback);

std::shared_ptr<class TableFactory>
__attribute__((weak))
NewToplingZipTableFactory(const ToplingZipTableOptions&);

std::shared_ptr<class TableFactory>
__attribute__((weak))
SingleToplingZipTableFactory(const ToplingZipTableOptions&);


bool
__attribute__((weak))
ToplingZipConfigFromEnv(struct DBOptions&,
                       struct ColumnFamilyOptions&);

bool __attribute__((weak)) ToplingZipCFOptionsFromEnv(struct ColumnFamilyOptions&);
void __attribute__((weak)) ToplingZipDBOptionsFromEnv(struct DBOptions&);

bool __attribute__((weak)) ToplingZipIsBlackListCF(const std::string& cfname);

void __attribute__((weak))
ToplingZipMultiCFOptionsFromEnv(const struct DBOptions& db_options,
      const std::vector<struct ColumnFamilyDescriptor>& cfvec);

#endif // _MSC_VER

} // namespace rocksdb
