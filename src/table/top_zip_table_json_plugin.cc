//
// Created by leipeng on 2020/8/5.
//

#include <topling/side_plugin_factory.h>
#include <topling/builtin_table_factory.h>
#include <table/top_table_common.h>
#include "top_zip_table.h"
#include "top_zip_internal.h" // for kToplingZipTableMagicNumber
#include <table/top_table_reader.h>
#include <terark/io/FileStream.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/num_to_str.hpp>
#include <terark/util/process.hpp>

#ifndef _MSC_VER
const char* git_version_hash_info_topling_zip_table_reader();
__attribute__((weak))
const char* git_version_hash_info_topling_rocks();
#endif

namespace terark {
TERARK_DLL_EXPORT int DictZipBlobStore_getZipThreads();
}

namespace rocksdb {

struct ToplingZipTableOptions_Json : ToplingZipTableOptions {
  ToplingZipTableOptions_Json(const json& js, const SidePluginRepo& repo) {
    Update(js);
  }
  void Update(const json& js) {
    ROCKSDB_JSON_OPT_PROP(js, indexNestLevel);
    ROCKSDB_JSON_OPT_PROP(js, checksumLevel);
    ROCKSDB_JSON_OPT_ENUM(js, entropyAlgo);
    ROCKSDB_JSON_OPT_PROP(js, debugLevel);
    ROCKSDB_JSON_OPT_PROP(js, minPrefetchPages);
    ROCKSDB_JSON_OPT_PROP(js, indexNestScale);
    ROCKSDB_JSON_OPT_PROP(js, forceLegacyZvType);
    ROCKSDB_JSON_OPT_PROP(js, useSuffixArrayLocalMatch);
    ROCKSDB_JSON_OPT_PROP(js, enableStatistics);
    ROCKSDB_JSON_OPT_ENUM(js, warmupLevel);
    ROCKSDB_JSON_OPT_PROP(js, builderMinLevel);
    ROCKSDB_JSON_OPT_PROP(js, optimizeCpuL3Cache);
    ROCKSDB_JSON_OPT_PROP(js, indexTempLevel);
    ROCKSDB_JSON_OPT_PROP(js, offsetArrayBlockUnits);
    ROCKSDB_JSON_OPT_PROP(js, acceptCompressionRatio);
    ROCKSDB_JSON_OPT_PROP(js, nltAcceptCompressionRatio);
    ROCKSDB_JSON_OPT_PROP(js, estimateCompressionRatio);
    ROCKSDB_JSON_OPT_PROP(js, keyRankCacheRatio);
    ROCKSDB_JSON_OPT_PROP(js, sampleRatio);
    ROCKSDB_JSON_OPT_PROP(js, localTempDir);
    ROCKSDB_JSON_OPT_PROP(js, indexType);
    ROCKSDB_JSON_OPT_SIZE(js, softZipWorkingMemLimit);
    ROCKSDB_JSON_OPT_SIZE(js, hardZipWorkingMemLimit);
    ROCKSDB_JSON_OPT_SIZE(js, smallTaskMemory);
    ROCKSDB_JSON_OPT_SIZE(js, minDictZipValueSize);
    ROCKSDB_JSON_OPT_PROP(js, keyPrefixLen);
    terark::minimize(keyPrefixLen, MAX_PREFIX_LEN);
    ROCKSDB_JSON_OPT_PROP(js, indexCacheRatio);
    ROCKSDB_JSON_OPT_PROP(js, minPreadLen);
    ROCKSDB_JSON_OPT_PROP(js, cacheShards);
    ROCKSDB_JSON_OPT_SIZE(js, cacheCapacityBytes);
    ROCKSDB_JSON_OPT_PROP(js, compressGlobalDict);
    ROCKSDB_JSON_OPT_PROP(js, indexMemAsResident);
    ROCKSDB_JSON_OPT_PROP(js, indexMemAsHugePage);
    ROCKSDB_JSON_OPT_PROP(js, bytesPerBatch);
    ROCKSDB_JSON_OPT_PROP(js, recordsPerBatch);
    ROCKSDB_JSON_OPT_PROP(js, speedupNestTrieBuild);
    ROCKSDB_JSON_OPT_SIZE(js, fileWriterBufferSize);
    //ROCKSDB_JSON_OPT_SIZE(js, maxRawKeyValueBytes);
    fileWriterBufferSize = (int)terark::__hsm_align_pow2(fileWriterBufferSize);
    ROCKSDB_JSON_OPT_SIZE(js, fixedLenIndexCacheLeafSize);
    ROCKSDB_JSON_OPT_PROP(js, enableApproximateKeyAnchors);
  }
  json ToJson(const json& dump_options) const {
    json djs;
    ROCKSDB_JSON_SET_PROP(djs, indexNestLevel);
    ROCKSDB_JSON_SET_PROP(djs, checksumLevel);
    ROCKSDB_JSON_SET_ENUM(djs, entropyAlgo);
    ROCKSDB_JSON_SET_PROP(djs, debugLevel);
    ROCKSDB_JSON_SET_PROP(djs, minPrefetchPages);
    ROCKSDB_JSON_SET_PROP(djs, indexNestScale);
    ROCKSDB_JSON_SET_PROP(djs, forceLegacyZvType);
    ROCKSDB_JSON_SET_PROP(djs, useSuffixArrayLocalMatch);
    ROCKSDB_JSON_SET_PROP(djs, enableStatistics);
    ROCKSDB_JSON_SET_ENUM(djs, warmupLevel);
    ROCKSDB_JSON_SET_PROP(djs, builderMinLevel);
    ROCKSDB_JSON_SET_PROP(djs, optimizeCpuL3Cache);
    ROCKSDB_JSON_SET_PROP(djs, indexTempLevel);
    ROCKSDB_JSON_SET_PROP(djs, offsetArrayBlockUnits);
    ROCKSDB_JSON_SET_PROP(djs, acceptCompressionRatio);
    ROCKSDB_JSON_SET_PROP(djs, nltAcceptCompressionRatio);
    ROCKSDB_JSON_SET_PROP(djs, estimateCompressionRatio);
    ROCKSDB_JSON_SET_PROP(djs, keyRankCacheRatio);
    ROCKSDB_JSON_SET_PROP(djs, sampleRatio);
    ROCKSDB_JSON_SET_PROP(djs, localTempDir);
    ROCKSDB_JSON_SET_PROP(djs, indexType);
    ROCKSDB_JSON_SET_SIZE(djs, softZipWorkingMemLimit);
    ROCKSDB_JSON_SET_SIZE(djs, hardZipWorkingMemLimit);
    ROCKSDB_JSON_SET_SIZE(djs, smallTaskMemory);
    ROCKSDB_JSON_SET_SIZE(djs, minDictZipValueSize);
    ROCKSDB_JSON_SET_PROP(djs, keyPrefixLen);
    ROCKSDB_JSON_SET_PROP(djs, indexCacheRatio);
    ROCKSDB_JSON_SET_PROP(djs, minPreadLen);
    ROCKSDB_JSON_SET_PROP(djs, cacheShards);
    ROCKSDB_JSON_SET_SIZE(djs, cacheCapacityBytes);
    ROCKSDB_JSON_SET_PROP(djs, compressGlobalDict);
    ROCKSDB_JSON_SET_PROP(djs, indexMemAsResident);
    ROCKSDB_JSON_SET_PROP(djs, indexMemAsHugePage);
    ROCKSDB_JSON_SET_PROP(djs, bytesPerBatch);
    ROCKSDB_JSON_SET_PROP(djs, recordsPerBatch);
    ROCKSDB_JSON_SET_PROP(djs, speedupNestTrieBuild);
    ROCKSDB_JSON_SET_SIZE(djs, fileWriterBufferSize);
    //ROCKSDB_JSON_SET_SIZE(djs, maxRawKeyValueBytes);
    ROCKSDB_JSON_SET_SIZE(djs, fixedLenIndexCacheLeafSize);
    ROCKSDB_JSON_SET_PROP(djs, enableApproximateKeyAnchors);
    return djs;
  }
};

void ToplingZipTableOptions_parse(ToplingZipTableOptions& tzto, const json& js) {
  static_cast<ToplingZipTableOptions_Json&>(tzto).Update(js);
}
json ToplingZipTableOptions_toJson(const ToplingZipTableOptions& tzto) {
  return static_cast<const ToplingZipTableOptions_Json&>(tzto).ToJson(json{});
}

std::string ToplingZipTableFactory::GetPrintableOptions() const {
  // NOLINTNEXTLINE
  auto& tzo = static_cast<const ToplingZipTableOptions_Json&>(table_options_);
  auto js = tzo.ToJson(json{});
  return js.dump(4);
}

namespace tzb_detail {
std::mutex g_sumMutex;
size_t g_sumKeyLen = 0;
size_t g_sumValueLen = 0;
size_t g_sumUserKeyLen = 0;
size_t g_sumUserKeyNum = 0;
size_t g_sumEntryNum = 0;
long long g_startupTime = 0;

__attribute((weak)) extern size_t sumWaitingMem;
__attribute((weak)) extern size_t sumWorkingMem;
__attribute((weak)) extern size_t& SyncWaitQueueSize();
}
using namespace tzb_detail;

static
std::shared_ptr<TableFactory>
JS_NewToplingZipTableFactory(const json& js, const SidePluginRepo& repo) {
  if (&SyncWaitQueueSize == nullptr) {
    STD_WARN("\nlibrocksdb.so is open source community version\n"
             "    ToplingZipTableReader is available, but\n"
             "    ToplingZipTableBuilder is not available\n"
             "    mailto:contact@topling.cn to get an enterprise version\n"
             );
  }
  ToplingZipTableOptions_Json options(js, repo);
  return NewToplingZipTableFactory(options);
}
ROCKSDB_FACTORY_REG("ToplingZipTable", JS_NewToplingZipTableFactory);
ROCKSDB_RegTableFactoryMagicNumber(kToplingZipTableMagicNumber, "ToplingZipTable");

typedef ToplingZipTableFactory::ZipSizeInfo ZipSizeInfo;
DATA_IO_DUMP_RAW_MEM_E(ZipSizeInfo)

// needs to be returned back to remote compaction's submitter
// return level_zip_size_inc_ to submitter
DATA_IO_LOAD_SAVE_E(ToplingZipTableFactoryState, & num_new_table
                    & internal_key_len
                    & internal_key_num
                    & user_key_num
                    & value_raw_size & value_zip_size
                    & index_raw_size & index_zip_size
                    & gdict_raw_size & gdict_zip_size
                    & sum_part_num & level_zip_size_inc_
                    )
void ToplingZipTableFactoryState::add(const ToplingZipTableFactoryState& y) {
  num_new_table += y.num_new_table;
  internal_key_len += y.internal_key_len;
  internal_key_num += y.internal_key_num;
  user_key_num += y.user_key_num;
  value_raw_size += y.value_raw_size; value_zip_size += y.value_zip_size;
  index_raw_size += y.index_raw_size; index_zip_size += y.index_zip_size;
  gdict_raw_size += y.gdict_raw_size; gdict_zip_size += y.gdict_zip_size;
  sum_part_num += y.sum_part_num;
  level_zip_size_.add(y.level_zip_size_inc_);
}

__attribute__((weak))
extern pid_t GetZipServerPID();

struct ToplingZipTableFactory_SerDe : SerDeFunc<TableFactory> {
  void Serialize(FILE* fp, const TableFactory& base) const override {
    auto& factory = dynamic_cast<const ToplingZipTableFactory&>(base);
    using namespace terark;
    LittleEndianDataOutput<NonOwnerFileStream> dio(fp);
    TERARK_VERIFY_EQ(factory.is_compaction_worker_, IsCompactionWorker());
    if (factory.is_compaction_worker_) {
      dio << static_cast<const ToplingZipTableFactoryState&>(factory);
    }
    else {
      if (&GetZipServerPID == nullptr) { // without ToplingZipTableBuilder
        if (0 == g_startupTime)
          g_startupTime = g_pf.now();
      }
      std::unique_lock<std::mutex> lock(factory.mtx_); // need lock
      dio << factory.level_zip_size_;
    }
  }

  void DeSerialize(FILE* fp, TableFactory* base) const override {
    auto object = dynamic_cast<ToplingZipTableFactory*>(base);
    TERARK_VERIFY(nullptr != object);
    using namespace terark;
    LittleEndianDataInput<NonOwnerFileStream> dio(fp);
    if (IsCompactionWorker()) {
      dio >> object->level_zip_size_; // don't need lock
      object->is_compaction_worker_ = true;
      // should likely overwrite these variables
      //   localTempDir
      //   softZipWorkingMemLimit
      //   hardZipWorkingMemLimit
      //   smallTaskMemory
      //   debugLevel
      ToplingZipTableOptionsFromEnv(object->table_options_);
    }
    else {
      ToplingZipTableFactoryState inc;
      dio >> inc;
      g_sumMutex.lock();
      g_sumKeyLen += inc.internal_key_len;
      g_sumValueLen += inc.value_raw_size;
      g_sumUserKeyLen += inc.index_raw_size;
      g_sumUserKeyNum += inc.user_key_num;
      g_sumEntryNum += inc.internal_key_num;
      g_sumMutex.unlock();
      object->mtx_.lock();
      object->add(inc);
      object->mtx_.unlock();
    }
  }
};
ROCKSDB_REG_PluginSerDe("ToplingZipTable", ToplingZipTableFactory_SerDe);

#define ToStr(...) json(std::string(buf, snprintf(buf, sizeof(buf), __VA_ARGS__)))

json JS_TopZipTable_Global_Stat(bool html) {
  char buf[64];
  long long t8 = g_pf.now();
  double td = g_pf.uf(g_startupTime, t8);
  size_t sumlen = g_sumKeyLen + g_sumValueLen;

  if (&GetZipServerPID == nullptr) {
    if (html)
      return json::object({
        { "sumKeyLen", ToStr("%.3f GB", g_sumKeyLen/1e9) },
        { "sumValueLen", ToStr("%.3f GB", g_sumValueLen/1e9) },
        { "sumUserKeyLen", ToStr("%.3f GB", g_sumUserKeyLen/1e9) },
        { "sumUserKeyNum", ToStr("%.6f M", g_sumUserKeyNum/1e6) },
        { "sumEntryNum", ToStr("%.6f M", g_sumEntryNum/1e6) },
        { "writeSpeed+seq", ToStr("%.3f MB/s", (sumlen) / td) },
        { "writeSpeed-seq", ToStr("%.3f MB/s", (sumlen - g_sumEntryNum * 8)/td) },
      });
    else
      return json::object({
        { "sumKeyLen", g_sumKeyLen },
        { "sumValueLen", g_sumValueLen },
        { "sumUserKeyLen", g_sumUserKeyLen },
        { "sumUserKeyNum", g_sumUserKeyNum },
        { "sumEntryNum", g_sumEntryNum },
      });
  }
  pid_t zip_server_pid = GetZipServerPID();
  auto& waitQueueSize = SyncWaitQueueSize();
  if (zip_server_pid > 0) {
    terark::process_obj_read(zip_server_pid,
        g_startupTime, g_sumKeyLen, g_sumValueLen,
        g_sumUserKeyLen, g_sumUserKeyNum, sumWorkingMem, sumWaitingMem,
        g_sumEntryNum, waitQueueSize);
  }
if (html)
  return json::object({
    { "sumWorkingMem", ToStr("%.3f GB", sumWorkingMem/1e9) },
    { "sumWaitingMem", ToStr("%.3f GB", sumWaitingMem/1e9) },
    { "waitQueueSize", waitQueueSize },
    { "sumKeyLen", ToStr("%.3f GB", g_sumKeyLen/1e9) },
    { "sumValueLen", ToStr("%.3f GB", g_sumValueLen/1e9) },
    { "sumUserKeyLen", ToStr("%.3f GB", g_sumUserKeyLen/1e9) },
    { "sumUserKeyNum", ToStr("%.6f M", g_sumUserKeyNum/1e6) },
    { "sumEntryNum", ToStr("%.6f M", g_sumEntryNum/1e6) },
    { "writeSpeed+seq", ToStr("%.3f MB/s", (sumlen) / td) },
    { "writeSpeed-seq", ToStr("%.3f MB/s", (sumlen - g_sumEntryNum * 8)/td) },
    { "zipServerPID", zip_server_pid },
  });
else
  return json::object({
    { "sumWorkingMem", sumWorkingMem },
    { "sumWaitingMem", sumWaitingMem },
    { "waitQueueSize", waitQueueSize },
    { "sumKeyLen", g_sumKeyLen },
    { "sumValueLen", g_sumValueLen },
    { "sumUserKeyLen", g_sumUserKeyLen },
    { "sumUserKeyNum", g_sumUserKeyNum },
    { "sumEntryNum", g_sumEntryNum },
    { "zipServerPID", zip_server_pid },
  });
}

json JS_TopZipTable_Global_Env() {
  extern int GetNltBuildThreads();
  return json::object({
    {"DictZipBlobStore_zipThreads", terark::DictZipBlobStore_getZipThreads()},
    {"ToplingZipTable_nltBuildThreads", GetNltBuildThreads()},
  });
}

static std::string HtmlGitHref(const char* git_repo, const char* git_hash) {
  std::string ztab = HtmlEscapeMin(strstr(git_hash, "commit ") + strlen("commit "));
  auto headstr = [](const std::string& s, auto pos) {
    return terark::fstring(s.data(), pos - s.begin());
  };
  auto tailstr = [](const std::string& s, auto pos) {
    return terark::fstring(&*pos, s.end() - pos);
  };
  auto ztab_sha_end = std::find_if(ztab.begin(), ztab.end(), &isspace);
  terark::string_appender<> oss;
  oss|"<pre>"
     |"<a href='https://github.com/"|git_repo|"/commit/"
     |headstr(ztab, ztab_sha_end)|"'>"
     |headstr(ztab, ztab_sha_end)|"</a>"
     |tailstr(ztab, ztab_sha_end)
     |"</pre>";
  return static_cast<std::string&&>(oss);
}
void JS_ZipTable_AddVersion(json& ver, bool html) {
  if (html) {
    ver["gdzip-reader"] = HtmlGitHref("topling/topling-zip_table_reader",
                         git_version_hash_info_topling_zip_table_reader());
    if (git_version_hash_info_topling_rocks) {
      ver["gdzip-builder"] = HtmlGitHref("rockeet/topling-rocks",
                            git_version_hash_info_topling_rocks());
    }
  } else {
    ver["gdzip-reader"] = git_version_hash_info_topling_zip_table_reader();
    if (git_version_hash_info_topling_rocks)
      ver["gdzip-builder"] = git_version_hash_info_topling_rocks();
  }
}

struct ToplingZipTableFactory_Json : ToplingZipTableFactory {
  void ToJson(const json& dump_options, json& djs) const {
    bool html = JsonSmartBool(dump_options, "html", true);
    if (html)
      ToJsonHtml(djs);
    else
      ToJsonJson(djs);
  }
  void ToJsonJson(json& djs) const {
    bool html = false;
    djs["num_new_table"] = num_new_table;
    djs["CompressInfo"] = json::object({
      { "gdict_raw_size", gdict_raw_size },
      { "index_raw_size", index_raw_size },
      { "index_zip_size", index_zip_size },
      { "value_raw_size", value_raw_size },
      { "value_zip_size", value_zip_size },
    });

    if (gdict_zip_size) {
      djs["CompressInfo"].merge_patch(json::object({
        { "gdict_zip_size", gdict_zip_size },
      }));
    }

    djs["Statistics"] = JS_TopZipTable_Global_Stat(html);

    auto pref_len = table_options_.keyPrefixLen;
    if (pref_len) mtx_.lock();
    auto lzs = level_zip_size_;
    if (pref_len) mtx_.unlock();

    if (pref_len) lzs.sort();
    json zsi_js;
    lzs.for_each([&](size_t prefix, const ZipSizeInfo& x) {
      char buf[64];
      if (x) {
        zsi_js.push_back(json::object({
            {"prefix", ToStr("%08zX", prefix) },
            {"raw_size", x.raw_size },
            {"zip_size", x.zip_size },
        }));
      }
    });
    if (!zsi_js.empty()) {
      djs["ZipSizeInfo"] = std::move(zsi_js);
    }
    JS_ZipTable_AddVersion(djs, html);
    JS_TopTable_AddVersion(djs, html);
    JS_ToplingDB_AddVersion(djs, html);
  }
  void ToJsonHtml(json& djs) const {
    bool html = true;
    char buf[64];
    djs["num_new_table"] = num_new_table;
    djs["CompressInfo"] = json::object({
      { "gdict_raw_size", ToStr("%.3f GB", gdict_raw_size / 1e9) },

      { "index_raw_size", ToStr("%.3f GB", index_raw_size / 1e9) },
      { "index_zip_size", ToStr("%.3f GB", index_zip_size / 1e9) },
      { "index:zip/raw" , ToStr("%.3f"   , index_zip_size / (index_raw_size + 0.1)) },
      { "index:raw/zip" , ToStr("%.3f"   , index_raw_size / (index_zip_size + 0.1)) },

      { "value_raw_size", ToStr("%.3f GB", value_raw_size / 1e9) },
      { "value_zip_size", ToStr("%.3f GB", value_zip_size / 1e9) },
      { "value:zip/raw" , ToStr("%.3f"   , value_zip_size / (value_raw_size + 0.1)) },
      { "value:raw/zip" , ToStr("%.3f"   , value_raw_size / (value_zip_size + 0.1)) },

      { ":raw:key/all", ToStr("%.3f", index_raw_size / (index_raw_size + value_raw_size + 0.1)) },
      { ":raw:val/all", ToStr("%.3f", value_raw_size / (index_raw_size + value_raw_size + 0.1)) },
      { ":zip:key/all", ToStr("%.3f", index_zip_size / (index_zip_size + value_zip_size + gdict_zip_size + 0.1)) },
      { ":zip:val/all", ToStr("%.3f", value_zip_size / (index_zip_size + value_zip_size + gdict_zip_size + 0.1)) },
    });

    if (gdict_zip_size) {
      djs["CompressInfo"].merge_patch(json::object({
        { "gdict_zip_size", ToStr("%.3f GB", gdict_zip_size / 1e9) },
        { "gdict:zip/raw" , ToStr("%.3f"   , gdict_zip_size / (gdict_raw_size + 0.1)) },
        { "gdict:raw/zip" , ToStr("%.3f"   , gdict_raw_size / (gdict_zip_size + 0.1)) },
      }));
    }

    djs["Statistics"] = JS_TopZipTable_Global_Stat(html);
    djs["Iterator"] = json{
      {"cumu_iter_num", cumu_iter_num},
      {"live_iter_num", live_iter_num},
      {"iter_seek_cnt", iter_seek_cnt},
      {"iter_next_cnt", iter_next_cnt},
      {"iter_prev_cnt", iter_prev_cnt},
      {"iter_key_len", iter_key_len},
      {"iter_val_len", iter_val_len},
    };

    auto pref_len = table_options_.keyPrefixLen;
    if (pref_len) mtx_.lock();
    auto lzs = level_zip_size_;
    if (pref_len) mtx_.unlock();

    if (pref_len) lzs.sort();
    json zsi_js;
    lzs.for_each([&](size_t prefix, const ZipSizeInfo& x) {
      if (x) {
        zsi_js.push_back(json::object({
            {"prefix", ToStr("%08zX", prefix)},
            {"raw_size", ToStr("%.3f GB", x.raw_size / 1e9)},
            {"zip_size", ToStr("%.3f GB", x.zip_size / 1e9)},
            {"raw/zip", ToStr("%.3f", 1.0 * x.raw_size / x.zip_size)},
        }));
      }
    });
    if (!zsi_js.empty()) {
      zsi_js[0]["<htmltab:col>"] = json::array({
          "prefix", "raw_size", "zip_size", "raw/zip"
      });
      djs["ZipSizeInfo"] = std::move(zsi_js);
    }
    JS_ZipTable_AddVersion(djs, html);
    JS_TopTable_AddVersion(djs, html);
    JS_ToplingDB_AddVersion(djs, html);
  }
};

struct ToplingZipTableFactory_Manip : PluginManipFunc<TableFactory> {
  void Update(TableFactory* p, const json&, const json& js,
              const SidePluginRepo& repo) const final {
    if (auto t = dynamic_cast<ToplingZipTableFactory*>(p)) {
      auto o = (ToplingZipTableOptions_Json*)(&t->table_options_);
      o->Update(js);
      return;
    }
    std::string name = p->Name();
    THROW_InvalidArgument("Is not ToplingZipTable, but is: " + name);
  }
  std::string ToString(const TableFactory& fac, const json& dump_options,
                       const SidePluginRepo& repo) const final {
    if (auto t = static_cast<const ToplingZipTableFactory_Json*>
               (dynamic_cast<const ToplingZipTableFactory*>(&fac))) {
      auto o = (const ToplingZipTableOptions_Json*)(&t->table_options_);
      bool metric = JsonSmartBool(dump_options, "metric", false);
      if (metric) {
        terark::string_appender<> oss;
        oss|"gdict_raw_size "  |t->gdict_raw_size  |"\n";
        oss|"index_raw_size "  |t->index_raw_size  |"\n";
        oss|"index_zip_size "  |t->index_zip_size  |"\n";
        oss|"value_raw_size "  |t->value_raw_size  |"\n";
        oss|"value_zip_size "  |t->value_zip_size  |"\n";
        oss|"user_key_num "    |t->user_key_num    |"\n";
        oss|"internal_key_len "|t->internal_key_len|"\n";
        oss|"internal_key_num "|t->internal_key_num|"\n";
        return std::move(oss);
      }
      json djs;
      djs["Options"] = o->ToJson(dump_options);
      djs["Env"] = JS_TopZipTable_Global_Env();
      t->ToJson(dump_options, djs);
      return JsonToString(djs, dump_options);
    }
    std::string name = fac.Name();
    THROW_InvalidArgument("Is not ToplingZipTable, but is: " + name);
  }
};
ROCKSDB_REG_PluginManip("ToplingZipTable", ToplingZipTableFactory_Manip);

}
