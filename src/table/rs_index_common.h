#pragma once

// must be last included file
#include <terark/succinct/rank_select_simple.hpp>

namespace rocksdb {
using namespace terark;

typedef rank_select_allone     AllOne;
typedef rank_select_allzero    AllZero;
typedef rank_select_il_256_32  IL_256_32;
typedef rank_select_se_512_64  SE_512_64;

enum class WorkingState : uint16_t {
  Building = 1,
  UserMemory = 2,
  MmapFile = 3,
};

} // namespace rocksdb

