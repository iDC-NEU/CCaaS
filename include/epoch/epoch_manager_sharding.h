//
// Created by user on 23-7-3.
//

#ifndef TAAS_EPOCH_MANAGER_SHARDING_H
#define TAAS_EPOCH_MANAGER_SHARDING_H
#pragma once

#include "tools/context.h"

namespace Taas {
    class ShardingEpochManager {
    public:
        static bool CheckEpochMergeState(const Context& ctx);
        static bool CheckEpochAbortMergeState(const Context& ctx);
        static bool CheckEpochCommitState(const Context& ctx);
    };
}

#endif //TAAS_EPOCH_MANAGER_SHARDING_H
