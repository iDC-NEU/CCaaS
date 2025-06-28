//
// Created by user on 23-7-3.
//

#ifndef TAAS_EPOCH_MANAGER_SHARDING_H
#define TAAS_EPOCH_MANAGER_SHARDING_H
#pragma once

#include "tools/context.h"

namespace Taas {
    class ShardEpochManager {
    public:
        static bool CheckEpochMergeState();
        static bool CheckEpochAbortMergeState();
        static bool CheckEpochCommitState();

        static void EpochLogicalTimerManagerThreadMain();
    };
}

#endif //TAAS_EPOCH_MANAGER_SHARDING_H
