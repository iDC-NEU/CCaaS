//
// Created by user on 23-7-3.
//

#ifndef TAAS_EPOCH_MANAGER_MULTI_MASTER_H
#define TAAS_EPOCH_MANAGER_MULTI_MASTER_H
#pragma once

#include "tools/context.h"

namespace Taas {
    class MultiMasterEpochManager {
    public:
        static bool CheckEpochMergeState(const Context& ctx);
        static bool CheckEpochAbortMergeState(const Context& ctx);
        static bool CheckEpochCommitState(const Context& ctx);

        static void EpochLogicalTimerManagerThreadMain(const Context &ctx);
    };
}
#endif //TAAS_EPOCH_MANAGER_MULTI_MASTER_H
