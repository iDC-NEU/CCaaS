//
// Created by 周慰星 on 23-3-30.
//

#ifndef TAAS_WORKER_EPOCH_MANAGER_H
#define TAAS_WORKER_EPOCH_MANAGER_H
#pragma once
#include "tools/context.h"

namespace Taas {
    extern void WorkerForPhysicalThreadMain(const Context &ctx);
    extern void WorkerForLogicalThreadMain(const Context& ctx);

    extern void WorkerForLogicalTxnMergeCheckThreadMain(const Context& ctx);
    extern void WorkerForLogicalAbortSetMergeCheckThreadMain(const Context& ctx);
    extern void WorkerForLogicalCommitCheckThreadMain(const Context& ctx);
    extern void WorkerForLogicalRedoLogPushDownCheckThreadMain(const Context& ctx);

    extern void WorkerForLogicalReceiveAndReplyCheckThreadMain(const Context& ctx) ;
    extern void WorkerForEpochAbortSendThreadMain(const Context& ctx);
    extern void WorkerForEpochEndFlagSendThreadMain(const Context& ctx) ;
    extern void WorkerForEpochBackUpEndFlagSendThreadMain(const Context& ctx);

}


#endif //TAAS_WORKER_EPOCH_MANAGER_H
