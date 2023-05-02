//
// Created by 周慰星 on 23-3-30.
//

#ifndef TAAS_WORKER_H
#define TAAS_WORKER_H

#include "tools/context.h"

namespace Taas {
    extern void WorkerForPhysicalThreadMain(const Context &ctx);
    extern void WorkerForLogicalThreadMain(const Context& ctx);

    extern void WorkerForLogicalTxnMergeCheckThreadMain(const Context& ctx);
    extern void WorkerForLogicalAbortSetMergeCheckThreadMain();
    extern void WorkerForLogicalCommitCheckThreadMain();
    extern void WorkerForLogicalRedoLogPushDownCheckThreadMain(const Context& ctx);

    extern void WorkerForLogicalReceiveAndReplyCheckThreadMain(const Context& ctx) ;
    extern void WorkerForEpochAbortSendThreadMain(const Context& ctx);
    extern void WorkerForEpochEndFlagSendThreadMain(const Context& ctx) ;
    extern void WorkerForEpochBackUpEndFlagSendThreadMain(const Context& ctx);

    extern void WorkerFroEpochMessageThreadMain(const Context& ctx, uint64_t id);
    extern void WorkerFroTxnMessageThreadMain(const Context& ctx, uint64_t id);
    extern void WorkerFroCommitThreadMain(const Context& ctx, uint64_t id);

    extern void WorkerForClientListenThreadMain(const Context& ctx);
    extern void WorkerForClientSendThreadMain(const Context& ctx);
    extern void WorkerForServerListenThreadMain(const Context& ctx);
    extern void WorkerForServerListenThreadMain_Epoch(const Context& ctx);
    extern void WorkerForServerSendThreadMain(const Context& ctx);
    extern void WorkerFroTiKVStorageThreadMain(const Context& ctx, uint64_t id);
    extern void WorkerFroMOTStorageThreadMain();

    extern void StateChecker(const Context& ctx);
}


#endif //TAAS_WORKER_H
