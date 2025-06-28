//
// Created by 周慰星 on 23-3-30.
//

#ifndef TAAS_WORKER_EPOCH_MANAGER_H
#define TAAS_WORKER_EPOCH_MANAGER_H
#pragma once
#include "tools/context.h"

namespace Taas {
    extern void WorkerForPhysicalThreadMain();
    extern void WorkerForLogicalThreadMain();

    extern void WorkerForLogicalTxnMergeCheckThreadMain();
    extern void WorkerForLogicalAbortSetMergeCheckThreadMain();
    extern void WorkerForLogicalCommitCheckThreadMain();
    extern void WorkerForLogicalRedoLogPushDownCheckThreadMain();

    extern void WorkerForEpochControlMessageThreadMain();
    extern void WorkerForLogicalReceiveAndReplyCheckThreadMain() ;
    extern void WorkerForEpochAbortSendThreadMain();
    extern void WorkerForEpochEndFlagSendThreadMain() ;
    extern void WorkerForEpochBackUpEndFlagSendThreadMain();

}


#endif //TAAS_WORKER_EPOCH_MANAGER_H
