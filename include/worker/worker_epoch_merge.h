//
// Created by user on 23-7-3.
//

#ifndef TAAS_WORKER_EPOCH_MERGE_H
#define TAAS_WORKER_EPOCH_MERGE_H
#pragma once
#include "tools/context.h"

namespace Taas {

    extern void WorkerFroMergeThreadMain(const Context& ctx, uint64_t id);
    extern void WorkerFroEpochMessageThreadMain(const Context& ctx, uint64_t id);
    extern void WorkerFroTxnMessageThreadMain(const Context& ctx, uint64_t id);
    extern void WorkerFroCommitThreadMain(const Context& ctx, uint64_t id);

}

#endif //TAAS_WORKER_EPOCH_MERGE_H
