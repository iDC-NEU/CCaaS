//
// Created by zwx on 23-7-3.
//

#ifndef TAAS_WORKER_STORAGE_H
#define TAAS_WORKER_STORAGE_H
#pragma once
#include "tools/context.h"

namespace Taas {

    extern void WorkerFroStorageThreadMain(const Context& ctx, uint64_t id);
    extern void WorkerFroMOTStorageThreadMain(const Context& ctx, uint64_t id);
    extern void WorkerFroLevelDBStorageThreadMain(const Context& ctx, uint64_t id);
    extern void WorkerFroHBaseStorageThreadMain(const Context& ctx, uint64_t id);
    extern void StateChecker(const Context& ctx);
}

#endif //TAAS_WORKER_STORAGE_H
