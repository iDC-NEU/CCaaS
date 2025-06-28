//
// Created by zwx on 23-7-3.
//

#ifndef TAAS_WORKER_STORAGE_H
#define TAAS_WORKER_STORAGE_H
#pragma once
#include "tools/context.h"

namespace Taas {

    extern void WorkerFroTiKVStorageThreadMain(uint64_t id);
    extern void WorkerFroMOTStorageThreadMain(uint64_t id);
    extern void WorkerFroNebulaStorageThreadMain(uint64_t id);
    extern void WorkerFroLevelDBStorageThreadMain(uint64_t id);
    extern void WorkerFroHBaseStorageThreadMain(uint64_t id);
    extern void StateChecker();
}

#endif //TAAS_WORKER_STORAGE_H
