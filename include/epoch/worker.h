//
// Created by 周慰星 on 23-3-30.
//

#ifndef TAAS_WORKER_H
#define TAAS_WORKER_H

#include "tools/context.h"

namespace Taas {
    extern void StateChecker(Context ctx);
    extern void WorkerThreadMain(uint64_t id, Context ctx);
}


#endif //TAAS_WORKER_H
