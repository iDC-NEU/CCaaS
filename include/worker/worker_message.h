//
// Created by user on 23-7-3.
//

#ifndef TAAS_WORKER_MESSAGE_H
#define TAAS_WORKER_MESSAGE_H
#pragma once
#include "tools/context.h"

namespace Taas {

    extern void WorkerFroMessageThreadMain(uint64_t id);
    extern void WorkerFroMessageEpochThreadMain(uint64_t id);

    extern void WorkerForClientListenThreadMain();
    extern void WorkerForClientSendThreadMain();
    extern void WorkerForServerListenThreadMain();
    extern void WorkerForServerListenThreadMain_Epoch();
    extern void WorkerForServerSendThreadMain();
    extern void WorkerForServerSendPUBThreadMain();
    extern void WorkerForStorageSendMOTThreadMain();
    extern void WorkerForStorageSendNebulaThreadMain();
}

#endif //TAAS_WORKER_MESSAGE_H
