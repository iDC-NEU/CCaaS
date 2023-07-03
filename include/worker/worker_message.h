//
// Created by user on 23-7-3.
//

#ifndef TAAS_WORKER_MESSAGE_H
#define TAAS_WORKER_MESSAGE_H
#pragma once
#include "tools/context.h"

namespace Taas {

    extern void WorkerForClientListenThreadMain(const Context& ctx);
    extern void WorkerForClientSendThreadMain(const Context& ctx);
    extern void WorkerForServerListenThreadMain(const Context& ctx);
    extern void WorkerForServerListenThreadMain_Epoch(const Context& ctx);
    extern void WorkerForServerSendThreadMain(const Context& ctx);

}

#endif //TAAS_WORKER_MESSAGE_H
