//
// Created by user on 23-8-24.
//

#ifndef TAAS_WORKER_H
#define TAAS_WORKER_H

#pragma once

#include "tools/utilities.h"
#include "tools/context.h"

#include "zmq.hpp"
#include "tikv_client.h"
#include "proto/message.pb.h"
#include <queue>
#include <utility>
#include <fstream>
#include <iostream>
#include <common/Init.h>

namespace workload {
    extern void ClientListenTaasThreadMain();

    [[noreturn]] extern void DequeueClientListenTaasMessageQueue();

    [[noreturn]] extern void SendTaasClientThreadMain();

    extern void WorkerGenerateTxnFileThreadMain();
    extern void WorkerReadTxnFileThreadMain();

    extern int main();
}

#endif //TAAS_WORKER_H
