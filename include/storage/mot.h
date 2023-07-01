//
// Created by user on 23-4-9.
//

#ifndef TAAS_MOT_H
#define TAAS_MOT_H

#pragma once


#include <proto/transaction.pb.h>
#include "tools/atomic_counters.h"
#include "tools/blocking_concurrent_queue.hpp"
#include "tools/context.h"

namespace Taas {

    class MOT {
        public:
        static Context ctx;
        static std::atomic<uint64_t> pushed_down_mot_epoch;
        static std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>> task_queue;

        static bool StaticInit(const Context &ctx_);
        static bool GeneratePushDownTask(uint64_t &epoch);
        static void SendToMOThreadMain_usleep();
        static void SendToMOThreadMain();

        static bool IsMOTPushDownComplete(uint64_t& epoch) {
            return epoch < pushed_down_mot_epoch.load();
        }
        static uint64_t GetPushedDownMOTEpoch() {
            return pushed_down_mot_epoch.load();
        }
        static uint64_t IncPushedDownMOTEpoch() {
            return pushed_down_mot_epoch.fetch_add(1);
        }


    };

}

#endif //TAAS_MOT_H