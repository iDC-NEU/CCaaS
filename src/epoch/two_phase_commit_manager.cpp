//
// Created by zwx on 23-11-9.
//
#include "epoch/epoch_manager.h"
#include "epoch/two_phase_commit.h"

#include "tools/thread_counters.h"
#include "message/epoch_message_receive_handler.h"

namespace Taas {
    void TwoPhaseCommitManager::TwoPhaseCommitManagerThreadMain() {
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        // print some info message
        while(!EpochManager::IsTimerStop()) {
            usleep(10000000);
            LOG(INFO) << "============ Two Phase Commit INFO ============";
        }
    }
}
