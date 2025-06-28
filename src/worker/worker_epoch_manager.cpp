//
// Created by 周慰星 on 2022/9/14.
//
#include "worker/worker_epoch_manager.h"
#include "epoch/epoch_manager_sharding.h"
#include "epoch/epoch_manager_multi_master.h"
#include "epoch/two_phase_commit.h"
#include "epoch/epoch_manager.h"
#include "transaction/merge.h"

namespace Taas {

    void WorkerForPhysicalThreadMain() {
        std::string name = "EpochPhysical";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        EpochPhysicalTimerManagerThreadMain();
   }

    void WorkerForLogicalThreadMain() {
        std::string name = "EpochLogical";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        ShardEpochManager::EpochLogicalTimerManagerThreadMain();
    }

    void WorkerForEpochControlMessageThreadMain() {
        SetCPU();
        while(!EpochManager::IsInitOK() || EpochManager::GetPhysicalEpoch() < 10) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(TaasContext::taasMode) {
                case TaasMode::MultiModel :
                case TaasMode::MultiMaster :
                case TaasMode::Shard : {
                    uint64_t local_server_id = TaasContext::txn_node_ip_index;
                    uint64_t shard_epoch = 1, remote_server_epoch = 1, abort_send_epoch = 1, server_num = TaasContext::kTxnNodeNum;
                    bool sleep_flag;
                    while(!EpochManager::IsInitOK()) usleep(sleep_time);
                    while(!EpochManager::IsTimerStop()) {
                        sleep_flag = true;
//                        while(shard_epoch >= EpochManager::GetPhysicalEpoch()) {
//                            usleep(100);
//                        }
                        if (shard_epoch < EpochManager::GetPhysicalEpoch() && EpochMessageReceiveHandler::CheckEpochClientTxnHandleComplete(shard_epoch)) {
                            EpochMessageSendHandler::SendEpochShardEndMessage(local_server_id, shard_epoch, server_num);
//                            LOG(INFO) << "Send EpochShardEndFlag epoch " << shard_epoch;
                            shard_epoch ++;
                            sleep_flag = false;
                        }

                        if(remote_server_epoch < shard_epoch &&
                            EpochMessageReceiveHandler::CheckEpochShardReceiveComplete(remote_server_epoch) &&
                            EpochMessageReceiveHandler::CheckEpochShardTxnHandleComplete(remote_server_epoch)) {
                            EpochMessageSendHandler::SendEpochRemoteServerEndMessage(local_server_id, remote_server_epoch, server_num);
//                            LOG(INFO) << "Send SendEpochRemoteServerEndMessage epoch " << remote_server_epoch;
                            remote_server_epoch ++;
                            sleep_flag = false;
                        }

                        if(abort_send_epoch < remote_server_epoch && EpochManager::IsEpochMergeComplete(abort_send_epoch)) {
                            EpochMessageSendHandler::SendAbortSet(local_server_id, abort_send_epoch);
//                            LOG(INFO) << "Send SendAbortSet epoch " << abort_send_epoch;
                            abort_send_epoch ++;
                            sleep_flag = false;
                        }

//
//                        if(EpochManager::IsEpochMergeComplete(abort_send_epoch)) {
//                            EpochMessageSendHandler::SendAbortSet(local_server_id, abort_send_epoch, TaasContext::kCacheMaxLength);
//                            abort_send_epoch ++;
//                            sleep_flag = false;
//                        }
//                        if(sleep_flag) usleep(100);
                        if(sleep_flag) std::this_thread::yield();
                    }
                    break;
                }
                case TaasMode::TwoPC : {
                    //
                    break;
                }
            }
        }
    }

    void WorkerForLogicalRedoLogPushDownCheckThreadMain() {
        SetCPU();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(TaasContext::taasMode) {
                case TaasMode::MultiModel :
                case TaasMode::MultiMaster :
                case TaasMode::Shard : {
                    CheckRedoLogPushDownState();
                    break;
                }
                case TaasMode::TwoPC : {
//                    TwoPhaseCommitManager::TwoPhaseCommitManagerThreadMain(ctx);
                }
            }
//            CheckRedoLogPushDownState(ctx);
        }
    }

}

