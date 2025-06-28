//
// Created by zwx on 23-7-3.
//

#include "epoch/epoch_manager.h"
#include "epoch/epoch_manager_sharding.h"
#include "message/epoch_message_receive_handler.h"
#include "transaction/merge.h"

#include "string"
#include "tools/thread_pool_light.h"

namespace Taas {

    static uint64_t last_total_commit_txn_num = 0;

    void ShardEpochManager::EpochLogicalTimerManagerThreadMain() {

        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        uint64_t epoch = 1;
        OUTPUTLOG("===== Start Epoch的合并 ===== ", epoch);
        util::thread_pool_light workers(5);
//        while(!EpochManager::IsInitOK() || EpochManager::GetPhysicalEpoch() < 10) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){

            while(epoch >= EpochManager::GetPhysicalEpoch()) std::this_thread::yield();;
            auto time1 = now_to_us();
//                LOG(INFO) << "**** Start Epoch Merge Epoch : " << epoch << "****\n";
            while(!EpochMessageReceiveHandler::CheckEpochClientTxnHandleComplete(epoch)) std::this_thread::yield();;

            while(!EpochMessageReceiveHandler::CheckEpochShardReceiveComplete(epoch)) std::this_thread::yield();;

            while(!EpochMessageReceiveHandler::CheckEpochShardTxnHandleComplete(epoch)) std::this_thread::yield();;
            auto time2 = now_to_us();
            while(!EpochMessageReceiveHandler::CheckEpochBackUpComplete(epoch)) std::this_thread::yield();;

            while(!EpochMessageReceiveHandler::CheckEpochRemoteServerReceiveComplete(epoch)) std::this_thread::yield();;
            auto time3 = now_to_us();

            while(!Merger::CheckEpochMergeComplete(epoch)) std::this_thread::yield();;
            EpochManager::SetEpochMergeComplete(epoch, true);
            merge_epoch.fetch_add(1);
            auto time4 = now_to_us();

            while(!EpochMessageReceiveHandler::CheckEpochAbortSetMergeComplete(epoch)) std::this_thread::yield();;
            EpochManager::SetAbortSetMergeComplete(epoch, true);
            abort_set_epoch.fetch_add(1);
            auto time5 = now_to_us();

            while(!Merger::CheckEpochCommitComplete(epoch)) std::this_thread::yield();;
            EpochManager::SetCommitComplete(epoch, true);
            auto time6 = now_to_us();
            while(!Merger::CheckEpochRecordCommitted(epoch)) std::this_thread::yield();;
            EpochManager::SetRecordCommitted(epoch, true);
            auto time7 = now_to_us();

            ///change to sync
//            while(redo_log_epoch.load() < epoch) std::this_thread::yield();;

            while(!Merger::CheckEpochResultReturned(epoch)) std::this_thread::yield();;
            EpochManager::SetResultReturned(epoch, true);
            auto time8 = now_to_us();
            commit_epoch.fetch_add(1);
            EpochManager::AddLogicalEpoch();
            auto epoch_commit_success_txn_num = ThreadCounters::GetAllThreadLocalCountNum(epoch,
                                               ThreadCounters::epoch_record_committed_txn_num_local_vec);
            total_commit_txn_num += epoch_commit_success_txn_num;///success
            if(epoch % TaasContext::print_mode_size == 0) {
                LOG(INFO) << PrintfToString(
                        "************ 完成一个Epoch的合并 Physical Epoch %lu, Logical Epoch: %lu, Local EpochSuccessCommitTxnNum: %lu,TotalSuccessTxnNum: %lu, EpochCommitTxnNum: %lu ",
                        EpochManager::GetPhysicalEpoch(), epoch, epoch_commit_success_txn_num, total_commit_txn_num,
                        EpochMessageSendHandler::TotalTxnNum.load() - last_total_commit_txn_num)
                          << "\n Time Cost  Epoch: " << epoch
                          << " ,Shard Transmit Txn cost: " << time2 - time1
                          << " ,Validate and Transmit write set Txn cost: " << time3 - time2
                          << " ,Merge time cost : " << time4 - time3
                          << " ,Abort Set Merge time cost : " << time5 - time4
                          << " ,Commit time cost : " << time6 - time5
                          << " ,Log time cost : " << time7 - time6
                          << " ,Result time cost : " << time8 - time7
                          << " Total Time Cost **** " << time8 - time1
                          << " ****end\n";
                OUTPUTLOG("===== Logical Start Epoch的合并 ===== ", epoch);
            }
            epoch ++;
            last_total_commit_txn_num = EpochMessageSendHandler::TotalTxnNum.load();
        }
    }
}