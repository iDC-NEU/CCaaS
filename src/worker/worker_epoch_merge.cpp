//
// Created by 周慰星 on 2022/9/14.
//
#include "transaction/two_phase_commit.h"
#include "worker/worker_epoch_merge.h"
#include "epoch/epoch_manager.h"
#include "transaction/merge.h"
#include "transaction/transaction_cache.h"

namespace Taas {

    void WorkerFroMergeThreadMain(uint64_t id) {
    }

    void EpochWorkerThreadMain(uint64_t id) {
        std::string name = "TaaSMerger-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        Merger merger;
        EpochMessageReceiveHandler receiveHandler;
        class TwoPC two_pc;
        while(init_ok_num.load() < 5) usleep(sleep_time);
//        LOG(INFO) << "start worker init" << id;
        merger.MergeInit(id);
        receiveHandler.Init(id);
        Taas::TwoPC::Init(id);
        bool sleep_flag;
        init_ok_num.fetch_add(1);
//        LOG(INFO) << "finish worker init" << id;
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        SetCPU();
        switch(TaasContext::taasMode) {
            case TaasMode::MultiModel :
            case TaasMode::MultiMaster :
            case TaasMode::Shard : {
                while(!EpochManager::IsTimerStop()) {
                    sleep_flag = true;

                    merger.epoch = EpochManager::GetLogicalEpoch();
                    merger.epoch_mod = merger.epoch % TaasContext::kCacheMaxLength;
                    while (TransactionCache::epoch_read_validate_queue[merger.epoch_mod]->try_dequeue(
                            merger.txn_ptr)) {
                        if (merger.txn_ptr != nullptr && merger.txn_ptr->txn_type() != proto::TxnType::NullMark) {
                            merger.ReadValidate();
                            merger.txn_ptr.reset();
                            sleep_flag = false;
                        }
                    }

                    while (!EpochManager::IsEpochMergeComplete(merger.epoch) && TransactionCache::epoch_merge_queue[merger.epoch_mod]->try_dequeue(merger.txn_ptr)) {
                        if (merger.txn_ptr != nullptr &&
                            merger.txn_ptr->txn_type() != proto::TxnType::NullMark) {
                            merger.Merge();
                            merger.txn_ptr.reset();
                            sleep_flag = false;
                        }
                    }

                    while (EpochManager::IsAbortSetMergeComplete(merger.epoch) &&
                           !EpochManager::IsCommitComplete(merger.epoch) &&
                           TransactionCache::epoch_commit_queue[merger.epoch_mod]->try_dequeue(
                                   merger.txn_ptr)) {
                        if (merger.txn_ptr != nullptr &&
                            merger.txn_ptr->txn_type() != proto::TxnType::NullMark) {
                            merger.Commit();
                            merger.txn_ptr.reset();
                            sleep_flag = false;
                        }
                    }

                    while (EpochManager::IsAbortSetMergeComplete(merger.epoch) &&
                           !EpochManager::IsRecordCommitted(merger.epoch) &&
                           TransactionCache::epoch_redo_log_queue[merger.epoch_mod]->try_dequeue(
                                   merger.txn_ptr)) {
                        if (merger.txn_ptr != nullptr && merger.txn_ptr->txn_type() !=
                                                         proto::TxnType::NullMark) { /// only local txn do redo log
                            merger.RedoLog();
                            merger.txn_ptr.reset();
                            sleep_flag = false;
                        }
                    }

                    while (EpochManager::IsRecordCommitted(merger.epoch) &&
                        !EpochManager::IsResultReturned(merger.epoch) &&
                        TransactionCache::epoch_result_return_queue[merger.epoch_mod]->try_dequeue(
                               merger.txn_ptr)) {
                        if (merger.txn_ptr != nullptr && merger.txn_ptr->txn_type() !=
                                                         proto::TxnType::NullMark) { /// only local txn do redo log
                            merger.ResultReturn();
                            merger.txn_ptr.reset();
                            sleep_flag = false;
                        }
                    }

                    if (sleep_flag) usleep(merge_sleep_time);
                }
                break;
            }
            case TaasMode::TwoPC : {
                two_pc.HandleClientMessage();
                break;
            }
        }
    }
}


