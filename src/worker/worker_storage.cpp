//
// Created by zwx on 23-7-3.
//
#include "worker/worker_message.h"
#include "epoch/epoch_manager.h"
#include "transaction/merge.h"
#include "storage/tikv.h"
#include "storage/leveldb.h"
#include "storage/hbase.h"
#include "storage/mot.h"

namespace Taas {

    void WorkerFroMOTStorageThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochMOT";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
//        uint64_t epoch;
        uint64_t epoch_mod;
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while (!EpochManager::IsTimerStop()) {
            if(id < ctx.kUsleepThreadNum ) {
                while(!EpochManager::IsTimerStop()){
//                    epoch = EpochManager::GetLogicalEpoch();
                    epoch_mod = EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength;
                    while(MOT::epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
//                        if(txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
//                            continue;
//                        }
                        MOT::redo_log_queue->enqueue(std::move(txn_ptr));
                        MOT::redo_log_queue->enqueue(nullptr);
                    }
                    usleep(logical_sleep_timme);
                }
            }
            else {
                if(id < ctx.kUsleepThreadNum)
                    MOT::SendTransactionToDB_Usleep();
                else
                    MOT::SendTransactionToDB_Block();
            }
        }
        ///EpochManager::CheckRedoLogPushDownState(); in this function
    }

    void WorkerFroTiKVStorageThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochTikv-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        uint64_t epoch_mod;
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while (!EpochManager::IsTimerStop()) {
            if(id < ctx.kUsleepThreadNum ) {
                while(!EpochManager::IsTimerStop()){
//                    epoch = EpochManager::GetLogicalEpoch();
                    epoch_mod = EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength;
                    while(TiKV::epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
//                        if(txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
//                            continue;
//                        }
                        TiKV::redo_log_queue->enqueue(std::move(txn_ptr));
                        TiKV::redo_log_queue->enqueue(nullptr);
                    }
                    usleep(logical_sleep_timme);
                }
            }
            else {
                if(id < ctx.kUsleepThreadNum)
                    TiKV::SendTransactionToDB_Usleep();
                else
                    TiKV::SendTransactionToDB_Block();
            }
        }
    }

    void WorkerFroLevelDBStorageThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochLevelDB-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        uint64_t epoch_mod;
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while (!EpochManager::IsTimerStop()) {
            if(id < ctx.kUsleepThreadNum ) {
                while(!EpochManager::IsTimerStop()){
//                    epoch = EpochManager::GetLogicalEpoch();
                    epoch_mod = EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength;
                    while(LevelDB::epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
//                        if(txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
//                            continue;
//                        }
                        LevelDB::redo_log_queue->enqueue(std::move(txn_ptr));
                        LevelDB::redo_log_queue->enqueue(nullptr);
                    }
                    usleep(logical_sleep_timme);
                }
            }
            else {
                if(id < ctx.kUsleepThreadNum)
                    LevelDB::SendTransactionToDB_Usleep();
                else
                    LevelDB::SendTransactionToDB_Block();
            }
        }
    }

    void WorkerFroHBaseStorageThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochHBase-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        uint64_t epoch_mod;
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while (!EpochManager::IsTimerStop()) {
            if(id < ctx.kUsleepThreadNum ) {
                while(!EpochManager::IsTimerStop()){
//                    epoch = EpochManager::GetLogicalEpoch();
                    epoch_mod = EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength;
                    while(HBase::epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
//                        if(txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
//                            continue;
//                        }
                        HBase::redo_log_queue->enqueue(std::move(txn_ptr));
                        HBase::redo_log_queue->enqueue(nullptr);
                    }
                    usleep(logical_sleep_timme);
                }
            }
            else {
                if(id < ctx.kUsleepThreadNum)
                    HBase::SendTransactionToDB_Usleep();
                else
                    HBase::SendTransactionToDB_Block();
            }
        }
    }
}