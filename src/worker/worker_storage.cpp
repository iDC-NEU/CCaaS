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
    void WorkerFroTiKVStorageThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochTikv-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, id);
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while (!EpochManager::IsTimerStop()) {
            TiKV::SendTransactionToDB_Usleep();
//                TiKV::SendTransactionToTiKV_Block();
            usleep(sleep_time);
        }
    }

    void WorkerFroLevelDBStorageThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochLevelDB-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, id);
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while (!EpochManager::IsTimerStop()) {
            LevelDB::SendTransactionToDB_Usleep();
            usleep(sleep_time);
        }
    }

    void WorkerFroHBaseStorageThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochHBase-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, id);
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while (!EpochManager::IsTimerStop()) {
            HBase::SendTransactionToDB_Usleep();
            usleep(sleep_time);
        }
    }

    void WorkerFroMOTStorageThreadMain() {
        std::string name = "EpochMOT";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        MOT::SendToMOThreadMain_usleep(); ///EpochManager::CheckRedoLogPushDownState(); in this function
//        MOT::SendToMOThreadMain();
    }


}