//
// Created by zwx on 23-7-3.
//
#include "worker/worker_message.h"
#include "epoch/epoch_manager.h"
#include "storage/tikv.h"
#include "storage/leveldb.h"
#include "storage/hbase.h"
#include "storage/mot.h"

namespace Taas {

    void WorkerFroMOTStorageThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochMOT";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while (!EpochManager::IsTimerStop()) {
//            if(id == 0)
                MOT::SendTransactionToDB_Usleep();
//            else
//                MOT::SendTransactionToDB_Block();
        }
        ///EpochManager::CheckRedoLogPushDownState(); in this function
    }

    void WorkerFroTiKVStorageThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochTikv-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while (!EpochManager::IsTimerStop()) {
//            if(id == 0)
                TiKV::SendTransactionToDB_Usleep();
//            else
//                TiKV::SendTransactionToDB_Block();
        }
    }

    void WorkerFroLevelDBStorageThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochLevelDB-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while (!EpochManager::IsTimerStop()) {
//            if(id == 0)
                LevelDB::SendTransactionToDB_Usleep();
//            else
//                LevelDB::SendTransactionToDB_Block();

        }
    }

    void WorkerFroHBaseStorageThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochHBase-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while (!EpochManager::IsTimerStop()) {
//            if(id == 0)
                HBase::SendTransactionToDB_Usleep();
//            else
//                HBase::SendTransactionToDB_Block();
        }
    }
}