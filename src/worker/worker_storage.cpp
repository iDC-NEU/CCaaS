//
// Created by zwx on 23-7-3.
//
#include "worker/worker_message.h"
#include "epoch/epoch_manager.h"
#include "storage/tikv.h"
#include "storage/leveldb.h"
#include "storage/hbase.h"
#include "storage/mot.h"
#include "storage/nebula.h"

namespace Taas {

    void WorkerFroMOTStorageThreadMain(uint64_t id) {
        std::string name = "EpochMOT";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        MOT mot;
        mot.Init();
        while (!EpochManager::IsTimerStop()) {
            mot.SendTransactionToDB_Usleep();
        }
    }

    void WorkerFroNebulaStorageThreadMain(uint64_t id) {
        std::string name = "EpochNebula";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        Nebula nebula;
        nebula.Init();
        while (!EpochManager::IsTimerStop()) {
            nebula.SendTransactionToDB_Usleep();
        }
    }

    void WorkerFroTiKVStorageThreadMain(uint64_t id) {
        std::string name = "EpochTikv-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        TiKV tikv;
        tikv.Init();
        while (!EpochManager::IsTimerStop()) {
//            if(id == 0)
            tikv.SendTransactionToDB_Usleep();
//            else
//                TiKV::SendTransactionToDB_Block();
        }
    }

    void WorkerFroLevelDBStorageThreadMain(uint64_t id) {
        std::string name = "EpochLevelDB-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        class LevelDB leveldb;
        leveldb.Init();
        while (!EpochManager::IsTimerStop()) {
//            if(id == 0)
            leveldb.SendTransactionToDB_Usleep();
//            else
//                LevelDB::SendTransactionToDB_Block();

        }
    }

    void WorkerFroHBaseStorageThreadMain(uint64_t id) {
        std::string name = "EpochHBase-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        class HBase hbase;
        hbase.Init();
        while (!EpochManager::IsTimerStop()) {
//            if(id == 0)
            hbase.SendTransactionToDB_Usleep();
//            else
//                HBase::SendTransactionToDB_Block();
        }
    }
}