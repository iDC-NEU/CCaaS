//
// Created by zwx on 23-8-23.
//

#ifndef TAAS_MULTI_MODEL_WORKLOAD_H
#define TAAS_MULTI_MODEL_WORKLOAD_H

#pragma once

#include "kv.h"

#include "tools/atomic_counters.h"
#include "tools/concurrent_hash_map.h"
#include "tools/context.h"
#include "tools/blocking_concurrent_queue.hpp"
#include "proto/message.pb.h"
#include "tools/thread_pool_light.h"

#include "zmq.hpp"
#include <nebula/client/Config.h>
#include <nebula/client/SessionPool.h>
#include <sql.h>
#include <sqlext.h>
#include "tools/thread_pool_light.h"

namespace workload {
    typedef std::string NebulaTxn;
    typedef std::vector<std::string> MotTxn;
    typedef std::vector<KeyValue> KVTxn;

    typedef struct MultiModelTxn{
        KVTxn kvTxn;
        MotTxn motTxn;//在里面插个value为tid的值
        NebulaTxn nebulaTxn;//在里面插个value为tid的值
        uint64_t tid;
        uint64_t typeNumber;
        uint64_t stTime;
        uint64_t edTime;
    }MultiModelTxn;

    struct send_multimodel_params {
        uint64_t txnid{};
        std::string* merge_request_ptr{};
        send_multimodel_params(uint64_t tid, std::string* ptr1)
                : txnid(tid), merge_request_ptr(ptr1) {}
        send_multimodel_params() = default;
    };

    extern std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<send_multimodel_params>>> send_multi_txn_queue;
    extern std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>>> client_listen_taas_message_queue;
    extern uint64_t commitTxnId;
    extern std::vector<bool> isExe;
    extern std::vector<uint64_t> execTimes;
    extern Taas::concurrent_unordered_map<uint64_t, std::condition_variable*> cv;
    extern Taas::concurrent_unordered_map<uint64_t ,bool> multiModelTxnMap;


    class MultiModelWorkload {
    private:
        static uint64_t txn_id, graph_vid;
    public:
        static Taas::MultiModelContext ctx;
        static std::unique_ptr<util::thread_pool_light> thread_pool;
        static std::mutex lock, _mutex, resLock;
        static nebula::SessionPoolConfig nebulaSessionPoolConfig;
        static std::unique_ptr<nebula::SessionPool> nebulaSessionPool;
        static SQLHENV MotEnv ;//定义环境句柄
        static SQLHDBC MotHdbc;//定义数据库连接句柄

        static void StaticInit(const Taas::MultiModelContext& ctx_);
        static void NebulaInit();
        static void MOTInit();
        static void MOTExec(SQLHDBC hdbc, SQLCHAR* sql);
        static void MOTClose();

        static void RunMotTxn(MotTxn motTxn);
        static void RunNebulaTxn(NebulaTxn nebulaTxn);
        static void RunMultiTxn(MultiModelTxn txn);

        static void SetTxnId(int value){ txn_id = value;}
        static uint64_t AddTxnId(){
            return ++txn_id;
        }
        static uint64_t GetTxnId(){ return txn_id;}
        static void SetGraphVid(int value){ graph_vid = value;}
        static uint64_t AddGraphVid(){
            return ++ graph_vid;
        }
        static uint64_t GetGraphVid(){ return graph_vid;}
    };
}


#endif //TAAS_MULTI_MODEL_WORKLOAD_H
