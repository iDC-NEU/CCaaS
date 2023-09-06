//
// Created by zwx on 23-8-23.
//

#ifndef TAAS_MULTI_MODEL_WORKLOAD_H
#define TAAS_MULTI_MODEL_WORKLOAD_H

#pragma once

#include "tools/concurrent_hash_map.h"
#include "tools/context.h"
#include "tools/utilities.h"
#include "tools/blocking_concurrent_queue.hpp"
#include "tools/thread_pool_light.h"

#include "proto/message.pb.h"

#include "zmq.hpp"
#include <bthread/countdown_event.h>

#include "kv.h"
#include "nebula.h"
#include "mot.h"
#include "generator/discrete_generator.h"
#include "generator/scrambled_zipfian_generator.h"
#include "common/byte_iterator.h"

#include <sql.h>
#include <sqlext.h>

namespace workload {

    enum class Operation {
        INSERT,
        READ,
        UPDATE,
        SCAN,
        READ_MODIFY_WRITE,
    };

    typedef struct MultiModelTxn{
        KVTxn kvTxn;
        MOTTxn motTxn;//在里面插个value为tid的值
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

    class MultiModelWorkload {
    public:
        static std::atomic<uint64_t> txn_id, graph_vid, success_txn_num, failed_txn_num, success_op_num, failed_op_num;
        static Taas::Context ctx;
        static std::unique_ptr<util::thread_pool_light> thread_pool;
        static std::unique_ptr<utils::DiscreteGenerator<Operation>> operationChooser;
        static std::vector<std::unique_ptr<utils::NumberGenerator>> keyChooser;
        static bthread::CountdownEvent workCountDown;
        static std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<send_multimodel_params>>> send_multi_txn_queue;
        static std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>>> client_listen_taas_message_queue;
        static std::vector<bool> isExe;
        static std::vector<uint64_t> execTimes;
        static Taas::concurrent_unordered_map<uint64_t ,bool> multiModelTxnMap;
        static Taas::concurrent_unordered_map<uint64_t, std::shared_ptr<std::condition_variable>> multiModelTxnConditionVariable;

        static void StaticInit(const Taas::Context& ctx_);
        static void buildValues(utils::ByteIteratorMap& values, const std::string& key);
        static void LoadData();
        static void LoadKVData();
        static void LoadSQLData();
        static void LoadGQLData();
        static void RunMultiTxn();
        static void SetTxnId(uint64_t value){ txn_id.store(value);}
        static uint64_t AddTxnId(){
            return txn_id.fetch_add(1);
        }
        static uint64_t GetTxnId(){ return txn_id.load();}
        static void SetGraphVid(uint64_t value){ graph_vid.store(value);}
        static uint64_t AddGraphVid(){
            return graph_vid.fetch_add(1);;
        }
        static uint64_t GetGraphVid(){ return graph_vid.load();}


        static void CreateOperationGenerator() {
            operationChooser = std::make_unique<utils::DiscreteGenerator<Operation>>();
            operationChooser->addValue((double)ctx.multiModelContext.kReadNum / 100.0, Operation::READ);
            operationChooser->addValue((double)ctx.multiModelContext.kWriteNum / 100.0, Operation::UPDATE);
        }

        static void CreateKeyChooser() {
            keyChooser.push_back(utils::ScrambledZipfianGenerator::NewScrambledZipfianGenerator(1, ctx.multiModelContext.kRecordCount/3));
            keyChooser.push_back(utils::ScrambledZipfianGenerator::NewScrambledZipfianGenerator(ctx.multiModelContext.kRecordCount/3 + 1,ctx.multiModelContext.kRecordCount * 2 / 3));
            keyChooser.push_back(utils::ScrambledZipfianGenerator::NewScrambledZipfianGenerator(ctx.multiModelContext.kRecordCount * 2 / 3 + 1,ctx.multiModelContext.kRecordCount));
        }

    };
}


#endif //TAAS_MULTI_MODEL_WORKLOAD_H
