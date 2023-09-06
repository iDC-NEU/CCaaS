//
// Created by zwx on 23-8-23.
//

#include <brpc/channel.h>
#include <proto/kvdb_server.pb.h>
#include "workload/multi_model_workload.h"
#include "generator/scrambled_zipfian_generator.h"
#include "generator/uniform_long_generator.h"
#include "common/byte_iterator.h"

namespace workload{
    std::atomic<uint64_t> MultiModelWorkload::txn_id(1), MultiModelWorkload::graph_vid(1), MultiModelWorkload::success_txn_num(1),
            MultiModelWorkload::failed_txn_num(1), MultiModelWorkload::success_op_num(1), MultiModelWorkload::failed_op_num(1);

    Taas::Context MultiModelWorkload::ctx;
    std::unique_ptr<util::thread_pool_light> MultiModelWorkload::thread_pool;
    bthread::CountdownEvent MultiModelWorkload::workCountDown;

    std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<send_multimodel_params>>> MultiModelWorkload::send_multi_txn_queue;
    std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>>> MultiModelWorkload::client_listen_taas_message_queue;
    std::vector<bool> MultiModelWorkload::isExe(5,false);
    std::vector<uint64_t> MultiModelWorkload::execTimes;
    Taas::concurrent_unordered_map<uint64_t ,bool> MultiModelWorkload::multiModelTxnMap;// (txn id,commit cnt)
    Taas::concurrent_unordered_map<uint64_t, std::shared_ptr<std::condition_variable>> MultiModelWorkload::multiModelTxnConditionVariable;

    void MultiModelWorkload::StaticInit(const Taas::Context& ctx_) {
        ctx = ctx_;
        thread_pool = std::make_unique<util::thread_pool_light>(ctx.multiModelContext.kClientNum);
        workCountDown.reset((int)ctx.multiModelContext.kClientNum);
        send_multi_txn_queue = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<send_multimodel_params>>>();
        client_listen_taas_message_queue = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>>>();

    }

    void MultiModelWorkload::buildValues(utils::ByteIteratorMap& values, const std::string& key) {
        auto field = "filed";
        for (int i = 0; i < 10; i ++) {
            // fill with random data
            auto fieldKey = field + std::to_string(i);
            values[fieldKey] = utils::RandomString(256);
        }
    }

    void MultiModelWorkload::LoadData() {
        if ((ctx.multiModelContext.kTestMode == Taas::MultiModel || ctx.multiModelContext.kTestMode == Taas::KV)) {
            workCountDown.reset((int) ctx.multiModelContext.kClientNum);
            for (int i = 0; i < (int) ctx.multiModelContext.kClientNum; i++) {
                thread_pool->push_task([] {
                    LoadKVData();
                    workCountDown.signal();
                });
            }
            workCountDown.wait();
            SetTxnId(0);
        }
        if ((ctx.multiModelContext.kTestMode == Taas::MultiModel || ctx.multiModelContext.kTestMode == Taas::SQL)) {
            workCountDown.reset((int) ctx.multiModelContext.kClientNum);
            for (int i = 0; i < (int) ctx.multiModelContext.kClientNum; i++) {
                thread_pool->push_task([] {
                    LoadSQLData();
                    workCountDown.signal();
                });
            }
            workCountDown.wait();
            SetTxnId(0);
        }
        if ((ctx.multiModelContext.kTestMode == Taas::MultiModel || ctx.multiModelContext.kTestMode == Taas::GQL)) {
            workCountDown.reset((int) ctx.multiModelContext.kClientNum);
            for (int i = 0; i < (int) ctx.multiModelContext.kClientNum; i++) {
                thread_pool->push_task([] {
                    LoadGQLData();
                    workCountDown.signal();
                });
            }
            workCountDown.wait();
            SetTxnId(0);
        }

    }

    void MultiModelWorkload::LoadKVData() {
        uint64_t txnId, kTotalTxnNum = ctx.multiModelContext.kTxnNum;
        while(true) {
            txnId = AddTxnId();
            if(txnId > kTotalTxnNum) break;
            KV::InsertData(txnId);
        }
    }

    void MultiModelWorkload::LoadSQLData() {
        uint64_t txnId, kTotalTxnNum = ctx.multiModelContext.kTxnNum;
        while(true) {
            txnId = AddTxnId();
            if(txnId > kTotalTxnNum) break;
            MOT::InsertData(txnId);
        }
    }

    void MultiModelWorkload::LoadGQLData() {
        uint64_t txnId, kTotalTxnNum = ctx.multiModelContext.kTxnNum;
        while(true) {
            txnId = AddTxnId();
            if(txnId > kTotalTxnNum) break;
            Nebula::InsertData(txnId);
        }
    }

    void MultiModelWorkload::RunMultiTxn() {
        uint64_t txnId, kTotalTxnNum = ctx.multiModelContext.kTxnNum;
        std::unique_ptr<proto::KvDBGetService_Stub> get_stub;
        if((ctx.multiModelContext.kTestMode == Taas::MultiModel || ctx.multiModelContext.kTestMode == Taas::KV)) {
            brpc::Channel chan;
            brpc::ChannelOptions options;
            chan.Init(ctx.storageContext.kLevelDBIP.c_str(), &options);
            std::shared_ptr<proto::Transaction> txn_ptr;
            get_stub = std::make_unique<proto::KvDBGetService_Stub>(&chan);
        }
        while(true) {
            txnId = AddTxnId();
            if(txnId > kTotalTxnNum) break;

            MultiModelTxn txn;
            txn.tid = txnId;

            txn.stTime = Taas::now_to_us();
            uint64_t txn_num = 0;
            auto msg = std::make_unique<proto::Message>();
            auto message_txn = msg->mutable_txn();
            if((ctx.multiModelContext.kTestMode == Taas::MultiModel || ctx.multiModelContext.kTestMode == Taas::KV)) {
                KV::RunTxn(message_txn, *get_stub);
                txn_num ++;
            }
            if((ctx.multiModelContext.kTestMode == Taas::MultiModel || ctx.multiModelContext.kTestMode == Taas::GQL)) {
                    Nebula::RunTxn(txnId);
                    txn_num ++;
            }
            if((ctx.multiModelContext.kTestMode == Taas::MultiModel || ctx.multiModelContext.kTestMode == Taas::GQL)) {
                    MOT::RunTxn(txnId);
                    txn_num ++;
            }
            txn.typeNumber =  txn_num;
            message_txn->set_csn(txn_num);
            message_txn->set_client_ip(ctx.multiModelContext.kMultiModelClient);
            message_txn->set_client_txn_id(txn.tid);
            message_txn->set_txn_type(proto::TxnType::ClientTxn);
            std::unique_ptr<std::string> serialized_txn_str_ptr(new std::string());
            auto res = Taas::Gzip(msg.get(), serialized_txn_str_ptr.get());
            assert(res);
            send_multi_txn_queue->enqueue(std::make_unique<send_multimodel_params>(
                    txn.tid, serialized_txn_str_ptr.release()));
            send_multi_txn_queue->enqueue(std::make_unique<send_multimodel_params>(0, nullptr));
            std::mutex _mutex;
            std::unique_lock<std::mutex> _lock(_mutex);
            auto cv_ptr = std::make_shared<std::condition_variable>();
            multiModelTxnConditionVariable.insert(txn.tid, cv_ptr);
            cv_ptr->wait(_lock);
            txn.edTime = Taas::now_to_us();
            execTimes.emplace_back(txn.edTime-txn.stTime);
        }
        workCountDown.signal();
    }




}