//
// Created by zwx on 23-8-23.
//

#include "workload/multi_model_workload.h"
#include "generator/scrambled_zipfian_generator.h"
#include "generator/uniform_long_generator.h"
#include "common/byte_iterator.h"

#include <glog/logging.h>
#include <brpc/channel.h>
#include <proto/kvdb_server.pb.h>

namespace workload{
    std::atomic<uint64_t> MultiModelWorkload::txn_id(1), MultiModelWorkload::graph_vid(1), MultiModelWorkload::success_txn_num(1),
            MultiModelWorkload::failed_txn_num(1), MultiModelWorkload::success_op_num(1), MultiModelWorkload::failed_op_num(1),
            MultiModelWorkload::subWorksNum(0);

    Taas::Context MultiModelWorkload::ctx;
    std::unique_ptr<util::thread_pool_light> MultiModelWorkload::thread_pool;
    std::unique_ptr<utils::DiscreteGenerator<Operation>> MultiModelWorkload::operationChooser;
    std::vector<std::unique_ptr<utils::NumberGenerator>> MultiModelWorkload::keyChooser;
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
        CreateOperationGenerator();
        CreateKeyChooser();
        execTimes.resize(ctx.multiModelContext.kTxnNum + 10000);
    }

    void MultiModelWorkload::buildValues(utils::ByteIteratorMap &values) {
        auto field = "filed";
        for (int i = 0; i < 10; i ++) {
            // fill with random data
            auto fieldKey = field + std::to_string(i);
            values[fieldKey] = utils::RandomString(256);
        }
    }

    void MultiModelWorkload::LoadData() {
        if ((ctx.multiModelContext.kTestMode == Taas::MultiModelTest || ctx.multiModelContext.kTestMode == Taas::KV)) {
            workCountDown.reset((int) ctx.multiModelContext.kClientNum);
            for (int i = 0; i < (int) ctx.multiModelContext.kClientNum; i++) {
                thread_pool->push_task([] {
                    int _seed = 0;
                    utils::GetThreadLocalRandomGenerator()->seed(_seed);
                    LoadKVData();
                    workCountDown.signal();
                });
            }
            workCountDown.wait();
            SetTxnId(0);
        }
        if ((ctx.multiModelContext.kTestMode == Taas::MultiModelTest || ctx.multiModelContext.kTestMode == Taas::SQL)) {
            workCountDown.reset((int) ctx.multiModelContext.kClientNum);
            for (int i = 0; i < (int) ctx.multiModelContext.kClientNum; i++) {
                thread_pool->push_task([] {
                    int _seed = 0;
                    utils::GetThreadLocalRandomGenerator()->seed(_seed);
                    LoadSQLData();
                    workCountDown.signal();
                });
            }
            workCountDown.wait();
            SetTxnId(0);
        }
        if ((ctx.multiModelContext.kTestMode == Taas::MultiModelTest || ctx.multiModelContext.kTestMode == Taas::GQL)) {
            workCountDown.reset((int) ctx.multiModelContext.kClientNum);
            for (int i = 0; i < (int) ctx.multiModelContext.kClientNum; i++) {
                thread_pool->push_task([] {
                    int _seed = 0;
                    utils::GetThreadLocalRandomGenerator()->seed(_seed);
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
        auto sunTxnNum = std::make_shared<std::atomic<uint64_t>>(0);
        uint64_t totalSubTxnNum = 0;
        int generator_thread_num = 4;
        auto threads = std::make_unique<util::thread_pool_light>(generator_thread_num);
        for(int i = 0; i < generator_thread_num; ++i) {
            threads->push_task([]{
                int _seed = 0;
                utils::GetThreadLocalRandomGenerator()->seed(_seed);
                usleep(2000);
            });
        }
        while(true) {
            txnId = AddTxnId();
            if(txnId > kTotalTxnNum) break;
            LOG(INFO) << "start txn " << txnId << " exec";
            auto stTime = Taas::now_to_us();

            auto txn_num = std::make_shared<std::atomic<uint64_t>>(0);
            auto msg = std::make_unique<proto::Message>();
            auto message_txn = msg->mutable_txn();
            sunTxnNum->store(0);
            totalSubTxnNum = 0;
            if((ctx.multiModelContext.kTestMode == Taas::MultiModelTest || ctx.multiModelContext.kTestMode == Taas::GQL)) {
                totalSubTxnNum ++;
                threads->push_task(Nebula::RunTxn, txnId, sunTxnNum, txn_num);
                ///if a read only txn , txn_num should not be added
            }
            if((ctx.multiModelContext.kTestMode == Taas::MultiModelTest || ctx.multiModelContext.kTestMode == Taas::SQL)) {
                totalSubTxnNum ++;
                threads->push_task(MOT::RunTxn, txnId, sunTxnNum, txn_num);
                ///if a read only txn , txn_num should not be added
            }
            if((ctx.multiModelContext.kTestMode == Taas::MultiModelTest || ctx.multiModelContext.kTestMode == Taas::KV)) {
                KV::RunTxn(message_txn);
                ///read onlu txn, also send to taas
            }
            txn_num->fetch_add(1);
            ///todo : block wait sql and gql send
            message_txn->set_csn(txn_num->load());
            message_txn->set_client_ip(ctx.multiModelContext.kMultiModelClientIP);
            message_txn->set_client_txn_id(txnId);
            message_txn->set_txn_type(proto::TxnType::ClientTxn);
            message_txn->set_storage_type("kv");
            std::unique_ptr<std::string> serialized_txn_str_ptr(new std::string());
            auto res = Taas::Gzip(msg.get(), serialized_txn_str_ptr.get());
            assert(res);
            send_multi_txn_queue->enqueue(std::make_unique<send_multimodel_params>(
                    txnId, serialized_txn_str_ptr.release()));
            send_multi_txn_queue->enqueue(std::make_unique<send_multimodel_params>(0, nullptr));
            std::mutex _mutex;
            std::unique_lock<std::mutex> _lock(_mutex);
            auto cv_ptr = std::make_shared<std::condition_variable>();
            multiModelTxnConditionVariable.insert(txnId, cv_ptr);
            cv_ptr->wait(_lock);/// check commit state is commit / abort, otherwise block again
            /// todo : collect return result
//                LOG(INFO) << "waiting for sub " << txnId << " txns execed";
            while(sunTxnNum->load() < totalSubTxnNum) usleep(1000);
//                LOG(INFO) << "sub " << txnId << " txns execed";
//            LOG(INFO) << "txn " << txnId << " exec finished";
            auto edTime = Taas::now_to_us();
            execTimes[txnId] = (edTime - stTime);
        }
        subWorksNum.fetch_add(1);
        workCountDown.signal();
    }




}