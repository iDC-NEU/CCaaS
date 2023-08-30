//
// Created by zwx on 23-8-23.
//

#include "workload/multi_model_workload.h"

namespace workload{
    std::atomic<uint64_t> MultiModelWorkload::txn_id(1), MultiModelWorkload::graph_vid(1);
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

    void MultiModelWorkload::LoadData() {
        uint64_t txnId, kTotalTxnNum = MultiModelWorkload::ctx.multiModelContext.kTxnNum;
        while(true) {
            txnId = MultiModelWorkload::AddTxnId();
            if(txnId > kTotalTxnNum) break;
            KV::InsertData(txnId);
            MOT::InsertData(txnId);
            Nebula::InsertData(txnId);
        }
        MultiModelWorkload::workCountDown.signal();
    }

    void MultiModelWorkload::RunMultiTxn() {
        uint64_t txnId, kTotalTxnNum = MultiModelWorkload::ctx.multiModelContext.kTxnNum;
        while(true) {
            txnId = MultiModelWorkload::AddTxnId();
            if(txnId > kTotalTxnNum) break;

            MultiModelTxn txn;
            txn.tid = txnId;
            txn.kvTxn = KV::txnVector[txnId];
            txn.motTxn = MOT::txnVector[txnId];
            txn.nebulaTxn = Nebula::txnVector[txnId];

            txn.stTime = Taas::now_to_us();
            uint64_t txn_num = 0;
            std::thread t1,t2;
            if(MultiModelWorkload::ctx.multiModelContext.isUseNebula&& !txn.nebulaTxn.empty()) {
                    Nebula::RunTxn(txn.nebulaTxn);
                    txn_num ++;
            }
            if(MultiModelWorkload::ctx.multiModelContext.isUseMot && !txn.motTxn.empty()) {
                    MOT::RunTxn(txn.motTxn);
                    txn_num ++;
            }
            auto msg = std::make_unique<proto::Message>();
            auto message_txn = msg->mutable_txn();
            if(MultiModelWorkload::ctx.multiModelContext.isUseMot && !txn.kvTxn.empty()) {
                KV::RunTxn(txn.kvTxn, message_txn);
                txn_num ++;
            }
            txn.typeNumber =  txn_num;
            message_txn->set_csn(txn_num);
            message_txn->set_client_ip(MultiModelWorkload::ctx.multiModelContext.kMultiModelClient);
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
            MultiModelWorkload::multiModelTxnConditionVariable.insert(txn.tid, cv_ptr);
            cv_ptr->wait(_lock);
            txn.edTime = Taas::now_to_us();
            execTimes.emplace_back(txn.edTime-txn.stTime);
        }
        MultiModelWorkload::workCountDown.signal();
    }



}