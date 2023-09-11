//
// Created by zwx on 23-8-29.
//

#include <brpc/controller.h>
#include "tools/utilities.h"
#include "workload/kv.h"
#include "workload/multi_model_workload.h"

#include "proto/message.pb.h"

namespace workload {

    void KV::InsertData(uint64_t tid) {
        if(tid > MultiModelWorkload::ctx.multiModelContext.kRecordCount) return;
        auto msg = std::make_unique<proto::Message>();
        auto message_txn = msg->mutable_txn();
        proto::Row *row = message_txn->add_row();
        char genKey[100];
        sprintf(genKey, "usertable_key:%064lu", tid);
        std::string data = Taas::RandomString(256);
        row->set_key(genKey);
        row->set_data(data);
        row->set_op_type(proto::OpType::Insert);
        message_txn->set_csn(1);
        message_txn->set_client_ip(MultiModelWorkload::ctx.multiModelContext.kMultiModelClientIP);
        message_txn->set_client_txn_id(tid);
        message_txn->set_txn_type(proto::TxnType::ClientTxn);
        std::unique_ptr<std::string> serialized_txn_str_ptr(new std::string());
        auto res = Taas::Gzip(msg.get(), serialized_txn_str_ptr.get());
        assert(res);
        MultiModelWorkload::send_multi_txn_queue->enqueue(std::make_unique<send_multimodel_params>(tid, serialized_txn_str_ptr.release()));
        MultiModelWorkload::send_multi_txn_queue->enqueue(std::make_unique<send_multimodel_params>(0, nullptr));
        std::mutex _mutex;
        std::unique_lock<std::mutex> _lock(_mutex);
        auto cv_ptr = std::make_shared<std::condition_variable>();
        MultiModelWorkload::multiModelTxnConditionVariable.insert(tid, cv_ptr);
        cv_ptr->wait(_lock);
        ///todo check return result
    }

    void KV::RunTxn(proto::Transaction* message_txn, proto::KvDBGetService_Stub& get_stub) {
        char genKey[100];
        std::string value;
        int cnt, i;
        if(MultiModelWorkload::ctx.multiModelContext.kTestMode == Taas::MultiModelTest) {
            cnt = 4;
        }
        else if(MultiModelWorkload::ctx.multiModelContext.kTestMode == Taas::KV) {
            cnt = 9;
        }
        else return ;

         {
            for (i = 0; i < cnt; i++) {
                auto opType = MultiModelWorkload::operationChooser->nextValue();
                auto id = MultiModelWorkload::keyChooser[0]->nextValue();
                sprintf(genKey, "usertable_key:%064lu", id);
                auto keyName = std::string(genKey);
                proto::Row *row = message_txn->add_row();
                if (opType == Operation::READ) {
                    proto::KvDBRequest request;
                    proto::KvDBResponse response;
                    brpc::Controller cntl;
                    cntl.set_timeout_ms(500);
                    auto data = request.add_data();
                    data->set_op_type(proto::OpType::Read);
                    data->set_key(keyName);
                    get_stub.Get(&cntl, &request, &response, nullptr);
                    if (cntl.Failed()) {
                        // RPC失败.
                        value = "";
                        LOG(WARNING) <<"KV Read OP ERROR : " <<  cntl.ErrorText();
                    } else {
                        value = response.data(0).value();
                    }
                    row->set_op_type(proto::OpType::Read);
                } else {
                    utils::ByteIteratorMap values;
                    MultiModelWorkload::buildValues(values);
                    for (const auto &it: values) {
                        value += it.second + ",";
                    }
                    row->set_op_type(proto::OpType::Update);
                }
                row->set_key(keyName);
                row->set_data(value);
            }
        }
    }

}
