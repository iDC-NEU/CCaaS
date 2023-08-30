//
// Created by zwx on 23-8-29.
//

#include "tools/utilities.h"
#include "workload/kv.h"
#include "workload/multi_model_workload.h"

#include "proto/message.pb.h"

namespace workload {
    std::vector<KVTxn > KV::txnVector;

    void KV::InsertData(uint64_t tid) {
        uint64_t num = MultiModelWorkload::ctx.multiModelContext.kOpNum;
        uint64_t randnum = Taas::RandomNumber(0, 100);
        KVTxn res;
        if (randnum < MultiModelWorkload::ctx.multiModelContext.kWriteNum) {
            while (num--) {
                auto key = Taas::RandomString(20);
                auto value = Taas::RandomString(20);
                res.emplace_back(key, value);
            }
        } else {
            while (num--) {
                auto key = Taas::RandomString(20);
                res.emplace_back(key);
            }
        }
        res.emplace_back("kvid" + std::to_string(tid), "kvid" + std::to_string(tid));
        txnVector.push_back(std::move(res));
    }

    void KV::GenerateTxn(uint64_t tid) {
        uint64_t num = MultiModelWorkload::ctx.multiModelContext.kOpNum;
        uint64_t randnum = Taas::RandomNumber(0, 100);
        KVTxn res;
        if (randnum < MultiModelWorkload::ctx.multiModelContext.kWriteNum) {
            while (num--) {
                auto key = Taas::RandomString(20);
                auto value = Taas::RandomString(20);
                res.emplace_back(key, value);
            }
        } else {
            while (num--) {
                auto key = Taas::RandomString(20);
                res.emplace_back(key);
            }
        }
        res.emplace_back("kvid" + std::to_string(tid), "kvid" + std::to_string(tid));
        txnVector.push_back(std::move(res));
    }

    void KV::RunTxn(const std::vector<workload::KeyValue>& txn, proto::Transaction* message_txn) {
        for (const auto& p: txn) {
            proto::Row *row = message_txn->add_row();
            std::string key = p.key;
            std::string data = p.value;
            row->set_key(key);
            row->set_data(data);
            if (data.empty())row->set_op_type(proto::OpType::Read);
            else row->set_op_type(proto::OpType::Insert);
        }
    }



}