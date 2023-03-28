//
// Created by user on 23-3-26.
//

#include <queue>
#include "epoch/epoch_manager.h"
#include "storage/tikv.h"
#include "tools/utilities.h"
#include "tikv_client.h"

bool sendTransactionToTiKV(uint64_t epoch_mod, std::unique_ptr<proto::Transaction> &txn_ptr) {
    ///todo 检查push down的epoch
    if(Taas::epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr) && txn_ptr != nullptr) {
        if(Taas::EpochManager::tikv_client_ptr == nullptr) return true;
        auto tikv_txn = Taas::EpochManager::tikv_client_ptr->begin();
        for (auto i = 0; i < txn_ptr->row_size(); i++) {
            const auto& row = txn_ptr->row(i);
            if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
                tikv_txn.put(row.key(), row.data());
            }
        }
        tikv_txn.commit();
        return true;
    }
    else {
        return false;
    }
}