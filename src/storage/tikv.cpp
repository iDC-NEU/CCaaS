//
// Created by user on 23-3-26.
//

#include <queue>
#include <utility>
#include "epoch/merge.h"
#include "epoch/epoch_manager.h"
#include "storage/tikv.h"
#include "tools/utilities.h"
#include "tikv_client.h"

bool sendTransactionToTiKV() {
    auto txn_ptr = std::make_unique<proto::Transaction>();
    if(Taas::redo_log_queue.try_dequeue(txn_ptr) && txn_ptr != nullptr) {
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