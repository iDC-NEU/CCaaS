//
// Created by user on 23-3-26.
//

#include <queue>
#include "epoch/epoch_manager.h"
#include "storage/tikv.h"
#include "tikv_client.h"
#include "storage/redo_loger.h"

namespace Taas {
    tikv_client::TransactionClient* TiKV::tikv_client_ptr = nullptr;

    bool TiKV::sendTransactionToTiKV(uint64_t epoch_mod, std::unique_ptr<proto::Transaction> &txn_ptr) {
        if(!RedoLoger::epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr) || txn_ptr == nullptr) {
            return false;
        }
        else {
            if(tikv_client_ptr == nullptr) return true;
            auto tikv_txn = tikv_client_ptr->begin();
            for (auto i = 0; i < txn_ptr->row_size(); i++) {
                const auto& row = txn_ptr->row(i);
                if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
                    tikv_txn.put(row.key(), row.data());
                }
            }
            tikv_txn.commit();
            return true;
        }
    }
}
