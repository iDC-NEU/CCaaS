//
// Created by zwx on 23-8-23.
//

#ifndef KVPair_H
#define KVPair_H

#include "txn_type.h"

namespace workload {
    class KV {
    public:
        static std::vector<KVTxn > txnVector;
        static void InsertData(uint64_t tid);
        static void GenerateTxn(uint64_t tid);
        static void RunTxn(const std::vector<workload::KeyValue>& txn, proto::Transaction* message_txn);
    };

}
#endif //KVPair_H
