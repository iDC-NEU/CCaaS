//
// Created by zwx on 23-8-23.
//

#ifndef KVPair_H
#define KVPair_H

#include "txn_type.h"

#include <proto/kvdb_server.pb.h>

namespace workload {
    class KV {
    public:
        static std::string GenerateKey(uint64_t key_id);
        static std::string GenerateValue();
        static void InsertData(uint64_t tid);
        static void GenerateTxn(uint64_t tid);
        static void RunTxn(proto::Transaction* message_txn, proto::KvDBGetService_Stub& get_stub);
    };

}
#endif //KVPair_H
