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
        static void InsertData(uint64_t tid);
        static void RunTxn(proto::Transaction* message_txn);
    };

}
#endif //KVPair_H
