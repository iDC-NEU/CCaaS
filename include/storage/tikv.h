//
// Created by user on 23-3-26.
//

#ifndef TAAS_TIKV_H
#define TAAS_TIKV_H

#include <proto/transaction.pb.h>
#include "tikv_client.h"

namespace Taas {
    class TiKV {
    public:
        static tikv_client::TransactionClient* tikv_client_ptr;
        static bool sendTransactionToTiKV(uint64_t epoch_mod, std::unique_ptr<proto::Transaction> &txn_ptr);
    };
}





#endif //TAAS_TIKV_H
