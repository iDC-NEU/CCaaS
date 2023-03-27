//
// Created by user on 23-3-26.
//

#ifndef TAAS_TIKV_H
#define TAAS_TIKV_H


class tikv {

};

bool sendTransactionToTiKV(uint64_t epoch_mod, std::unique_ptr<proto::Transaction> &txn_ptr);


#endif //TAAS_TIKV_H
