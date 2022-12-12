//
// Created by 周慰星 on 11/9/22.
//

#ifndef TAAS_TRANSMIT_H
#define TAAS_TRANSMIT_H
#include "epoch/epoch_manager.h"
#include "utils/context.h"
namespace Taas {

    class MessageTransmitter {
    public:
        static bool ReplyTxnStateToClient(Context& ctx, proto::Transaction& txn, proto::TxnState txn_state);
        static bool SendTxnToPack(Context& ctx, proto::Transaction& txn);
        static bool SendEpochEndMessage(uint64_t& id, Context& ctx, uint64_t& send_epoch);
        static bool SendEpochSerializedTxn(uint64_t& id, Context& ctx, uint64_t& send_epoch, std::unique_ptr<pack_params>& pack_param);
    private:
    };
}
#endif //TAAS_TRANSMIT_H
