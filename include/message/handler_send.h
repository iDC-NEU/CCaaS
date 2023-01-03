//
// Created by 周慰星 on 11/9/22.
//

#ifndef TAAS_HANDLER_SEND_H
#define TAAS_HANDLER_SEND_H
#include "epoch/epoch_manager.h"
#include "utils/context.h"
namespace Taas {

    class MessageSendHandler {
    public:
        static bool ReplyTxnStateToClient(Context& ctx, proto::Transaction& txn, proto::TxnState txn_state);
        static bool SendTxnToPack(Context& ctx, proto::Transaction& txn, proto::TxnType txn_type);
        static bool SendEpochEndMessage(uint64_t& id, Context& ctx, std::vector<uint64_t>& send_epoch);
        static bool SendBackUpEpochEndMessage(uint64_t& id, Context& ctx, uint64_t& send_epoch);
        static bool SendTxn(uint64_t& id, Context& ctx);
    private:
    };
}
#endif //TAAS_HANDLER_SEND_H
