//
// Created by 周慰星 on 11/9/22.
//

#ifndef TAAS_HANDLER_SEND_H
#define TAAS_HANDLER_SEND_H
#include "epoch/epoch_manager.h"
#include "tools/context.h"
namespace Taas {

    class MessageSendHandler {
    public:
        static bool SendTxnCommitResultToClient(Context& ctx, proto::Transaction& txn, proto::TxnState txn_state);
        static bool SendTaskToPackThread(Context& ctx, uint64_t &epoch, uint64_t to_whom, proto::TxnType txn_type);
        static bool SendTxnToPackThread(Context& ctx, proto::Transaction& txn, proto::TxnType txn_type);

        static bool HandlerSendTask(uint64_t& id, Context& ctx);

        static bool SendEpochEndMessage(uint64_t& id, Context& ctx, std::vector<uint64_t>& send_epoch);
        static bool SendBackUpEpochEndMessage(uint64_t& id, Context& ctx, uint64_t& send_epoch);
        static bool SendAbortSet(uint64_t& id, Context& ctx, uint64_t& send_epoch);
        static bool SendInsertSet(uint64_t& id, Context& ctx, uint64_t& send_epoch);
        static bool SendACK(uint64_t &id, Context &ctx, uint64_t &send_epoch, uint64_t to_whom, proto::TxnType txn_type);

    private:
        static AtomicCounters_Cache ///epoch, server_id, num
            sharding_send_txn_num, backup_send_txn_num;

    };
}
#endif //TAAS_HANDLER_SEND_H
