//
// Created by 周慰星 on 11/9/22.
//

#ifndef TAAS_HANDLER_SEND_H
#define TAAS_HANDLER_SEND_H
#include "proto/message.pb.h"
#include "message.h"
#include "epoch/epoch_manager.h"
#include "tools/context.h"


namespace Taas {

    class MessageSendHandler {
    public:
        static bool SendTxnCommitResultToClient(Context& ctx, proto::Transaction& txn, proto::TxnState txn_state);
        static bool SendTxnToServer(Context& ctx, uint64_t &epoch, uint64_t& to_whom, proto::Transaction &txn, proto::TxnType txn_type);
        static bool SendRemoteServerTxn(Context& ctx, uint64_t &epoch, uint64_t& to_whom, proto::Transaction &txn, proto::TxnType& txn_type);
        static bool SendBackUpTxn(Context &ctx, uint64_t &epoch, proto::Transaction &txn, proto::TxnType &txn_type);
        static bool SendACK(Context& ctx, uint64_t &epoch, uint64_t& to_whom, proto::Transaction &txn, proto::TxnType& txn_type);
        static bool SendMessageToAll(Context &ctx, uint64_t &epoch, proto::TxnType &txn_type);

        ///一下函数都由single one线程执行
        static void StaticInit(Context &ctx);
        static void StaticClear(uint64_t &epoch, Context &ctx);
        static std::vector<std::vector<std::unique_ptr<std::atomic<bool>>>> sharding_send_epoch;
        static std::vector<std::unique_ptr<std::atomic<bool>>> backup_send_epoch, abort_set_send_epoch, insert_set_send_epoch;
        static uint64_t sharding_sent_epoch, backup_sent_epoch, abort_sent_epoch, insert_set_sent_epoch, abort_set_sent_epoch;
        static bool SendEpochEndMessage(Context &ctx);
        static bool SendBackUpEpochEndMessage(Context &ctx);
        static bool SendAbortSet(Context &ctx);
        static bool SendInsertSet(Context &ctx);



    private:
        bool sleep_flag = false;
        std::unique_ptr<pack_params> pack_param;
    };
}
#endif //TAAS_HANDLER_SEND_H
