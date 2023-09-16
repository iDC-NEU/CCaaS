//
// Created by user on 23-7-17.
//

#ifndef TAAS_TWOPCMESSAGESENDHANDLER_H
#define TAAS_TWOPCMESSAGESENDHANDLER_H

#pragma once

#include "message.h"
#include "epoch/epoch_manager.h"
#include "tools/context.h"

#include "proto/message.pb.h"

namespace Taas {
    class TwoPCMessageSendHandler {
    public:
        static std::atomic<uint64_t> TotalLatency, TotalTxnNum, TotalSuccessTxnNUm, TotalSuccessLatency;
        static bool SendTxnCommitResultToClient(const Context& ctx, std::shared_ptr<proto::Transaction> txn_ptr, proto::TxnState txn_state);
        static bool SendTxnToServer(const Context& ctx, uint64_t& to_whom, std::shared_ptr<proto::Transaction> txn_ptr, proto::TxnType txn_type);
        static bool SendRemoteServerTxn(const Context& ctx, uint64_t& to_whom, std::shared_ptr<proto::Transaction> txn_ptr, proto::TxnType txn_type);
        static bool SendBackUpTxn(const Context &ctx, std::shared_ptr<proto::Transaction> txn_ptr, proto::TxnType txn_type);
        static bool SendACK(const Context &ctx, uint64_t &epoch, uint64_t &to_whom, proto::TxnType txn_type);
        static bool SendMessageToAll(const Context& ctx, proto::TxnType txn_type);

        ///一下函数都由single one线程执行
        static void StaticInit(const Context& ctx);
        static void StaticClear();
        static std::vector<std::unique_ptr<std::atomic<uint64_t>>> sharding_send_epoch, backup_send_epoch, abort_set_send_epoch, insert_set_send_epoch;
        static uint64_t sharding_sent_epoch, backup_sent_epoch, abort_sent_epoch, insert_set_sent_epoch, abort_set_sent_epoch;
        static bool SendEpochEndMessage(const Context& ctx);
        static bool SendBackUpEpochEndMessage(const Context& ctx);
        static bool SendAbortSet(const Context& ctx);
        static bool SendInsertSet(const Context& ctx);



    private:
        bool sleep_flag = false;
        std::unique_ptr<pack_params> pack_param;
    };
}



#endif //TAAS_TWOPCMESSAGESENDHANDLER_H
