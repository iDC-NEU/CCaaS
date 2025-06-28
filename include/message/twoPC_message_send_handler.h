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
        static bool SendTxnCommitResultToClient(const std::shared_ptr<proto::Transaction>& txn_ptr, proto::TxnState txn_state);
        static bool SendTxnToServer(uint64_t& to_whom, const std::shared_ptr<proto::Transaction>& txn_ptr, proto::TxnType txn_type);
        static bool SendRemoteServerTxn(uint64_t& to_whom, const std::shared_ptr<proto::Transaction>& txn_ptr, proto::TxnType txn_type);
        static bool SendBackUpTxn(std::shared_ptr<proto::Transaction> txn_ptr, proto::TxnType txn_type);
        static bool SendACK(uint64_t &epoch, uint64_t &to_whom, proto::TxnType txn_type);
        static bool SendMessageToAll(proto::TxnType txn_type);

        ///一下函数都由single one线程执行
        static void StaticInit();
        static void StaticClear();
        static std::vector<std::unique_ptr<std::atomic<uint64_t>>> shard_send_epoch, backup_send_epoch, abort_set_send_epoch, insert_set_send_epoch;
        static uint64_t shard_sent_epoch, backup_sent_epoch, abort_sent_epoch, insert_set_sent_epoch, abort_set_sent_epoch;
        static bool SendEpochEndMessage();
        static bool SendBackUpEpochEndMessage();
        static bool SendAbortSet();
        static bool SendInsertSet();



    private:
        bool sleep_flag = false;
        std::unique_ptr<pack_params> pack_param;
    };
}



#endif //TAAS_TWOPCMESSAGESENDHANDLER_H
