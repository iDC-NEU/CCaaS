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
        static bool SendTxnToServer(Context& ctx, uint64_t &epoch, uint64_t& to_whom, proto::Transaction &txn, proto::TxnType txn_type);
        static bool SendRemoteServerTxn(Context& ctx, uint64_t &epoch, uint64_t& to_whom, proto::Transaction &txn, proto::TxnType& txn_type);
        static bool SendBackUpTxn(Context& ctx, uint64_t &epoch, uint64_t& to_whom, proto::Transaction &txn, proto::TxnType& txn_type);
        static bool SendACK(Context& ctx, uint64_t &epoch, uint64_t& to_whom, proto::Transaction &txn, proto::TxnType& txn_type);

        void Init(uint64_t &id, Context& ctx);

        ///一下函数都由0号线程执行
        bool SendEpochEndMessage(uint64_t& id, Context& ctx);
        bool SendBackUpEpochEndMessage(uint64_t& id, Context& ctx);
        bool SendAbortSet(uint64_t& id, Context& ctx);
        bool SendInsertSet(uint64_t& id, Context& ctx);


    private:
        uint64_t backup_send_epoch = 1, abort_set_send_epoch = 1, insert_set_send_epoch = 1;
        std::vector<uint64_t> sharding_send_epoch;
        bool sleep_flag = false;
        std::unique_ptr<pack_params> pack_param;


    };
}
#endif //TAAS_HANDLER_SEND_H
