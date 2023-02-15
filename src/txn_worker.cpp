//
// Created by 周慰星 on 2022/9/14.
//

#include <queue>
#include <utility>
#include "epoch/merge.h"
#include "epoch/epoch_manager.h"
#include "tools/utilities.h"
//#include "tikv_client/tikv_client.h"

namespace Taas {

//    bool SendToTiKV(tikv_client::TransactionClient &client) {
//        auto txn_ptr = std::make_unique<proto::Transaction>();
//        if(redo_log_queue.try_dequeue(txn_ptr)) {
//            auto tikv_txn = client.begin();
//            printf("开启client的连接，开始向tikv客户端，准备向tikv中写入数据！！！\n");
//            while(redo_log_queue.try_dequeue(txn_ptr)) {
//                for (auto i = 0; i < txn_ptr->row_size(); i++) {
//                    const auto& row = txn_ptr->row(i);
//                    if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
//                        tikv_txn.put(row.key(), row.data());
//                        printf("put key: %s, put value: %s\n", row.key().c_str(), row.data().c_str());
//                    }
//                }
//            }
//            tikv_txn.commit();
//        }
//    }


/**
 * @brief do local_merge remote_merge and commit
 *
 * @param ctx XML中的配置相关信息
 * @return true
 * @return false
 */
    void MergeWorkerThreadMain(uint64_t id, Context ctx) {
        Merger merger;
        merger.Run(id, ctx);
        merger.Init(id, std::move(ctx));

//        tikv_client::TransactionClient client({"172.19.215.168:2379"});

        auto sleep_flag = false;
        while(!EpochManager::IsTimerStop()) {
            sleep_flag = false;

            sleep_flag = sleep_flag | merger.EpochMerge();

            sleep_flag = sleep_flag | merger.EpochCommit_RedoLog_TxnMode();

//            sleep_flag = sleep_flag | SendToTiKV(client);

//            sleep_flag = sleep_flag | merger.EpochCommit_RedoLog_ShardingMode();

            if(!sleep_flag) usleep(200);
        }
    }
}

