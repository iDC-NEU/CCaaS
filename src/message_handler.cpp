//
// Created by 周慰星 on 2022/9/14.
//

#include "epoch/epoch_manager.h"
#include "utils/utilities.h"
#include "queue"
#include "message/handler.h"
#include "message/transmit.h"
using namespace std;
///监听sql node写集 或使用同一个线程监听，在后面handler中进行分类处理。
namespace Taas {

/**
 * @brief 打包线程，从pack_txn_queue中获取txn，并放到send_to_server_queue中
 *
 * @param id
 * @param ctx
 */
    void PackThreadMain(uint64_t id, Context ctx) { /// 线程数 2-4个
        SetCPU();
        auto sleep_flag = false;
        uint64_t send_epoch = 1;
        std::unique_ptr<pack_params> pack_param;
        while (!init_ok.load()) usleep(100);
        printf("PackThreadMain start %llu\n", id);
        if (ctx.kTxnNodeNum == 1) {
            while (!EpochManager::IsTimerStop()) {
                pack_txn_queue.wait_dequeue(pack_param);
            }
        }
        while (!EpochManager::IsTimerStop()) {
            while(pack_txn_queue.try_dequeue(pack_param)) {
                if(pack_param == nullptr || pack_param->str == nullptr) continue;
                if((EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength) ==  ((EpochManager::GetPhysicalEpoch() + 2) % ctx.kCacheMaxLength) ) assert(false);
                if(!send_to_server_queue.enqueue(std::move(
                        std::make_unique<send_params>(0, 0, "", std::move(pack_param->str)))) ) assert(false);
                if(!send_to_server_queue.enqueue(std::move(
                        std::make_unique<send_params>(0, 0, "", nullptr))) )  assert(false); //防止moodycamel取不出
                EpochManager::local_packed_txn_num.IncCount(pack_param->epoch, id, 1);
            }
            if(id == 0) sleep_flag = MessageTransmitter::SendEpochEndMessage(id, ctx, send_epoch);
            if (sleep_flag)
                usleep(200);
        }
    }

///目前实现的是对远端txn node传递过来的写集进行缓存处理，只想merge_queue中放入当前epoch的写集。
///来自sql node传递过来的写集的缓存处理
    void MessageCacheThreadMain(const uint64_t id, Context ctx) {//处理写集，远端，本地写集需要给写集附加epoch和csn。
        SetCPU();
        MessageHandler message_handler;
        message_handler.Init(id, ctx);
        printf("CacheThreadMain start %lu txn_node_ip_index %lu\n", id, ctx.txn_node_ip_index);
        while (!init_ok.load()) usleep(100);
        auto sleep_flag = false;
        while (!EpochManager::IsTimerStop()) {
            sleep_flag = false;
            sleep_flag = sleep_flag | message_handler.HandleReceivedMessage();

            sleep_flag = sleep_flag | message_handler.HandleLocalMergedTxn();

            sleep_flag = sleep_flag | message_handler.HandleTxnCache();

            if (!sleep_flag) {
                usleep(200);
            }
        }
    }
}