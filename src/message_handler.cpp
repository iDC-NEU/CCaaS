//
// Created by 周慰星 on 2022/9/14.
//

#include "epoch/epoch_manager.h"
#include "tools/utilities.h"
#include "queue"
#include "message/handler_receive.h"
#include "message/handler_send.h"
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
        printf("MessageSendHandler PackThreadMain start %llu\n", id);
        MessageSendHandler::HandlerSendTask(id, ctx);
    }

///目前实现的是对远端txn node传递过来的写集进行缓存处理，只想merge_queue中放入当前epoch的写集。
///来自sql node传递过来的写集的缓存处理
    void MessageCacheThreadMain(const uint64_t id, Context ctx) {//处理写集，远端，本地写集需要给写集附加epoch和csn。
        SetCPU();
        MessageReceiveHandler message_receive_handler;
        message_receive_handler.Init(id, ctx);
        printf("MessageReceiveHandler CacheThreadMain start %lu txn_node_ip_index %lu\n", id, ctx.txn_node_ip_index);
        while (!init_ok.load()) usleep(100);
        auto sleep_flag = false;
        if(id == 0) {
            while (!EpochManager::IsTimerStop()) {
                sleep_flag = false;
                sleep_flag = sleep_flag | message_receive_handler.HandleReceivedMessage();

                sleep_flag = sleep_flag | message_receive_handler.HandleTxnCache();

                sleep_flag = sleep_flag | message_receive_handler.CheckReceivedStatesAndReply(); /// 该函数只由handler的第一个线程调用，防止多次发送ACK

                sleep_flag = sleep_flag | message_receive_handler.ClearStaticCounters(); /// 该函数只由handler的第一个线程调用，防止多次发送ACK

                if (!sleep_flag) {
                    usleep(200);
                }
            }
        }
        else {
            while (!EpochManager::IsTimerStop()) {
                sleep_flag = false;
                sleep_flag = sleep_flag | message_receive_handler.HandleReceivedMessage();

                sleep_flag = sleep_flag | message_receive_handler.HandleTxnCache();

                if (!sleep_flag) {
                    usleep(200);
                }
            }
        }
    }
}