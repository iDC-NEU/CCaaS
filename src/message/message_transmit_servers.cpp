//
// Created by 周慰星 on 2022/9/17.
//

#include "epoch/epoch_manager.h"
#include "message/message.h"
#include "tools/utilities.h"
#include "tools/zeromq.h"

namespace Taas {

/**
 * port status:                                                                     PULL bind *:port   PUSH connect ip+port
 * 5557 :
 * 20000+id : txn nodes sends txns to other txn nodes; txn nodes send raft states   txn PULL           other txn PUSH
 *
 * PUB bind port      SUB connect ip+port

 */

/**
 * @brief 将send_to_server_queue中的数据通过5557端口发送给其他txn node
 *
 * @param id 暂未使用
 * @param ctx 暂未使用
 */
    void SendServerThreadMain(Context ctx) {
        SetCPU();
        zmq::context_t context(1);
        zmq::message_t reply(5);
        zmq::send_flags sendFlags = zmq::send_flags::none;
        int queue_length = 0;
        std::unordered_map<std::uint64_t, std::unique_ptr<util::ZMQInstance>> socket_map;
        std::unique_ptr<send_params> params;
        for (int i = 0; i < (int) ctx.kServerIp.size(); i++) {
            if (i == (int) ctx.txn_node_ip_index) continue;
            auto ret = util::ZMQInstance::NewClient<zmq::socket_type::push>(ctx.kServerIp[i], 20000+i);
            CHECK(ret != nullptr);
            socket_map[i] = std::move(ret);
            printf("Send Server connect ZMQ_PUSH %s", ("tcp://" + ctx.kServerIp[i] + ":" + std::to_string(20000+i) + "\n").c_str());
        }
        printf("线程开始工作 SendServerThread\n");
        while(!EpochManager::IsInitOK()) usleep(1000);
        while (!EpochManager::IsTimerStop()) {
            send_to_server_queue->wait_dequeue(params);
            if (params == nullptr || params->type == proto::TxnType::NullMark) continue;
            if(params->id == ctx.txn_node_ip_index) assert(false);
            printf("send a message type %d\n", (params->type));
            socket_map[params->id]->send(std::move(*(params->str)));
        }
    }

/**
 * @brief 监听其他txn node发来的写集，并放在listen_message_queue中
 *
 * @param id 暂时未使用
 * @param ctx XML的配置信息
 */
    void ListenServerThreadMain(const Context& ctx) {///监听远端txn node写集
        SetCPU();
        // 设置ZeroMQ的相关变量，监听其他txn node是否有写集发来
        zmq::context_t listen_context(1);
        zmq::recv_flags recvFlags = zmq::recv_flags::none;
        zmq::recv_result_t  recvResult;
        int queue_length = 0;
        auto server = util::ZMQInstance::NewServer<zmq::socket_type::pull>(20000+ctx.txn_node_ip_index);
        CHECK(server != nullptr);
        printf("线程开始工作 ListenServerThread ZMQ_PULL tcp://*:%s\n", std::to_string(20000+ctx.txn_node_ip_index).c_str());
        while(!EpochManager::IsInitOK()) usleep(1000);
        while (!EpochManager::IsTimerStop()) {
            auto ret = server->receive();
            CHECK(ret != std::nullopt);
            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>(std::move(*ret));
            printf("receive a message\n");
            if(recvResult < 0) assert(false);
            if (is_epoch_advance_started.load()) {
                if (!listen_message_queue->enqueue(std::move(message_ptr))) assert(false);
                if (!listen_message_queue->enqueue(std::make_unique<zmq::message_t>()))
                    assert(false); //防止moodycamel取不出
                break;
            }
        }

        while (!EpochManager::IsTimerStop()) {
            auto ret = server->receive();
            CHECK(ret != std::nullopt);
            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>(std::move(*ret));
            if(recvResult < 0) assert(false);
            printf("receive a message\n");
            if (!listen_message_queue->enqueue(std::move(message_ptr))) assert(false);
            if (!listen_message_queue->enqueue(std::make_unique<zmq::message_t>()))
                assert(false); //防止moodycamel取不出
        }
    }
}