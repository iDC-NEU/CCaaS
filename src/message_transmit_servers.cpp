//
// Created by 周慰星 on 2022/9/17.
//

#include "epoch/epoch_manager.h"
#include "tools/utilities.h"

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
    void SendServerThreadMain(uint64_t id, Context ctx) {
        SetCPU();
        zmq::context_t context(1);
        zmq::message_t reply(5);
        int queue_length = 0;
        std::unordered_map<std::uint64_t, std::unique_ptr<zmq::socket_t>> socket_map;
        std::unique_ptr<send_params> params;
        std::unique_ptr<zmq::message_t> msg;
        for (int i = 0; i < (int) ctx.kServerIp.size(); i++) {
            if (i == (int) ctx.txn_node_ip_index) continue;
            auto socket = std::make_unique<zmq::socket_t>(context, ZMQ_PUSH);
            socket->setsockopt(ZMQ_SNDHWM, &queue_length, sizeof(queue_length));
            socket->setsockopt(ZMQ_RCVHWM, &queue_length, sizeof(queue_length));
            socket->connect("tcp://" + ctx.kServerIp[i] + ":" + std::to_string(20000+i));
            socket_map[i] = std::move(socket);
            printf("Send Server connect ZMQ_PUSH %s", ("tcp://" + ctx.kServerIp[i] + ":" + std::to_string(20000+i) + "\n").c_str());
        }
        printf("线程开始工作 SendServerThread\n");
        while (!init_ok.load());
        while (!EpochManager::IsTimerStop()) {
            send_to_server_queue.wait_dequeue(params);
            if (params == nullptr || params->type == proto::TxnType::NullMark) continue;
            msg = std::make_unique<zmq::message_t>(*(params->str));
            socket_map[params->id]->send(*msg);
        }
        socket_map[id]->send((zmq::message_t &) "end");
    }

/**
 * @brief 监听其他txn node发来的写集，并放在listen_message_queue中
 *
 * @param id 暂时未使用
 * @param ctx XML的配置信息
 */
    void ListenServerThreadMain(uint64_t id, Context ctx) {///监听远端txn node写集
        SetCPU();
        // 设置ZeroMQ的相关变量，监听其他txn node是否有写集发来
        zmq::context_t listen_context(1);
        zmq::socket_t socket_listen(listen_context, ZMQ_PULL);
        int queue_length = 0;
        socket_listen.bind("tcp://*:" + std::to_string(20000+ctx.txn_node_ip_index));//to server
        socket_listen.setsockopt(ZMQ_SNDHWM, &queue_length, sizeof(queue_length));
        socket_listen.setsockopt(ZMQ_RCVHWM, &queue_length, sizeof(queue_length));
        printf("线程开始工作 ListenServerThread ZMQ_PULL tcp://*:%s\n", std::to_string(20000+ctx.txn_node_ip_index).c_str());

        while (!EpochManager::IsTimerStop()) {
            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
            socket_listen.recv(&(*message_ptr));//防止上次遗留消息造成message cache出现问题
            if (is_epoch_advance_started.load()) {
                if (!listen_message_queue.enqueue(std::move(message_ptr))) assert(false);
                if (!listen_message_queue.enqueue(std::move(std::make_unique<zmq::message_t>())))
                    assert(false); //防止moodycamel取不出
                break;
            }
        }

        while (!EpochManager::IsTimerStop()) {
            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
            socket_listen.recv(&(*message_ptr));
            if (!listen_message_queue.enqueue(std::move(message_ptr))) assert(false);
            if (!listen_message_queue.enqueue(std::move(std::make_unique<zmq::message_t>())))
                assert(false); //防止moodycamel取不出
        }
    }
}