//
// Created by 周慰星 on 2022/9/17.
//

#include "epoch/epoch_manager.h"
#include "tools/utilities.h"

namespace Taas {

/**
 * port status:                                                                     PULL bind *:port   PUSH connect ip+port
 *
 * 5551 : client sends txns to txn node                                             client  PUSH       txn PULL
 * 5552 : txn node sends txn_state to client                                        client  PULL       txn PUSH

 */

/**
 * @brief 监听client，并接受client发来的写集，并发到listen_message_queue中
 *
 * @param id 暂时未使用
 * @param ctx 暂时未使用
 */
    void ListenClientThreadMain(Context ctx) {///监听client 写集
        SetCPU();
        // 设置ZeroMQ的相关变量，并监听5555端口，接受client发来的写集
        int queue_length = 0;
        zmq::context_t listen_context(1);
        zmq::socket_t socket_listen(listen_context, ZMQ_PULL);
        zmq::recv_flags recvFlags = zmq::recv_flags::none;
        zmq::recv_result_t recvResult;
        socket_listen.set(zmq::sockopt::sndhwm, queue_length);
        socket_listen.set(zmq::sockopt::rcvhwm, queue_length);
        socket_listen.bind("tcp://*:5551");
        printf("线程开始工作 ListenClientThread ZMQ_PULL tcp://*:5551\n");

        while (!EpochManager::IsTimerStop()) {
            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
            recvResult = socket_listen.recv((*message_ptr), recvFlags);//防止上次遗留消息造成message cache出现问题
            if(recvResult == -1) assert(false);
            if (is_epoch_advance_started.load()) {
                if (!listen_message_queue->enqueue(std::move(message_ptr))) assert(false);
                if (!listen_message_queue->enqueue(std::make_unique<zmq::message_t>()))
                    assert(false); //防止moodycamel取不出
                break;
            }
        }

        while (!EpochManager::IsTimerStop()) {
            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
            recvResult = socket_listen.recv((*message_ptr), recvFlags);
            if(recvResult == -1) assert(false);
            if (!listen_message_queue->enqueue(std::move(message_ptr))) assert(false);
            if (!listen_message_queue->enqueue(std::make_unique<zmq::message_t>()))
                assert(false); //防止moodycamel取不出
        }
    }

/**
 * @brief 将send_to_client_queue中的Reply消息发送给client
 *
 * @param id
 * @param ctx
 */
    void SendClientThreadMain(Context ctx) {
        SetCPU();
        // 设置ZeroMQ的相关变量，通过5556端口发送Reply给client
        zmq::context_t context(1);
        zmq::send_flags sendFlags = zmq::send_flags::none;
        zmq::send_result_t sendResult;
        int queue_length = 0;
        std::unique_ptr<send_params> params;
        std::unique_ptr<zmq::message_t> msg;
        printf("线程开始工作 SendClientThread ZMQ_PUSH tcp://ip+:5552 \n");
        while (!init_ok.load()) usleep(200);
        std::unordered_map<std::string, std::unique_ptr<zmq::socket_t>> socket_map;
        // 测试用，如果设置了会丢弃发送给client的Reply
        if (ctx.kTestClientNum > 0) {
            while (!EpochManager::IsTimerStop()) {
                send_to_client_queue->wait_dequeue(params);
            }
        } else {
//         使用ZeroMQ发送Reply给client
            while(!EpochManager::IsTimerStop()){
                send_to_client_queue->wait_dequeue(params);
                if(params == nullptr || params->type == proto::TxnType::NullMark) continue;
                msg = std::make_unique<zmq::message_t>(*(params->str));
                auto key = "tcp://" + params->ip;
                if(socket_map.find(key) != socket_map.end()) {
                    socket_map[key]->send(*(msg), sendFlags);
                }
                else {
                    auto socket = std::make_unique<zmq::socket_t>(context, ZMQ_PUSH);
                    socket->set(zmq::sockopt::sndhwm, queue_length);
                    socket->set(zmq::sockopt::rcvhwm, queue_length);
                    socket->connect("tcp://" + params->ip + ":5552");
                    socket_map[key] = std::move(socket);
                    socket_map[key]->send(*(msg), sendFlags);
                }
            }
//==========================PUB==========================
//            zmq::socket_t socket_send(context, ZMQ_PUB);
//            socket_send.setsockopt(ZMQ_SNDHWM, &queue_length, sizeof(queue_length));
//            socket->setsockopt(ZMQ_SNDHWM, &queue_length, sizeof(queue_length));
//            socket->setsockopt(ZMQ_RCVHWM, &queue_length, sizeof(queue_length));
//            socket_send.bind("tcp://*:5552");
//            while (!EpochManager::IsTimerStop()) {
//                send_to_client_queue.wait_dequeue(params);
//                if (params == nullptr || params->merge_request_ptr == nullptr) continue;
//                msg = std::make_unique<zmq::message_t>(*(params->merge_request_ptr));
//                socket_send.send(*(msg));
////            printf("txn time: %lu, id: %lu \n",now_to_us() - params->time, params->id);
//            }
        }
    }
}