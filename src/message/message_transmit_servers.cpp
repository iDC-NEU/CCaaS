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
 * 20000+id : txn nodes sends Shard txns to other txn nodes; txn nodes send raft states   txn PULL           other txn PUSH
 * 21000+id : txn nodes sends Replica txns to other txn nodes; txn nodes send raft states   txn PUB           other txn SUB
 *
 * PUB bind port      SUB connect ip+port

 */

/**
 * @brief 将send_to_server_queue中的数据通过5557端口发送给其他txn node
 *
 * @param id 暂未使用
 * @param ctx 暂未使用
 */
    void SendServerThreadMain() {
        auto server_num = TaasContext::kTxnNodeNum,
                shard_num = TaasContext::kShardNum,
                replica_num = TaasContext::kReplicaNum,
                local_server_id = TaasContext::txn_node_ip_index,
                max_length = TaasContext::kCacheMaxLength;
        zmq::context_t context(1);
        zmq::message_t reply(5);
        zmq::send_flags sendFlags = zmq::send_flags::none;
        int queue_length = 1000000000;
        std::unordered_map<std::uint64_t, std::unique_ptr<zmq::socket_t>> socket_map;
        std::unique_ptr<send_params> params;
        std::unique_ptr<zmq::message_t> msg;
        assert(TaasContext::kServerIp.size() >= TaasContext::kTxnNodeNum);
        for (uint64_t i = 0; i < server_num; i++) {
            if(i == local_server_id) continue;
            auto socket = std::make_unique<zmq::socket_t>(context, ZMQ_PUSH);
            socket->set(zmq::sockopt::sndhwm, queue_length);
            socket->set(zmq::sockopt::rcvhwm, queue_length);
            socket->connect("tcp://" + TaasContext::kServerIp[i] + ":" + std::to_string(20000+i));
            socket_map[i] = std::move(socket);
            printf("Send Server connect ZMQ_PUSH %s", ("tcp://" + TaasContext::kServerIp[i] + ":" + std::to_string(20000+i) + "\n").c_str());
        }
        printf("线程开始工作 SendServerThread\n");


        init_ok_num.fetch_add(1);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while (!EpochManager::IsTimerStop()) {
            MessageQueue::send_to_server_queue->wait_dequeue(params);
            if (params == nullptr || params->type == proto::TxnType::NullMark || params->str == nullptr) continue;
            assert(params->id != TaasContext::txn_node_ip_index);
            assert(params->id < TaasContext::kTxnNodeNum);
            msg = std::make_unique<zmq::message_t>(*(params->str));
            socket_map[params->id]->send(*msg, sendFlags);
        }
        socket_map[0]->send((zmq::message_t &) "end", sendFlags);
    }

    void SendServerPUBThreadMain() {

        auto server_num = TaasContext::kTxnNodeNum,
                shard_num = TaasContext::kShardNum,
                replica_num = TaasContext::kReplicaNum,
                local_server_id = TaasContext::txn_node_ip_index,
                max_length = TaasContext::kCacheMaxLength;

        zmq::context_t context(1);
        zmq::message_t reply(5);
        zmq::send_flags sendFlags = zmq::send_flags::none;

        int queue_length = 1000000000;
        std::unordered_map<std::uint64_t, std::unique_ptr<zmq::socket_t>> socket_map;
        std::unique_ptr<send_params> params;
        std::unique_ptr<zmq::message_t> msg;
        assert(TaasContext::kServerIp.size() >= TaasContext::kTxnNodeNum);
        for (uint64_t i = 0; i < shard_num; i++) {
            auto socket = std::make_unique<zmq::socket_t>(context, ZMQ_PUB);
            socket->set(zmq::sockopt::sndhwm, queue_length);
            socket->set(zmq::sockopt::rcvhwm, queue_length);
            socket->bind("tcp://*:" + std::to_string(21000 + i));
            socket_map[i] = std::move(socket);
            printf("Send Server connect ZMQ_PUB %s", (std::to_string(21000+i) + "\n").c_str());///Shard Replica
        }

        std::unique_ptr<zmq::socket_t> socket_to_all;
        socket_to_all= std::make_unique<zmq::socket_t>(context, ZMQ_PUB);
        socket_to_all->set(zmq::sockopt::sndhwm, queue_length);
        socket_to_all->set(zmq::sockopt::rcvhwm, queue_length);
        socket_to_all->bind("tcp://*:" + std::to_string(22000+TaasContext::txn_node_ip_index));
        printf("Send Server bind ZMQ_PUB %s", ("tcp://*:" + std::to_string(22000+TaasContext::txn_node_ip_index) + "\n").c_str());///ACK

        std::unique_ptr<zmq::socket_t> socket_back_up;
        socket_back_up= std::make_unique<zmq::socket_t>(context, ZMQ_PUB);
        socket_back_up->set(zmq::sockopt::sndhwm, queue_length);
        socket_back_up->set(zmq::sockopt::rcvhwm, queue_length);
        socket_back_up->bind("tcp://*:" + std::to_string(23000+TaasContext::txn_node_ip_index));
        printf("Send Server bind ZMQ_PUB %s", ("tcp://*:" + std::to_string(23000+TaasContext::txn_node_ip_index) + "\n").c_str());///BackUp

        printf("线程开始工作 SendServerThread\n");

        init_ok_num.fetch_add(1);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while (!EpochManager::IsTimerStop()) {
            MessageQueue::send_to_server_pub_queue->wait_dequeue(params);
            if (params == nullptr || params->type == proto::TxnType::NullMark || params->str == nullptr) continue;
            msg = std::make_unique<zmq::message_t>(*(params->str));
            if(params->type == proto::TxnType::BackUpTxn) {
                socket_back_up->send(*msg, sendFlags);
                continue;
            }
            if(params->send_to_all) {
                socket_to_all->send(*msg, sendFlags);
                continue;
            }
            socket_map[params->id]->send(*msg, sendFlags);
        }
        socket_map[0]->send((zmq::message_t &) "end", sendFlags);
    }

/**
 * @brief 监听其他txn node发来的写集，并放在listen_message_queue中
 *
 * @param id 暂时未使用
 * @param ctx XML的配置信息
 */
    void ListenServerThreadMain() {///监听远端txn node写集
        // 设置ZeroMQ的相关变量，监听其他txn node是否有写集发来
        zmq::context_t listen_context(1);
        zmq::recv_flags recvFlags = zmq::recv_flags::none;
        zmq::recv_result_t  recvResult;
        int queue_length = 1000000000;
        zmq::socket_t socket_listen(listen_context, ZMQ_PULL);
        socket_listen.bind("tcp://*:" + std::to_string(20000+TaasContext::txn_node_ip_index));//to server
        socket_listen.set(zmq::sockopt::sndhwm, queue_length);
        socket_listen.set(zmq::sockopt::rcvhwm, queue_length);
        printf("线程开始工作 ListenServerThread ZMQ_PULL tcp://*:%s\n", std::to_string(20000+TaasContext::txn_node_ip_index).c_str());

        init_ok_num.fetch_add(1);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while (!EpochManager::IsTimerStop()) {
            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
            recvResult = socket_listen.recv((*message_ptr), recvFlags);//防止上次遗留消息造成message cache出现问题
            assert(recvResult >= 0);
            if (is_epoch_advance_started.load()) {
                auto res = MessageQueue::listen_message_epoch_queue->enqueue(std::move(message_ptr));
                assert(res);
                res = MessageQueue::listen_message_epoch_queue->enqueue(nullptr);
                assert(res); //防止moodycamel取不出
                break;
            }
        }
        while (!EpochManager::IsTimerStop()) {
            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
            recvResult = socket_listen.recv((*message_ptr), recvFlags);
            assert(recvResult >= 0);
            auto res = MessageQueue::listen_message_epoch_queue->enqueue(std::move(message_ptr));
            assert(res);
            res = MessageQueue::listen_message_epoch_queue->enqueue(nullptr);
            assert(res); //防止moodycamel取不出
        }
    }

    void ListenServerThreadMain_Sub() {///监听远端txn node写集
        auto server_num = TaasContext::kTxnNodeNum,
                shard_num = TaasContext::kShardNum,
                replica_num = TaasContext::kReplicaNum,
                local_server_id = TaasContext::txn_node_ip_index,
                max_length = TaasContext::kCacheMaxLength;
        std::vector<std::vector<bool>> is_local_shard;
        is_local_shard.resize(server_num);
        for(auto &i : is_local_shard) {
            i.resize(shard_num);
        }

        zmq::context_t listen_context(1);
        zmq::recv_flags recvFlags = zmq::recv_flags::none;
        zmq::recv_result_t  recvResult;
        int queue_length = 1000000000;
        zmq::socket_t socket_listen(listen_context, ZMQ_SUB);

          for(uint64_t i = 0; i < shard_num; i ++) {
              if(i % server_num == local_server_id) {
                  for(uint64_t j = 1; j < replica_num; j ++ ) {
                      socket_listen.connect("tcp://" + TaasContext::kServerIp[(i + j) % server_num] + ":"
                                            + std::to_string(21000 + i));  /// shard replica
                      LOG(INFO) << "Server:" << (i + j) % server_num << "Shard: " << i;
                  }
              }
              else {
                  for(uint64_t j = 0; j < replica_num; j ++ ) {
                      if((i + j) % server_num == local_server_id) {
                          for(uint64_t k = 0; k < replica_num; k ++) {
                              if((i + k) % server_num == local_server_id) continue;
                              socket_listen.connect("tcp://" + TaasContext::kServerIp[(i + k) % server_num] + ":"
                                                    + std::to_string(21000 + i));  /// shard replica
                              LOG(INFO) << "Server:" << (i + k) % server_num << "Shard: " << i;
                          }
                      }
                  }
              }
          }

//        for (uint64_t i = 0; i < server_num; i++) {
//            if (i == TaasContext::txn_node_ip_index) continue;
//            for(uint64_t j = 0; j < shard_num; j++) {
//                if(is_local_shard[i][j]) {///shard j send from i is a local shard of current server, then receive the replica
//                    socket_listen.connect("tcp://" + TaasContext::kServerIp[i] + ":" + std::to_string(21000+j));///shard replica
//                    printf("Listen Server connect ZMQ_SUB %s", ("tcp://" + TaasContext::kServerIp[i] + ":" + std::to_string(21000+j) + "\n").c_str());
//                }
//            }
//        }

        for (uint64_t i = 0; i < server_num; i++) {
            if (i == TaasContext::txn_node_ip_index) continue;
            socket_listen.connect("tcp://" + TaasContext::kServerIp[i] + ":" + std::to_string(22000+i));///ACK to sall erver
            printf("Listen Server connect ZMQ_SUB %s", ("tcp://" + TaasContext::kServerIp[i] + ":" + std::to_string(22000+i) + "\n").c_str());
        }

        uint64_t to_id;
        for(uint64_t i = 0; i < TaasContext::kBackUpNum; i++) {
            to_id = (TaasContext::txn_node_ip_index + TaasContext::kTxnNodeNum - i - 1) % TaasContext::kTxnNodeNum;
            if(to_id == TaasContext::txn_node_ip_index) continue;
            socket_listen.connect("tcp://" + TaasContext::kServerIp[to_id] + ":" + std::to_string(23000+to_id));///BackUp
            printf("Listen Server connect ZMQ_SUB %s", ("tcp://" + TaasContext::kServerIp[i] + ":" + std::to_string(23000+to_id) + "\n").c_str());
        }

        socket_listen.set(zmq::sockopt::subscribe,"");
        socket_listen.set(zmq::sockopt::sndhwm, queue_length);
        socket_listen.set(zmq::sockopt::rcvhwm, queue_length);
        printf("线程开始工作 ListenServerThread ZMQ_SUB\n");

        init_ok_num.fetch_add(1);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while (!EpochManager::IsTimerStop()) {
            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
            recvResult = socket_listen.recv((*message_ptr), recvFlags);//防止上次遗留消息造成message cache出现问题
//            LOG(INFO) << "receive a message";
            assert(recvResult >= 0);
            if (is_epoch_advance_started.load()) {
                auto res = MessageQueue::listen_message_epoch_queue->enqueue(std::move(message_ptr));
                assert(res);
                res = MessageQueue::listen_message_epoch_queue->enqueue(nullptr);
                assert(res); //防止moodycamel取不出
                break;
            }
        }

        while (!EpochManager::IsTimerStop()) {
            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
            recvResult = socket_listen.recv((*message_ptr), recvFlags);
//            LOG(INFO) << "receive a message";
            assert(recvResult >= 0);
            auto res = MessageQueue::listen_message_epoch_queue->enqueue(std::move(message_ptr));
            assert(res);
            res = MessageQueue::listen_message_epoch_queue->enqueue(nullptr);
            assert(res); //防止moodycamel取不出
        }
    }
}