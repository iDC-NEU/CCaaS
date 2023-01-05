//
// Created by 周慰星 on 2022/9/17.
//

#include "epoch/epoch_manager.h"
#include "tools/utilities.h"

namespace Taas {

/**
 * port status:                                                                     PULL bind *:port   PUSH connect ip+port

 * 5553 : storage sends pull log request to txn node                                storage PUSH       txn PULL
 * 5554 : txn node sends pull_response to storage node                              storage PULL       txn PUSH
 *
 * 5555 :
 * 5556 : txn nodes sends log to storage nodes                                      txn PUSH            storage PULL

 */

//    void ListenStorageThreadMain(uint64_t id, Context ctx);//PUSH PULL
//    void SendStoragePUBThreadMain(uint64_t id, Context ctx); //PUSH txn
//    void SendStoragePUBThreadMain2(uint64_t id, Context ctx); //PUSH pack txn

    void ListenStorageThreadMain(uint64_t id, Context ctx) { //PULL & PUSH
        uint32_t recv_port = 5553;
        UNUSED_VALUE(id);
        UNUSED_VALUE(ctx);
        zmq::context_t context(1);
        zmq::socket_t recv_socket(context, ZMQ_PULL);
        zmq::socket_t send_socket(context, ZMQ_PUSH);
        recv_socket.bind("tcp://*:" + std::to_string(recv_port));

        while (!EpochManager::IsTimerStop()) {
            std::unique_ptr<zmq::message_t> recv_message = std::make_unique<zmq::message_t>();
            recv_socket.recv(&(*recv_message));
            auto message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(recv_message->data()),
                                                                    recv_message->size());
            auto pull_msg = std::make_unique<proto::Message>();
            auto res = UnGzip(pull_msg.get(), message_string_ptr.get());
            assert(res);
            auto &pull_req = pull_msg->storage_pull_request();
            uint64_t epoch_id = pull_req.epoch_id();
//        std::string endpoint = "tcp://" + pull_req.send_node().ip() + std::to_string(pull_req.send_node().port());
            auto endpoint = "tcp://" + pull_req.send_node().ip() + ":5554";
            send_socket.connect(endpoint);

            auto pull_msg_resp = std::make_unique<proto::Message>();
            auto pull_resp = pull_msg_resp->mutable_storage_pull_response();

            // load corresponding epoch's all txns
            if (EpochManager::committed_txn_num.GetCount(epoch_id) ==
                EpochManager::should_commit_txn_num.GetCount(epoch_id)) { // the epoch's txns all have been c committed
                auto s = std::to_string(epoch_id) + ":";
                auto epoch_mod = epoch_id % EpochManager::max_length;
                auto total_num = EpochManager::epoch_log_lsn.GetCount(epoch_id);
                for (int i = 0; i < total_num; i++) {
                    auto key = s + std::to_string(i);
                    auto *ptr = pull_resp->add_txns();
                    EpochManager::committed_txn_cache[epoch_mod]->getValue(key, (*ptr)); //copy
                }
//            for (auto iter = EpochManager::redo_log[epoch_id]->begin(); iter != EpochManager::redo_log[epoch_id]->end(); iter++) {
//                proto::Transaction* ptr = pull_resp->add_txns();
//                ptr->CopyFrom(*iter);
//            }
                pull_resp->set_result(proto::Success);
                pull_resp->set_epoch_id(epoch_id);
            } else {
                pull_resp->set_result(proto::Fail);
            }

//        std::unique_ptr<std::string> serialized_pull_resp = std::make_unique<std::string>();
//        pull_resp->SerializeToString(&(*serialized_pull_resp));
            auto serialized_pull_resp = std::make_unique<std::string>();
            res = Gzip(pull_msg_resp.get(), serialized_pull_resp.get());
            std::unique_ptr<zmq::message_t> send_message = std::make_unique<zmq::message_t>(*serialized_pull_resp);
            send_socket.send(*send_message);
            send_socket.disconnect(endpoint);
        }

    }

    void SendStoragePUBThreadMain(uint64_t id, Context ctx) { //PUB Txn
        zmq::context_t context(1);
        zmq::message_t reply(5);
        zmq::socket_t socket_send(context, ZMQ_PUB);
        socket_send.bind("tcp://*:5556");//to server
        printf("线程开始工作 SendStoragePUBServerThread ZMQ_PUB tcp:// ip + :5556\n");
        std::unique_ptr<send_params> params;
        std::unique_ptr<zmq::message_t> msg;
        while (init_ok.load() == false);
        while (!EpochManager::IsTimerStop()) {
            send_to_storage_queue.wait_dequeue(params);
            if (params == nullptr || params->merge_request_ptr == nullptr) continue;
//        msg = std::make_unique<zmq::message_t>(static_cast<void*>(const_cast<char*>(params->merge_request_ptr->data())),
//                                               params->merge_request_ptr->size(), string_free, static_cast<void*>(&(params->merge_request_ptr)));
            msg = std::make_unique<zmq::message_t>(*(params->merge_request_ptr));
            socket_send.send(*msg);
        }
        socket_send.send((zmq::message_t &) "end");
    }

    void SendStoragePUBThreadMain2(uint64_t id, Context ctx) {//PUB PACK
        SetCPU();
        zmq::context_t context(1);
        zmq::message_t reply(5);
        zmq::socket_t socket_send(context, ZMQ_PUB);
        int queue_length = 0;
        socket_send.setsockopt(ZMQ_SNDHWM, &queue_length, sizeof(queue_length));
        socket_send.bind("tcp://*:5556");//to server
        printf("线程开始工作 SendStorage PUBServerThread ZMQ_PUB tcp:// ip + :5556\n");
        std::unique_ptr<send_params> params;
        std::unique_ptr<zmq::message_t> msg;
        uint64_t epoch = 1;
        while (init_ok.load() == false);
        while (!EpochManager::IsTimerStop()) {
            if (epoch < EpochManager::GetLogicalEpoch()) {
                auto push_msg = std::make_unique<proto::Message>();
                auto push_response = push_msg->mutable_storage_push_response();
                assert(push_response != nullptr);
                auto s = std::to_string(epoch) + ":";
                auto epoch_mod = epoch % EpochManager::max_length;
                auto total_num = EpochManager::epoch_log_lsn.GetCount(epoch);
                for (int i = 0; i < total_num; i++) {
                    auto key = s + std::to_string(i);
                    auto *ptr = push_response->add_txns();
                    assert(ptr != nullptr);
                    assert(EpochManager::committed_txn_cache[epoch_mod]->getValue(key, (*ptr))); //copy
                }
                push_response->set_result(proto::Success);
                push_response->set_epoch_id(epoch);
                push_response->set_txn_num(total_num);
                auto serialized_pull_resp_str = std::make_unique<std::string>();
                auto res = Gzip(push_msg.get(), serialized_pull_resp_str.get());
                assert(res);
//            printf("message size %u\n", serialized_pull_resp_str->size());
                auto send_message = std::make_unique<zmq::message_t>(*serialized_pull_resp_str);
                if (!socket_send.send(*send_message)) printf("send error!!!!!\n");
//            printf("send to storage pub pack\n");

                epoch++;
            } else {
                usleep(2000);
            }
        }
        socket_send.send((zmq::message_t &) "end");
    }

}