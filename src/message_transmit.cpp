////
//// Created by 周慰星 on 2022/9/17.
////
//
//#include "epoch/epoch_manager.h"
//#include "utils/utilities.h"
//
//namespace Taas {
//
///**
// * port status:                                                                     PULL bind *:port   PUSH connect ip+port
// *
// * 5551 : client sends txns to txn node                                             client  PUSH       txn PULL
// * 5552 : txn node sends txn_state to client                                        client  PULL       txn PUSH
// *
// * 5553 : storage sends pull log request to txn node                                storage PUSH       txn PULL
// * 5554 : txn node sends pull_response to storage node                              storage PULL       txn PUSH
// *
// *
// * 5555 :
// * 5556 : txn nodes sends log to storage nodes                                      txn PUSH            storage PULL
// *
// * 5557 :
// * 20000+id : txn nodes sends txns to other txn nodes; txn nodes send raft states   txn PULL           other txn PUSH
// *
// * PUB bind port      SUB connect ip+port
// * @param data
// * @param hint
// */
////    void SendClientThreadMain(uint64_t id, Context ctx);//PUSH
////    void ListenClientThreadMain(uint64_t id, Context ctx);//PULL
////
////    void ListenStorageThreadMain(uint64_t id, Context ctx);//PUSH PULL
////
////    void SendStoragePUBThreadMain(uint64_t id, Context ctx); //PUSH txn
////    void SendStoragePUBThreadMain2(uint64_t id, Context ctx); //PUSH pack txn
////
////    void SendServerThreadMain(uint64_t id, Context ctx);//PUSH
////    void ListenServerThreadMain(uint64_t id, Context ctx);//PULL
//
//
//    void string_free(void *data, void *hint) {
//        (void) data;
//        delete static_cast<std::string *>(hint);
//    }
//
///**
// * ************************************** *
// *         Client Send and Listen         *
// * ************************************** *
// * */
//
///**
// * @brief 监听client，并接受client发来的写集，并发到listen_message_queue中
// *
// * @param id 暂时未使用
// * @param ctx 暂时未使用
// */
//    void ListenClientThreadMain(uint64_t id, const Context& ctx) {///监听client 写集
//        SetCPU();
//        (void) id;
//        (void) ctx;
//        // 设置ZeroMQ的相关变量，并监听5555端口，接受client发来的写集
//        zmq::context_t listen_context(1);
//        zmq::socket_t socket_listen(listen_context, ZMQ_PULL);
//        socket_listen.bind("tcp://*:5551");
//        printf("线程开始工作 ListenClientThread ZMQ_PULL tcp://*:5551\n");
//
//        while (!EpochManager::IsTimerStop()) {
//            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
//            socket_listen.recv(&(*message_ptr));//防止上次遗留消息造成message cache出现问题
//            if (is_epoch_advance_started.load()) {
//                // TODO: 将client发来的写集放到local_listen_queue中，而不是listen_message_queue
//                // 从而将client的写集和txn node的写集队列分开. by singheart.
//                if (!listen_message_queue.enqueue(std::move(message_ptr))) assert(false);
//                if (!listen_message_queue.enqueue(std::move(std::make_unique<zmq::message_t>())))
//                    assert(false); //防止moodycamel取不出
//                break;
//            }
//        }
//
//        while (!EpochManager::IsTimerStop()) {
//            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
//            socket_listen.recv(&(*message_ptr));
//            if (!listen_message_queue.enqueue(std::move(message_ptr))) assert(false);
//            if (!listen_message_queue.enqueue(std::move(std::make_unique<zmq::message_t>())))
//                assert(false); //防止moodycamel取不出
//        }
//    }
//
///**
// * @brief 将send_to_client_queue中的Reply消息发送给client
// *
// * @param id
// * @param ctx
// */
//    void SendClientThreadMain(uint64_t id, Context ctx) {
//        SetCPU();
//        // 设置ZeroMQ的相关变量，通过5556端口发送Reply给client
//        (void) id;
//        zmq::context_t context(1);
//        int queue_length = 0;
//        std::unique_ptr<send_params> params;
//        std::unique_ptr<zmq::message_t> msg;
//        printf("线程开始工作 SendClientThread ZMQ_PUSH tcp://ip+:5552 \n");
//        while (!init_ok.load()) usleep(200);
//        std::unordered_map<std::string, std::unique_ptr<zmq::socket_t>> socket_map;
//
//        // 测试用，如果设置了会丢弃发送给client的Reply
//        if (ctx.kTestClientNum > 0) {
//            while (!EpochManager::IsTimerStop()) {
//                send_to_client_queue.wait_dequeue(params);
//            }
//        } else {
////         使用ZeroMQ发送Reply给client
//            while(!EpochManager::IsTimerStop()){
//                send_to_client_queue.wait_dequeue(params);
//                if(params == nullptr || params->merge_request_ptr == nullptr) continue;
//    //            msg = std::make_unique<zmq::message_t>(static_cast<void*>(const_cast<char*>(params->merge_request_ptr->data())),
//    //                                                   params->merge_request_ptr->size(), string_free, static_cast<void*>(&(params->merge_request_ptr)));
//                msg = std::make_unique<zmq::message_t>(*(params->merge_request_ptr));
//                auto key = "tcp://" + params->ip + ":5552";
//                if(socket_map.find(key) != socket_map.end()) {
//                    socket_map[key]->send(*(msg));
//                }
//                else {
//                    auto socket = std::make_unique<zmq::socket_t>(context, ZMQ_PUSH);
//                    socket->setsockopt(ZMQ_SNDHWM, &queue_length, sizeof(queue_length));
//                    socket->connect("tcp://" + params->ip + ":5552");
//                    socket_map[key] = std::move(socket);
//                    socket_map[key]->send(*(msg));
//                }
//    //            zmq::socket_t socket_send(context, ZMQ_PUSH);
//    //            socket_send.setsockopt(ZMQ_SNDHWM, &queue_length, sizeof(queue_length));
//    ////            printf("send a txn reply to %s\n", ("tcp://" + params->ip + ":5552").c_str());
//    //            socket_send.connect("tcp://" + params->ip + ":5552");//to server
//    //            socket_send.send(*(msg));
//            }
////==========================PUB==========================
////            zmq::socket_t socket_send(context, ZMQ_PUB);
////            socket_send.setsockopt(ZMQ_SNDHWM, &queue_length, sizeof(queue_length));
////            socket_send.bind("tcp://*:5552");
////            while (!EpochManager::IsTimerStop()) {
////                send_to_client_queue.wait_dequeue(params);
////                if (params == nullptr || params->merge_request_ptr == nullptr) continue;
////                msg = std::make_unique<zmq::message_t>(*(params->merge_request_ptr));
////                socket_send.send(*(msg));
//////            printf("txn time: %lu, id: %lu \n",now_to_us() - params->time, params->id);
////            }
//        }
//    }
//
//
///**
// * ************************************** *
// *         Storage Send and Listen        *
// * ************************************** *
// * */
//
//
//    void ListenStorageThreadMain(uint64_t id, Context ctx) { //PULL & PUSH
//        uint32_t recv_port = 5553;
//        UNUSED_VALUE(id);
//        UNUSED_VALUE(ctx);
//        zmq::context_t context(1);
//        zmq::socket_t recv_socket(context, ZMQ_PULL);
//        zmq::socket_t send_socket(context, ZMQ_PUSH);
//        recv_socket.bind("tcp://*:" + std::to_string(recv_port));
//
//        while (!EpochManager::IsTimerStop()) {
//            std::unique_ptr<zmq::message_t> recv_message = std::make_unique<zmq::message_t>();
//            recv_socket.recv(&(*recv_message));
//            auto message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(recv_message->data()),
//                                                                    recv_message->size());
//            auto pull_msg = std::make_unique<proto::Message>();
//            auto res = UnGzip(pull_msg.get(), message_string_ptr.get());
//            assert(res);
//            auto &pull_req = pull_msg->storage_pull_request();
//            uint64_t epoch_id = pull_req.epoch_id();
////        std::string endpoint = "tcp://" + pull_req.send_node().ip() + std::to_string(pull_req.send_node().port());
//            auto endpoint = "tcp://" + pull_req.send_node().ip() + ":5554";
//            send_socket.connect(endpoint);
//
//            auto pull_msg_resp = std::make_unique<proto::Message>();
//            auto pull_resp = pull_msg_resp->mutable_storage_pull_response();
//
//            // load corresponding epoch's all txns
//            if (EpochManager::committed_txn_num.GetCount(epoch_id) ==
//                EpochManager::should_commit_txn_num.GetCount(epoch_id)) { // the epoch's txns all have been c committed
//                auto s = std::to_string(epoch_id) + ":";
//                auto epoch_mod = epoch_id % EpochManager::max_length;
//                auto total_num = EpochManager::epoch_log_lsn.GetCount(epoch_id);
//                for (int i = 0; i < total_num; i++) {
//                    auto key = s + std::to_string(i);
//                    auto *ptr = pull_resp->add_txns();
//                    EpochManager::committed_txn_cache[epoch_mod]->getValue(key, (*ptr)); //copy
//                }
////            for (auto iter = EpochManager::redo_log[epoch_id]->begin(); iter != EpochManager::redo_log[epoch_id]->end(); iter++) {
////                proto::Transaction* ptr = pull_resp->add_txns();
////                ptr->CopyFrom(*iter);
////            }
//                pull_resp->set_result(proto::Success);
//                pull_resp->set_epoch_id(epoch_id);
//            } else {
//                pull_resp->set_result(proto::Fail);
//            }
//
////        std::unique_ptr<std::string> serialized_pull_resp = std::make_unique<std::string>();
////        pull_resp->SerializeToString(&(*serialized_pull_resp));
//            auto serialized_pull_resp = std::make_unique<std::string>();
//            res = Gzip(pull_msg_resp.get(), serialized_pull_resp.get());
//            std::unique_ptr<zmq::message_t> send_message = std::make_unique<zmq::message_t>(*serialized_pull_resp);
//            send_socket.send(*send_message);
//            send_socket.disconnect(endpoint);
//        }
//
//    }
//
//    void SendStoragePUBThreadMain(uint64_t id, Context ctx) { //PUB Txn
//        zmq::context_t context(1);
//        zmq::message_t reply(5);
//        zmq::socket_t socket_send(context, ZMQ_PUB);
//        socket_send.bind("tcp://*:5556");//to server
//        printf("线程开始工作 SendStoragePUBServerThread ZMQ_PUB tcp:// ip + :5556\n");
//        std::unique_ptr<send_params> params;
//        std::unique_ptr<zmq::message_t> msg;
//        while (init_ok.load() == false);
//        while (!EpochManager::IsTimerStop()) {
//            send_to_storage_queue.wait_dequeue(params);
//            if (params == nullptr || params->merge_request_ptr == nullptr) continue;
////        msg = std::make_unique<zmq::message_t>(static_cast<void*>(const_cast<char*>(params->merge_request_ptr->data())),
////                                               params->merge_request_ptr->size(), string_free, static_cast<void*>(&(params->merge_request_ptr)));
//            msg = std::make_unique<zmq::message_t>(*(params->merge_request_ptr));
//            socket_send.send(*msg);
//        }
//        socket_send.send((zmq::message_t &) "end");
//    }
//
//    void SendStoragePUBThreadMain2(uint64_t id, Context ctx) {//PUB PACK
//        SetCPU();
//        zmq::context_t context(1);
//        zmq::message_t reply(5);
//        zmq::socket_t socket_send(context, ZMQ_PUB);
//        int queue_length = 0;
//        socket_send.setsockopt(ZMQ_SNDHWM, &queue_length, sizeof(queue_length));
//        socket_send.bind("tcp://*:5556");//to server
//        printf("线程开始工作 SendStorage PUBServerThread ZMQ_PUB tcp:// ip + :5556\n");
//        std::unique_ptr<send_params> params;
//        std::unique_ptr<zmq::message_t> msg;
//        uint64_t epoch = 1;
//        while (init_ok.load() == false);
//        while (!EpochManager::IsTimerStop()) {
//            if (epoch < EpochManager::GetLogicalEpoch()) {
//                auto push_msg = std::make_unique<proto::Message>();
//                auto push_response = push_msg->mutable_storage_push_response();
//                assert(push_response != nullptr);
//                auto s = std::to_string(epoch) + ":";
//                auto epoch_mod = epoch % EpochManager::max_length;
//                auto total_num = EpochManager::epoch_log_lsn.GetCount(epoch);
//                for (int i = 0; i < total_num; i++) {
//                    auto key = s + std::to_string(i);
//                    auto *ptr = push_response->add_txns();
//                    assert(ptr != nullptr);
//                    assert(EpochManager::committed_txn_cache[epoch_mod]->getValue(key, (*ptr))); //copy
//                }
//                push_response->set_result(proto::Success);
//                push_response->set_epoch_id(epoch);
//                push_response->set_txn_num(total_num);
//                auto serialized_pull_resp_str = std::make_unique<std::string>();
//                auto res = Gzip(push_msg.get(), serialized_pull_resp_str.get());
//                assert(res);
////            printf("message size %u\n", serialized_pull_resp_str->size());
//                auto send_message = std::make_unique<zmq::message_t>(*serialized_pull_resp_str);
//                if (!socket_send.send(*send_message)) printf("send error!!!!!\n");
////            printf("send to storage pub pack\n");
//
//                epoch++;
//            } else {
//                usleep(2000);
//            }
//        }
//        socket_send.send((zmq::message_t &) "end");
//    }
//
//
//
///**
// * ************************************** *
// *         txn nodes Send and Listen         *
// * ************************************** *
// * */
//
///**
// * @brief 将send_to_server_queue中的数据通过5557端口发送给其他txn node
// *
// * @param id 暂未使用
// * @param ctx 暂未使用
// */
//    void SendServerThreadMain(uint64_t id, Context ctx) {
//        SetCPU();
//        zmq::context_t context(1);
//        zmq::message_t reply(5);
//        int queue_length = 0;
//        std::unordered_map<std::uint64_t, std::unique_ptr<zmq::socket_t>> socket_map;
//        std::unique_ptr<send_params> params;
//        std::unique_ptr<zmq::message_t> msg;
//        for (int i = 0; i < (int) ctx.kServerIp.size(); i++) {
//            if (i == (int) ctx.txn_node_ip_index) continue;
//            auto socket = std::make_unique<zmq::socket_t>(context, ZMQ_PUSH);
//            socket->connect("tcp://" + params->ip + ":" + std::to_string(20000+i));
//            socket_map[i] = std::move(socket);
//            printf("Listen Server ZMQ_SUB %s", ("tcp://" + ctx.kServerIp[i] + ":5558\n").c_str());
//        }
//        printf("线程开始工作 SendServerThread ZMQ_PUB tcp:// ip + :5558\n");
//        while (!init_ok.load());
//        while (!EpochManager::IsTimerStop()) {
//
//            send_to_server_queue.wait_dequeue(params);
//            if (params == nullptr || params->merge_request_ptr == nullptr) continue;
////        msg = std::make_unique<zmq::message_t>(static_cast<void*>(const_cast<char*>(params->merge_request_ptr->data())),
////                                               params->merge_request_ptr->size(), string_free, static_cast<void*>(&(params->merge_request_ptr)));
//            msg = std::make_unique<zmq::message_t>(*(params->merge_request_ptr));
//            socket_map[params->id]->send(*msg);
//        }
//        socket_map[id]->send((zmq::message_t &) "end");
//    }
//
///**
// * @brief 监听其他txn node发来的写集，并放在listen_message_queue中
// *
// * @param id 暂时未使用
// * @param ctx XML的配置信息
// */
//    void ListenServerThreadMain(uint64_t id, Context ctx) {///监听远端txn node写集
//        SetCPU();
//        // 设置ZeroMQ的相关变量，监听其他txn node是否有写集发来
//        zmq::context_t listen_context(1);
//        zmq::socket_t socket_listen(listen_context, ZMQ_PULL);
//        int queue_length = 0;
//        socket_listen.bind("tcp://*:5558");//to server
//        socket_listen.setsockopt(ZMQ_SNDHWM, &queue_length, sizeof(queue_length));
//        socket_listen.setsockopt(ZMQ_SUBSCRIBE, "", 0);
//        printf("线程开始工作 ListenServerThread ZMQ_PULL\n");
//
//        while (!EpochManager::IsTimerStop()) {
//            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
//            socket_listen.recv(&(*message_ptr));//防止上次遗留消息造成message cache出现问题
//            if (is_epoch_advance_started.load() == true) {
//                if (!listen_message_queue.enqueue(std::move(message_ptr))) assert(false);
//                if (!listen_message_queue.enqueue(std::move(std::make_unique<zmq::message_t>())))
//                    assert(false); //防止moodycamel取不出
//                break;
//            }
//        }
//
//        while (!EpochManager::IsTimerStop()) {
//            std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
//            socket_listen.recv(&(*message_ptr));
//            if (!listen_message_queue.enqueue(std::move(message_ptr))) assert(false);
//            if (!listen_message_queue.enqueue(std::move(std::make_unique<zmq::message_t>())))
//                assert(false); //防止moodycamel取不出
//        }
//    }
//}