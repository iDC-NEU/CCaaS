//
// Created by user on 23-8-24.
//
#include "workload/worker.h"
#include "workload/multi_model_workload.h"

namespace workload {

    void SendTaasClientThreadMain(){
        zmq::context_t context(1);
        zmq::message_t reply(5);
        zmq::send_flags sendFlags = zmq::send_flags::none;
        int queue_length = 0;
        std::unordered_map<std::uint64_t, std::unique_ptr<zmq::socket_t>> socket_map;
        std::unique_ptr<send_multimodel_params> params;
        std::unique_ptr<zmq::message_t> msg;
        auto socket = std::make_unique<zmq::socket_t>(context, ZMQ_PUSH);
        socket->set(zmq::sockopt::sndhwm, queue_length);
        socket->set(zmq::sockopt::rcvhwm, queue_length);
        socket->connect("tcp://" + MultiModelWorkload::ctx.multiModelContext.kTaasIP + ":" + std::to_string(10001));
        MultiModelWorkload::isExe[0] = true;
        printf("Send Server connect ZMQ_PUSH %s", ("tcp://" + MultiModelWorkload::ctx.multiModelContext.kTaasIP + ":" + std::to_string(10001) + "\n").c_str());
        printf("线程开始工作 SendServerThread\n");
        while (true) {
            MultiModelWorkload::send_multi_txn_queue->wait_dequeue(params);
            if (params == nullptr || params->merge_request_ptr == nullptr) continue;
            msg = std::make_unique<zmq::message_t>(*(params->merge_request_ptr));
            socket->send(*msg, sendFlags);
        }
    }

    void ClientListenTaasThreadMain() {
        printf("ClientListenTaasThreadMain  5552 Start!\n");
        int queue_length = 0;
        zmq::recv_flags recvFlags = zmq::recv_flags::none;
        zmq::recv_result_t recvResult;
        zmq::context_t listen_context(1);
        zmq::socket_t socket_listen(listen_context, ZMQ_PULL);
        socket_listen.set(zmq::sockopt::sndhwm, queue_length);
        socket_listen.set(zmq::sockopt::rcvhwm, queue_length);
        socket_listen.bind("tcp://*:5552");
        MultiModelWorkload::isExe[1] = true;
        std::unique_ptr<proto::Message> msg_ptr;
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        for (;;) {
            message_ptr = std::make_unique<zmq::message_t>();
            recvResult = socket_listen.recv((*message_ptr), recvFlags);
            assert(recvResult != -1);
            MultiModelWorkload::client_listen_taas_message_queue->enqueue(std::move(message_ptr));
            MultiModelWorkload::client_listen_taas_message_queue->enqueue(std::make_unique<zmq::message_t>());
        }
    }

    void DequeueClientListenTaasMessageQueue() {
        printf("DequeueClientListenTaasMessageQueue Start!\n");
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        std::unique_ptr<proto::Message> msg_ptr;
        MultiModelWorkload::isExe[2] = true;
        uint64_t csn;
        bool res;
        while (true) {
            MultiModelWorkload::client_listen_taas_message_queue->wait_dequeue(message_ptr);
            if (message_ptr != nullptr && !message_ptr->empty()) {
                message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()),
                                                                   message_ptr->size());
                msg_ptr = std::make_unique<proto::Message>();
                res = Taas::UnGzip(msg_ptr.get(), message_string_ptr.get());
                assert(res);
                if (msg_ptr->type_case() == proto::Message::TypeCase::kReplyTxnResultToClient) {
                    auto &txn = msg_ptr->reply_txn_result_to_client();
                    csn = txn.client_txn_id();
                    if (txn.txn_state() == proto::TxnState::Commit) {
                        printf("kReplyTxnResultToClient Commit  \n");
                        if(MultiModelWorkload::multiModelTxnConditionVariable.contain(csn)){
                            printf("kReplyTxnResultToClient notify  \n");
                            std::shared_ptr<std::condition_variable> cv_tmp;
                            MultiModelWorkload::multiModelTxnConditionVariable.getValue(csn, cv_tmp);
                            cv_tmp->notify_all();
                            MultiModelWorkload::multiModelTxnConditionVariable.remove(csn);
                        }
                    } else {
                        printf("未找到 csn %lu \n", csn);
                    }
                }  else {
                    printf("not kReplyTxnResultToClient \n");
                }
            }
        }
    }

    bool check(){
        for(int i=0;i<3;i++) if(!MultiModelWorkload::isExe[i]) return false;
        return true;
    }

    int main() {
        printf("====== Taas Multi-Model Client Init Start ======\n");
        Taas::Context ctx;
        MultiModelWorkload param;
        MultiModelWorkload::StaticInit(ctx);
        std::vector<std::unique_ptr<std::thread>> threads;

        threads.push_back(std::make_unique<std::thread>(ClientListenTaasThreadMain));
        threads.push_back(std::make_unique<std::thread>(DequeueClientListenTaasMessageQueue));
        threads.push_back(std::make_unique<std::thread>(SendTaasClientThreadMain));

//        if(ctx.multiModelContext.isUseNebula) Nebula::Init(ctx);
//        if(ctx.multiModelContext.isUseMot) MOT::Init();
        printf("====== Taas Multi-Model Client Init OK ======\n");
        printf("====== Taas Multi-Model Client LoadData Start ======\n");
        if(ctx.multiModelContext.isLoadData) {
            MultiModelWorkload::LoadData();
        }
        printf("====== Taas Multi-Model Client Run Start ======\n");
        MultiModelWorkload::workCountDown.reset((int)ctx.multiModelContext.kClientNum);
        for(int i = 0; i < (int)MultiModelWorkload::ctx.multiModelContext.kClientNum; i ++) {
            MultiModelWorkload::thread_pool->push_task(MultiModelWorkload::RunMultiTxn);
        }
        while(!check()) {
            usleep(10000);
        }
        uint64_t startTime = Taas::now_to_us();
        uint64_t cnt = 0;
        while(MultiModelWorkload::subWorksNum.load() < MultiModelWorkload::ctx.multiModelContext.kClientNum) {
            if(cnt % 100 == 0) {
                LOG(INFO) << "Test Exec:" << Taas::now_to_us() - startTime << ", Commit txn number : " << MultiModelWorkload::execTimes.size();
            }
            cnt ++;
            usleep(10000);
        }
        uint64_t consumeTime = Taas::now_to_us() - startTime;
        std::cout<<"Total consume time(ms) : "<<1.0 * (double)consumeTime / 1000.0<<std::endl;
        double avgTime = 1.0 * (double)MultiModelWorkload::execTimes[0];
        for(int i = 1; i < (int)MultiModelWorkload::execTimes.size(); i++){
            avgTime = (avgTime + (double)MultiModelWorkload::execTimes[i]) / 2.0;
        }
        std::cout << "Commit txn number : " << MultiModelWorkload::execTimes.size() <<std::endl;
        std::cout << "Per txn consume time(us) : " << avgTime << std::endl;
        std::cout << "============================================================================" << std::endl;
        std::cout << "=====================              END                 =====================" << std::endl;
        std::cout << "============================================================================" << std::endl;
        return 0;
    }
}