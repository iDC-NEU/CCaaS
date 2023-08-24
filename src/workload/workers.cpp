//
// Created by user on 23-8-24.
//
#include "workload/worker.h"

namespace workload {

    [[noreturn]] void SendTaasClientThreadMain(){
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
        socket->connect("tcp://" + MultiModelWorkload::ctx.kTaasIP + ":" + std::to_string(10001));
        isExe[0] = true;
        printf("Send Server connect ZMQ_PUSH %s", ("tcp://" + MultiModelWorkload::ctx.kTaasIP + ":" + std::to_string(10001) + "\n").c_str());
        printf("线程开始工作 SendServerThread\n");
        while (true) {
            send_multi_txn_queue->wait_dequeue(params);
            if (params == nullptr || params->merge_request_ptr == nullptr) continue;
//            printf("send to server epoch %lu type %d \n", params->txnid,params->merge_request_ptr->size());
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
        isExe[1] = true;
        std::unique_ptr<proto::Message> msg_ptr;
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        for (;;) {
            message_ptr = std::make_unique<zmq::message_t>();
            recvResult = socket_listen.recv((*message_ptr), recvFlags);
            assert(recvResult != -1);
            client_listen_taas_message_queue->enqueue(std::move(message_ptr));
            client_listen_taas_message_queue->enqueue(std::make_unique<zmq::message_t>());
        }
    }

    [[noreturn]] void DequeueClientListenTaasMessageQueue() {
        printf("DequeueClientListenTaasMessageQueue Start!\n");
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        std::unique_ptr<proto::Message> msg_ptr;
        isExe[2] = true;
        uint64_t csn;
        bool res;
        while (true) {
            client_listen_taas_message_queue->wait_dequeue(message_ptr);
            if (message_ptr != nullptr && !message_ptr->empty()) {
//                Log_print("worker dequeue " + std::to_string(message_ptr->size()));
                msg_ptr = std::make_unique<proto::Message>();
                message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()),
                                                                   message_ptr->size());
                msg_ptr = std::make_unique<proto::Message>();
                res = Taas::UnGzip(msg_ptr.get(), message_string_ptr.get());
                assert(res);
                if (msg_ptr->type_case() == proto::Message::TypeCase::kReplyTxnResultToClient) {
                    // wake up local thread and return the commit result
                    auto &txn = msg_ptr->reply_txn_result_to_client();
                    csn = txn.client_txn_id();
                    if (txn.txn_state() == proto::TxnState::Commit) {
                        printf("kReplyTxnResultToClient Commit  \n");
                        if(cv.contain(csn)){
                            printf("kReplyTxnResultToClient notify  \n");
                            std::condition_variable* cv_tmp;
                            cv.getValue(csn, cv_tmp);
                            cv_tmp->notify_all();
                            cv.remove(csn);
                        }
                    } else {
                        printf("未找到 csn %lu \n", csn);
                        // MOT_LOG_INFO(%lu csn %llu", csn);
                    }
                }  else {
                    printf("not kReplyTxnResultToClient \n");
                }
            } else {
//                Log_print("ClientWorkerThreadMain empty ");
            }
        }
    }

    void WorkerGenerateTxnFileThreadMain() {
        std::fstream kvTxnWritefile;
        kvTxnWritefile.open("../kvTxn.txt", std::ios::out | std::ios::trunc);
        std::fstream gqlTxnWritefile;
        gqlTxnWritefile.open("../gqlTxn.txt", std::ios::out | std::ios::trunc);
        std::fstream sqlTxnWritefile;
        sqlTxnWritefile.open("../sqlTxn.txt", std::ios::out | std::ios::trunc);
        std::cout << "Generate txnFile" << std::endl;
        while (true) {
            MultiModelWorkload::lock.lock();
            uint64_t now_tid = MultiModelWorkload::GetTxnId();
            if (MultiModelWorkload::ctx.kTxnNum < now_tid) {
                MultiModelWorkload::lock.unlock();
                break;
            }
            GenerateGQLFile(now_tid, &gqlTxnWritefile);
            GenerateKVTxnFile(now_tid, &kvTxnWritefile);
            GenerateSQLTxnFile(now_tid, &sqlTxnWritefile);
            MultiModelWorkload::AddTxnId();
            MultiModelWorkload::lock.unlock();
        }
        std::cout << "Generate txnFile Done" << std::endl;
        kvTxnWritefile.close();
        gqlTxnWritefile.close();
        sqlTxnWritefile.close();
    }

    void WorkerReadTxnFileThreadMain() {
        std::cout << "WorkerReadTxnThreadMain Start ";
        std::ifstream kvTxnReadfile;
        kvTxnReadfile.open("../kvTxn.txt", std::ios::in);
        std::ifstream gqlTxnReadfile;
        gqlTxnReadfile.open("../gqlTxn.txt", std::ios::in);
        std::ifstream sqlTxnReadfile;
        sqlTxnReadfile.open("../sqlTxn.txt", std::ios::in);
        std::vector<std::thread> threads;
        while (true) {
            MultiModelWorkload::lock.lock();
            uint64_t now_tid = MultiModelWorkload::GetTxnId();
            MultiModelTxn txn;
            txn.tid = now_tid;
            if(MultiModelWorkload::ctx.useNebula) txn.nebulaTxn = GenerateGQLTxn(&gqlTxnReadfile);
            if(MultiModelWorkload::ctx.useMot) txn.motTxn = GenerateSQLTxn(&sqlTxnReadfile);
            txn.kvTxn = GenerateKVTxn(&kvTxnReadfile);
            bool kvBool = kvTxnReadfile.eof();
            bool sqlBool = sqlTxnReadfile.eof();
            bool gqlBool = gqlTxnReadfile.eof();
            if(MultiModelWorkload::ctx.useNebula) gqlBool = true;
            if(MultiModelWorkload::ctx.useMot) sqlBool = true;
            if ( kvBool && gqlBool && sqlBool)  {
                std::cout << "Done read" << std::endl;
                break;
            }
            if(txn.tid > MultiModelWorkload::ctx.kTxnNum) break;
            MultiModelWorkload::thread_pool->push_task([txn] {
                workload::MultiModelWorkload::RunMultiTxn(txn);
            });
            MultiModelWorkload::AddTxnId();
            MultiModelWorkload::lock.unlock();
            // 多线程
            //nebula to nebulaServer
//            std::cout<<txn.kvTxn.size()<<std::endl;
        }
        while(commitTxnId < MultiModelWorkload::ctx.kTxnNum) usleep(1000);
//        MultiModelWorkload::thread_pool->shutdown();
        std::cout << "txn generate txn done" << std::endl;
    }

    bool check(){
        for(int i=0;i<3;i++) if(!isExe[i]) return false;
        return true;
    }

    int main() {
        Taas::MultiModelContext mctx;
        mctx.GetMultiModelInfo();
        MultiModelWorkload param;
        MultiModelWorkload::StaticInit(mctx);
//        param.threadpool = new ThreadPool(mctx.kclient_threads);
//        param.threadpool->init();
        MultiModelWorkload::SetTxnId(1);
        MultiModelWorkload::SetGraphVid(0);

        printf("System Start\n");
        std::vector<std::unique_ptr<std::thread>> threads;
        if(mctx.useNebula) MultiModelWorkload::NebulaInit();
        if(mctx.useMot) MultiModelWorkload::MOTInit();
        if(mctx.isGenerateTxn){
            threads.push_back(std::make_unique<std::thread>(WorkerGenerateTxnFileThreadMain));
            threads[0]->join();
        }
        int cc=0;
        if(mctx.isGenerateTxn){
            cc++;
        }

        threads.push_back(std::make_unique<std::thread>(ClientListenTaasThreadMain));
        threads.push_back(std::make_unique<std::thread>(DequeueClientListenTaasMessageQueue));
        threads.push_back(std::make_unique<std::thread>(SendTaasClientThreadMain));
        while(!check()) usleep(10000);
        uint64_t startTime = Taas::now_to_us();
        threads.push_back(std::make_unique<std::thread>(WorkerReadTxnFileThreadMain));
        threads[threads.size() - 1]->join();
        uint64_t consumeTime = Taas::now_to_us() - startTime;

        std::cout<<"Total consume time(ms) : "<<1.0 * (double)consumeTime / 1000.0<<std::endl;
        double avgTime = 1.0 * (double)execTimes[0];
        for(int i = 1; i < (int)execTimes.size(); i++){
            avgTime = (avgTime + (double)execTimes[i]) / 2.0;
        }
        std::cout << "Commit txn number : " << execTimes.size() <<std::endl;
        std::cout << "Per txn consume time(us) : " << avgTime << std::endl;
        for(; cc < (int)threads.size() - 1; cc ++) {
            threads[cc]->join();
        }
        std::cout << "============================================================================" << std::endl;
        std::cout << "=====================              END                 =====================" << std::endl;
        std::cout << "============================================================================" << std::endl;

        return 0;
    }
}