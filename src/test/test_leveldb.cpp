//
// Created by zwx on 23-8-13.
//
#include "transaction/merge.h"
#include <random>
#include <brpc/channel.h>
#include <proto/transaction.pb.h>
#include "proto/kvdb_server.pb.h"

namespace Taas {

    void LevelDBClient(const Context& ctx, uint64_t id) {
        LOG(INFO) << "Leveldb Client test = " << id;
        brpc::Channel channel;
        brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_BAIDU_STD;
        if (channel.Init(ctx.kLevevDBIP.c_str(), &options) != 0) {
            LOG(ERROR) << "Fail to initialize channel";
            return;
        }
        proto::KvDBGetService_Stub get_stub(&channel);
        srand(now_to_us() % 71);
        uint64_t txn_id = 0, op_num, op_type;
        std::string read_version, write_version;
        std::random_device rd;
        auto gen = std::default_random_engine(rd());
        std::uniform_int_distribution<int>
                op_num_dis(1, static_cast<int>(ctx.kTestTxnOpNum)),
                op_type_dis(1, 4),
                key_range_dis(1, static_cast<int>(ctx.kTestTxnOpNum)),
                sleep_dis(1, 20000);

        while (!EpochManager::IsInitOK() || EpochManager::GetLogicalEpoch() < 10) usleep(sleep_time);
        while (!EpochManager::IsTimerStop()) {
            auto message_ptr = std::make_unique<proto::Message>();
            auto *txn_ptr = message_ptr->mutable_txn();
            txn_ptr->set_client_txn_id(txn_id * ctx.kTestClientNum + id);
            write_version = std::to_string(txn_ptr->client_txn_id()) + std::to_string(ctx.txn_node_ip_index);
            txn_id++;
            txn_ptr->set_txn_type(proto::ClientTxn);
            op_num = op_num_dis(gen);
            for (unsigned int i = 0; i < op_num; i++) {
                auto key = std::to_string(key_range_dis(gen) % ctx.kTestKeyRange);
                op_type = op_type_dis(gen);
                auto row = txn_ptr->add_row();
                row->set_key(key);
                switch (op_type) {
                    case 0 : {
                        proto::KvDBRequest get_Request;
                        proto::KvDBResponse get_Response;
                        brpc::Controller get_Cntl;
                        get_Cntl.set_timeout_ms(5000);
                        auto get_data = get_Request.add_data();
                        int rand_num = key_range_dis(gen);
                        get_data->set_key("hello" + std::to_string(rand_num));
                        get_stub.Get(&get_Cntl, &get_Request, &get_Response, nullptr);
                        if (get_Cntl.Failed()) {
                            if (get_Response.result()) {
                                write_version = get_Response.data()[0].value();
                            }
                            else {
                                write_version = "";
                            }
                        } else {
                            LOG(WARNING) << get_Cntl.ErrorText();
                            write_version = "";
                        }
                        row->set_op_type(proto::Read);
                        break;
                    }
                    case 1 : {
                        row->set_op_type(proto::Insert);
                        break;
                    }
                    case 2 : {
                        row->set_op_type(proto::Update);
                        break;
                    }
                    case 3 : {
                        row->set_op_type(proto::Delete);
                        break;
                    }
                    default:
                        break;
                }
                row->set_data(write_version);
            }
            txn_ptr->set_client_ip("127.0.0.1");
            auto serialized_txn_str = std::string();
            google::protobuf::io::GzipOutputStream::Options gzip_options;
            gzip_options.format = google::protobuf::io::GzipOutputStream::GZIP;
            gzip_options.compression_level = 9;
            google::protobuf::io::StringOutputStream outputStream(&serialized_txn_str);
            google::protobuf::io::GzipOutputStream gzipStream(&outputStream, gzip_options);
            message_ptr->SerializeToZeroCopyStream(&gzipStream);
            gzipStream.Close();
            void *data = static_cast<void *>(const_cast<char *>(serialized_txn_str.data()));
            MessageQueue::listen_message_txn_queue->enqueue(
                    std::make_unique<zmq::message_t>(data, serialized_txn_str.size()));
            usleep(sleep_dis(gen) % 2000);
        }
    }
}