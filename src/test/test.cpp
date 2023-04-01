//
// Created by 周慰星 on 2022/9/20.
//

#include "test/test.h"
#include "transaction/merge.h"
#include <random>

namespace Taas {

    void Client(uint64_t id, const Context& ctx) {
        printf("Test Client %lu start\n", id);
        srand(now_to_us() % 71);
        uint64_t txn_id = 0, op_num, op_type;
        std::string read_version, write_version;
        std::random_device rd;
        auto gen = std::default_random_engine (rd());
        std::uniform_int_distribution<int>
            op_num_dis(1, static_cast<int>(ctx.kTestTxnOpNum)),
            op_type_dis(1,4),
            key_range_dis(1,static_cast<int>(ctx.kTestTxnOpNum)),
            sleep_dis(1, 20000);

        while(!EpochManager::IsInitOK()) usleep(10000);
        usleep(10000);
        while(!EpochManager::IsTimerStop()) {
            auto message_ptr = std::make_unique<proto::Message>();
            auto* txn_ptr = message_ptr->mutable_txn();
            txn_ptr->set_client_txn_id(txn_id * ctx.kTestClientNum + id);
            write_version = std::to_string(txn_ptr->client_txn_id()) + std::to_string(ctx.txn_node_ip_index);
            txn_id ++;
            txn_ptr->set_txn_type(proto::ClientTxn);
            op_num = op_num_dis(gen);
//            op_num = rand() % ctx.kTestTxnOpNum + 1;
            for(unsigned int i = 0; i < op_num; i ++) {
                auto key = std::to_string(key_range_dis(gen) % ctx.kTestKeyRange);
                op_type = op_type_dis(gen);
//                auto key = std::to_string(rand() % ctx.kTestKeyRange);
//                op_type = rand() % 4;
                auto row = txn_ptr->add_row();
                row->set_key(key);
                switch (op_type) {
                    case 0 : {
                        if(Merger::read_version_map.getValue(key, read_version)) {
                            row->set_op_type(proto::Read);
                            row->set_data(read_version);
                            break;
                        }
                        else {
                            row->set_op_type(proto::Insert);
                            break;
                        }
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
//        printf("read version is %s, write version is %s\n", read_version.c_str(), write_version.c_str());
            txn_ptr->set_client_ip("127.0.0.1");
            auto serialized_txn_str = std::string();
            google::protobuf::io::GzipOutputStream::Options options;
            options.format = google::protobuf::io::GzipOutputStream::GZIP;
            options.compression_level = 9;
            google::protobuf::io::StringOutputStream outputStream(&serialized_txn_str);
            google::protobuf::io::GzipOutputStream gzipStream(&outputStream, options);
            message_ptr->SerializeToZeroCopyStream(&gzipStream);
            gzipStream.Close();
            void *data = static_cast<void*>(const_cast<char*>(serialized_txn_str.data()));
            MessageQueue::listen_message_queue->enqueue(std::make_unique<zmq::message_t>(data, serialized_txn_str.size()));

            usleep(sleep_dis(gen) % 2000);
        }
    }
}

