//
// Created by zwx on 23-6-30.
//


#include "leveldb_server/leveldb_server.h"
#include "db/db_interface.h"
#include <future>

namespace Taas {

    static std::vector<std::unique_ptr<DBConnection>> leveldb_connections;
    static std::atomic<uint64_t> connection_num(0);

    void LevelDBServer(const Context &context){
        brpc::Server leveldb_server;
        brpc::ServerOptions options;
        LevelDBGetService leveldb_get_service;
        LevelDBPutService leveldb_put_service;

        leveldb_connections.resize(1);
        for(int i = 0; i < 1; i ++) {
            leveldb_connections[i] = DBConnection::NewConnection("leveldb");
        }


        if(leveldb_server.AddService(&leveldb_get_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add leveldb_get_service";
            assert(false);
        }
        if(leveldb_server.AddService(&leveldb_put_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add leveldb_put_service";
            assert(false);
        }
        if (leveldb_server.Start(2379, &options) != 0) {
            LOG(ERROR) << "Fail to start leveldb_server";
        }
        LOG(INFO) << "======*** LEVELDB SERVER START ***=====\n";
        leveldb_server.RunUntilAskedToQuit();
    }

    void LevelDBGetService::Get(::google::protobuf::RpcController *controller, const ::proto::KvDBRequest *request,
                         ::proto::KvDBResponse *response, ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        std::string value;
        const auto& data = request->data();
        const std::string& key= data[0].key();
        auto res = leveldb_connections[0]->get(key, &value);
        response->set_result(res);
        const auto& response_data = response->add_data();
        response_data->set_value(value);
//        LOG(INFO) << "get-key : " << key << ",get-value : " << value << ",response result : " << res ;
    }

    void LevelDBPutService::Put(::google::protobuf::RpcController *controller, const ::proto::KvDBRequest *request,
                         ::proto::KvDBResponse *response, ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        const auto& data = request->data();
        const std::string& key = data[0].key();
        const std::string& value = data[0].value();
        auto res = leveldb_connections[0]->syncPut(key, value);
        response->set_result(res);
//        LOG(INFO) << "put-key : " << key << ",put-value : " << value << ",response result : " << res ;
    }
}