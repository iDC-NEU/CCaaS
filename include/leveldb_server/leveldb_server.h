//
// Created by zwx on 23-6-30.
//

#ifndef TAAS_LEVELDB_SERVER_H
#define TAAS_LEVELDB_SERVER_H

#pragma once

#include "proto/kvdb_server.pb.h"
#include "brpc/server.h"
#include "tools/context.h"
namespace Taas {

    void LevelDBServer(const Context &context);

    class LevelDBGetService : public proto::KvDBGetService{
    public:
        void Get(::google::protobuf::RpcController* controller,
                 const ::proto::KvDBRequest* request,
                 ::proto::KvDBResponse* response,
                 ::google::protobuf::Closure* done);
    };

    class LevelDBPutService : public proto::KvDBPutService{
    public:
        void Put(::google::protobuf::RpcController* controller,
                 const ::proto::KvDBRequest* request,
                 ::proto::KvDBResponse* response,
                 ::google::protobuf::Closure* done);
    };

}

#endif //TAAS_LEVELDB_SERVER_H
