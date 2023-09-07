//
// Created by zwx on 23-8-29.
//

#include "workload/nebula.h"
#include "tools/utilities.h"
#include "workload/multi_model_workload.h"

#include<glog/logging.h>

namespace workload {

    nebula::SessionPoolConfig Nebula::nebulaSessionPoolConfig;
    std::unique_ptr<nebula::SessionPool> Nebula::nebulaSessionPool;

    void Nebula::Init(const Taas::Context& ctx) {
        nebula::ConnectionPool pool;
        auto connectConfig = nebula::Config{};
        connectConfig.maxConnectionPoolSize_ = ctx.multiModelContext.kClientNum ;
        pool.init({ctx.multiModelContext.kNebulaIP}, connectConfig);
        auto session = pool.getSession(ctx.multiModelContext.kNebulaUser, ctx.multiModelContext.kNebulaPwd);
        assert(session.valid());
        auto resp = session.execute(
                "CREATE SPACE IF NOT EXISTS " + ctx.multiModelContext.kNebulaSpace +" (vid_type = FIXED_STRING(30));");
        assert(resp.errorCode == nebula::ErrorCode::SUCCEEDED);

        nebulaSessionPoolConfig.username_ = ctx.multiModelContext.kNebulaUser;
        nebulaSessionPoolConfig.password_ = ctx.multiModelContext.kNebulaPwd;
        nebulaSessionPoolConfig.addrs_ = {ctx.multiModelContext.kNebulaIP};
        nebulaSessionPoolConfig.spaceName_ = ctx.multiModelContext.kNebulaSpace;
        nebulaSessionPoolConfig.maxSize_ = ctx.multiModelContext.kClientNum;
        nebulaSessionPool = std::make_unique<nebula::SessionPool>(nebulaSessionPoolConfig);
        nebulaSessionPool->init();
        resp = nebulaSessionPool->execute("CREATE TAG IF NOT EXISTS usertable (key, string, filed, tid string);");
        assert(resp.errorCode == nebula::ErrorCode::SUCCEEDED);
        resp = nebulaSessionPool->execute("CREATE TAG INDEX IF NOT EXISTS usertable_index on usertable(key(10));");
        assert(resp.errorCode == nebula::ErrorCode::SUCCEEDED);
        LOG(INFO) << "=== connect to nebula done ===";
    }

    void Nebula::InsertData(uint64_t& tid) {
        if(tid > MultiModelWorkload::ctx.multiModelContext.kRecordCount) return;
        char genKey[100], gql[800];
        std::string data1 = Taas::RandomString(256);
        std::string data2 = Taas::RandomString(256);
        sprintf(genKey, "taas_nebula_key:%064lu", tid);
        auto keyName = std::string(genKey);
        utils::ByteIteratorMap values;
        MultiModelWorkload::buildValues(values, keyName);
        std::string value;
        for (const auto &it: values) {
            value += it.second + ",";
        }
        sprintf(gql, R"(INSERT VERTEX IF NOT EXISTS usertable(key, filed, txnid) VALUES "%s" :("%s","%s","%lu");)",
                                genKey, genKey, value.c_str(), tid);

        auto resp = nebulaSessionPool->execute(gql);
    }

    void Nebula::RunTxn(uint64_t& tid) {///single op txn
        char genKey[100], gql[800];
        std::string value;
        int cnt, i;
        if(MultiModelWorkload::ctx.multiModelContext.kTestMode == Taas::MultiModel) {
            cnt = 1;
        }
        else if(MultiModelWorkload::ctx.multiModelContext.kTestMode == Taas::GQL) {
            cnt = 9;
        }
        else return ;
        {
            for (i = 0; i < cnt; i++) {
                auto opType = MultiModelWorkload::operationChooser->nextValue();
                auto id = MultiModelWorkload::keyChooser[0]->nextValue();
                sprintf(genKey, "usertable_key:%064lu", id);
                auto keyName = std::string(genKey);
                if (opType == Operation::READ) {
                    sprintf(gql, R"(FETCH PROP ON usertable "%s" YIELD properties(VERTEX);)",genKey);
                    auto resp = nebulaSessionPool->execute(gql);

                } else {
                    utils::ByteIteratorMap values;
                    MultiModelWorkload::buildValues(values, keyName);
                    for (const auto &it: values) {
                        value += it.second + ",";
                    }
                    sprintf(gql, R"(UPDATE VERTEX on usertable "%s" set filed = "%s", txnid = "%lu";)",
                            genKey, value.c_str(), tid);
                    auto resp = nebulaSessionPool->execute(gql);

                }
            }
        }
    }
}

