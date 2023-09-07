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
        resp = nebulaSessionPool->execute("CREATE TAG IF NOT EXISTS usertable (key, string, filed0 string, filed1 string, " \
                                          "filed2 string , filed3 string, filed4 string, filed5 string, filed6 string, "
                                          "filed7 string, filed8 string, filed9 string, tid string);");
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

        sprintf(gql, R"(INSERT VERTEX IF NOT EXISTS usertable(key, filed0, filed1, filed2, filed3, filed4, filed5, filed6, filed7" \
                                "filed8, filed9, txnid) VALUES "%s" :("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%lu");)",
                                genKey, genKey, values["filed0"].c_str(), values["filed1"].c_str(), values["filed2"].c_str(),
                                values["filed3"].c_str(), values["filed4"].c_str(), values["filed5"].c_str(), values["filed6"].c_str(),
                                values["filed7"].c_str(), values["filed8"].c_str(), values["filed9"].c_str(), tid);

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
                    sprintf(gql, R"(UPDATE VERTEX on usertable "%s" set filed0 = %s, filed1 = %s, filed2 = %s, filed3 = %s, filed4 = %s, filed5 = %s, filed6 = %s, filed7 = %s" \
                                "filed8 = %s, filed9 = %s, txnid = %lu;)",
                            genKey, values["filed0"].c_str(), values["filed1"].c_str(), values["filed2"].c_str(),
                            values["filed3"].c_str(), values["filed4"].c_str(), values["filed5"].c_str(), values["filed6"].c_str(),
                            values["filed7"].c_str(), values["filed8"].c_str(), values["filed9"].c_str(), tid);
                    auto resp = nebulaSessionPool->execute(gql);

                }
            }
        }
    }

}

