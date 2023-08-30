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
    std::vector<NebulaTxn> Nebula::txnVector;

    void Nebula::Init(const Taas::Context& ctx) {
        nebula::ConnectionPool pool;
        auto connectConfig = nebula::Config{};
        connectConfig.maxConnectionPoolSize_ = 1;
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
        nebulaSessionPoolConfig.maxSize_ = 1;
        nebulaSessionPool = std::make_unique<nebula::SessionPool>(nebulaSessionPoolConfig);
        nebulaSessionPool->init();
        resp = nebulaSessionPool->execute("CREATE TAG IF NOT EXISTS person (name string, age int , tid string);");
        assert(resp.errorCode == nebula::ErrorCode::SUCCEEDED);
        resp = nebulaSessionPool->execute("CREATE TAG INDEX IF NOT EXISTS person_index on person(name(10));");
        assert(resp.errorCode == nebula::ErrorCode::SUCCEEDED);
        LOG(INFO) << "=== connect to nebula done ===";
    }

    void Nebula::RunTxn(const std::string &txn) {
        auto resp = nebulaSessionPool->execute(txn);
        std::cout << "Nebula Txn done " << txn << std::endl;
    }

    void Nebula::GenerateTxn(uint64_t tid) {
        uint64_t randnum = Taas::RandomNumber(0, 100);
        if (randnum < MultiModelWorkload::ctx.multiModelContext.kWriteNum) {
            std::string name = Taas::RandomString(20);
            int age = (int)Taas::RandomNumber(10, 90);
            txnVector.push_back("INSERT VERTEX IF NOT EXISTS person(name,age,tid) VALUES \"vid" +
                                                          std::to_string(tid) +  "\" :(\"" + name + "\"," + std::to_string(age) + ",\"vid" + std::to_string(tid) + "\");");
        } else {
            uint64_t number = Taas::RandomNumber(0, MultiModelWorkload::GetTxnId());
            txnVector.push_back("FETCH PROP ON person \"vid" + std::to_string(number) + "\" YIELD properties(VERTEX);");
        }
    }

    void Nebula::InsertData(uint64_t tid) {

    }
}

