//
// Created by zwx on 23-8-29.
//

#include "workload/mot.h"
#include "tools/utilities.h"
#include "workload/multi_model_workload.h"

#include "glog/logging.h"

namespace workload {


    class MOTConnectionPool {
    private:
        static std::vector<std::unique_ptr<SQLHENV>> motEnv;
        static std::vector<std::unique_ptr<SQLHDBC>> motHdbc;
        static std::atomic<uint64_t> count;
        static uint64_t connectionNum;

    public:
        static void Init();
        static void ExecSQL(SQLCHAR *sql);
        static void CloseDB();
    };

    std::vector<std::unique_ptr<SQLHENV>> MOTConnectionPool::motEnv;
    std::vector<std::unique_ptr<SQLHDBC>> MOTConnectionPool::motHdbc;
    std::atomic<uint64_t> MOTConnectionPool::count(0);
    uint64_t MOTConnectionPool::connectionNum(1);

    void MOTConnectionPool::Init() {
        connectionNum = MultiModelWorkload::ctx.multiModelContext.kClientNum;
        for(int i = 0; i < (int)connectionNum; i ++) {
            motEnv.push_back(std::make_unique<SQLHENV>());
            motHdbc.push_back(std::make_unique<SQLHDBC>());
            auto& env = *motEnv[i];
            auto& conn = *motHdbc[i];
            env = SQL_NULL_HENV;
            conn = SQL_NULL_HDBC;
            RETCODE retcode;//错误返回码
            // Allocate the ODBC Environment and save handle.
            //  1. 申请环境句柄
            retcode = SQLAllocHandle (SQL_HANDLE_ENV, nullptr, &env);
            if(retcode < 0 ) {//错误处理
                LOG(FATAL) << "allocate ODBC Environment handle errors.";
                return ;
            }
            // Notify ODBC that this is an ODBC 3.0 application.
            //2. 设置环境属性
            retcode = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                                    (SQLPOINTER) SQL_OV_ODBC3, SQL_IS_INTEGER);
            if(retcode < 0 ) {//错误处理
                LOG(FATAL) << "the ODBC is not version3.0 ";
                return ;
            }
            // Allocate an ODBC connection and connect.
            //3. 申请连接句柄
            retcode = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);
            if(retcode < 0 ) {//错误处理
                LOG(FATAL) << "allocate ODBC connection handle errors.";
                return ;
            }
            //4. 设置连接属性
            retcode = SQLSetConnectAttr(conn, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER) SQL_AUTOCOMMIT_ON, 0);
            if(retcode < 0 ) { //错误处理
                LOG(FATAL) << "SQLSetConnectAttr AutoCommit failed";
                return ;
            }
            retcode = SQLSetConnectAttr(conn, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);
            if(retcode < 0 ) {
                LOG(FATAL) << "SQL SetConnectAttr SQL_LOGIN_TIMEOUT failed";
                return ;
            }
            //Data Source Name must be of type User DNS or System DNS
            char* DsnName = (char*)"MPPODBC";
            char* DsnUID = (char*)"jack";//log name
            char* DsnAuthStr = (char*)"Test@123";//passward
            //connect to the Data Source
            //5. 连接数据源
            retcode=SQLConnect(conn, (SQLCHAR*)DsnName, (SWORD)strlen(DsnName),
                               (SQLCHAR*)DsnUID, (SWORD)strlen(DsnUID), (SQLCHAR*)DsnAuthStr,
                               (SWORD)strlen(DsnAuthStr));
            if(retcode < 0 ) {//错误处理
                LOG(FATAL) << "connect to ODBC datasource errors.";
                return ;
            }
            else {
                LOG(INFO) << "connect to ODBC datasource done.";
            }
        }
    }

    void MOTConnectionPool::ExecSQL(SQLCHAR *sql) {
        auto id = count.fetch_add(1);
        auto& conn = *motHdbc[id % connectionNum];
        SQLRETURN retcode;                  // Return status
        SQLHSTMT hstmt = SQL_NULL_HSTMT;    // Statement handle
        // Allocate Statement Handle
        retcode = SQLAllocHandle(SQL_HANDLE_STMT, conn, &hstmt);
        if (!SQL_SUCCEEDED(retcode)) {
            printf("SQLAllocHandle(SQL_HANDLE_STMT) failed");
            return;
        }
        // Prepare Statement
        // retcode = SQLPrepare(hstmt, (SQLCHAR*) sql, SQL_NTS);
        // if (!SQL_SUCCEEDED(retcode)) {
        //     printf("SQLPrepare(hstmt, (SQLCHAR*) sql, SQL_NTS) failed");
        //     return;
        // }
        // Execute Statement
        // retcode = SQLExecute(hstmt);
        // if (!SQL_SUCCEEDED(retcode)) {
        //     std::cout<<"SQLExecute(hstmt) failed"<<sql<<std::endl;
        //     printf("SQLExecute(hstmt) failed \n");
        //     return;
        // }
        // Free Handle
        retcode = SQLExecDirect(hstmt, sql, SQL_NTS);
        if (!SQL_SUCCEEDED(retcode)) {
            LOG(INFO) << "failed to exec:" << sql << "\n";
            return;
        }

        retcode = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        if (!SQL_SUCCEEDED(retcode)) {
            printf("SQLFreeHandle(SQL_HANDLE_STMT, hstmt) failed");
            return;
        }
    }

    void MOTConnectionPool::CloseDB() {
        connectionNum = MultiModelWorkload::ctx.multiModelContext.kClientNum;
        for(int i = 0; i < (int)connectionNum; i ++) {
            auto &env = *motEnv[i];
            auto &conn = *motHdbc[i];
            SQLDisconnect(conn);
            SQLFreeHandle(SQL_HANDLE_DBC, conn);
            SQLFreeHandle(SQL_HANDLE_ENV, env);
        }
    }


    void MOT::Init() {
        MOTConnectionPool::Init();
        MOTConnectionPool::ExecSQL((SQLCHAR *)
            "CREATE Foreign TABLE IF NOT EXISTS usertable(key VARCHAR(64) PRIMARY KEY, filed0 VARCHAR(64), filed1 VARCHAR(64), " \
            "filed2 VARCHAR(64), filed3 VARCHAR(64), filed4 VARCHAR(64), filed5 VARCHAR(64), filed6 VARCHAR(64)," \
            "filed7 VARCHAR(64), filed8 VARCHAR(64), filed9 VARCHAR(64), txnid VARCHAR(64));");
        LOG(INFO) << "MOT Exec:" << "CREATE Foreign TABLE IF NOT EXISTS usertable(key VARCHAR(64) PRIMARY KEY, filed0 VARCHAR(64), filed1 VARCHAR(64), " \
            "filed2 VARCHAR(64), filed3 VARCHAR(64), filed4 VARCHAR(64), filed5 VARCHAR(64), filed6 VARCHAR(64)," \
            "filed7 VARCHAR(64), filed8 VARCHAR(64), filed9 VARCHAR(64), txnid VARCHAR(64));";
    }

    void MOT::InsertData(const uint64_t& tid) {
        if(tid > MultiModelWorkload::ctx.multiModelContext.kRecordCount) return;
        char genKey[100], sql[5000];
        std::string data = Taas::RandomString(32);
        sprintf(genKey, "usertable_key:%032lu", tid);
        auto keyName = std::string(genKey);
        utils::ByteIteratorMap values;
        MultiModelWorkload::buildValues(values);
        sprintf(sql, R"(INSERT INTO usertable VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');)",
                genKey, values["filed0"].c_str(), values["filed1"].c_str(), values["filed2"].c_str(),
                values["filed3"].c_str(), values["filed4"].c_str(), values["filed5"].c_str(),
                values["filed6"].c_str(), values["filed7"].c_str(), values["filed8"].c_str(),
                values["filed9"].c_str(), ("tid:" + std::to_string(tid)).c_str());
        MOTConnectionPool::ExecSQL((SQLCHAR *)sql);
//        LOG(INFO) << "MOT Exec:" << sql;
    }

    void MOT::RunTxn(const uint64_t& tid, const std::shared_ptr<std::atomic<uint64_t>>& sunTxnNum, std::shared_ptr<std::atomic<uint64_t>>& txn_num) {
        char genKey[100], sql[5000];
        std::string value;
        int cnt, i;
        if(MultiModelWorkload::ctx.multiModelContext.kTestMode == Taas::MultiModelTest) {
            cnt = 4;
        }
        else if(MultiModelWorkload::ctx.multiModelContext.kTestMode == Taas::SQL) {
            cnt = 9;
        }
        else {
            sunTxnNum->fetch_add(1);
            return ;
        }

        {
            bool is_read_only = true;
            std::string txn = "start transaction;";
            for (i = 0; i < cnt; i++) {
                auto opType = MultiModelWorkload::operationChooser->nextValue();
                auto id = MultiModelWorkload::keyChooser[1]->nextValue();
                sprintf(genKey, "usertable_key:%032lu", id);
                auto keyName = std::string(genKey);
                if (opType == Operation::READ) {
                    sprintf(sql, "SELECT * FROM usertable WHERE key = '%s';", genKey);
                    txn += std::string(sql);
                } else {
                    is_read_only = false;
                    utils::ByteIteratorMap values;
                    MultiModelWorkload::buildValues(values);
                    sprintf(sql, R"(update usertable set filed0='%s', filed1='%s', filed2='%s', filed3='%s', filed4='%s', filed5='%s', filed6='%s', filed7 = '%s', filed8='%s', filed9='%s', txnid='%s' where key ='%s';)",
                            values["filed0"].c_str(), values["filed1"].c_str(), values["filed2"].c_str(),
                            values["filed3"].c_str(), values["filed4"].c_str(), values["filed5"].c_str(),
                            values["filed6"].c_str(), values["filed7"].c_str(), values["filed8"].c_str(),
                            values["filed9"].c_str(), ("tid:" + std::to_string(tid)).c_str(), genKey);
                    txn += std::string(sql);
                }
            }
            txn += "commit;";
            if(!is_read_only) txn_num->fetch_add(1);
            /// todo : add a counter to notify RunMultiTxn sub txn gql send
            MOTConnectionPool::ExecSQL((SQLCHAR *)txn.c_str());
//            LOG(INFO) << "MOT Exec:" << txn.c_str();
//            usleep(5000);
        }

        sunTxnNum->fetch_add(1);
    }

    void MOT::CloseDB() {
        MOTConnectionPool::CloseDB();
    }

}