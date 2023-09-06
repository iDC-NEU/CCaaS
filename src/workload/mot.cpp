//
// Created by zwx on 23-8-29.
//

#include "workload/mot.h"
#include "tools/utilities.h"
#include "workload/multi_model_workload.h"

#include "glog/logging.h"

namespace workload {

    static SQLHENV motEnv = SQL_NULL_HENV;
    static SQLHDBC motHdbc = SQL_NULL_HDBC;

    void ExecSQL(SQLHDBC hdbc, SQLCHAR *sql) {
        SQLRETURN retcode;                  // Return status
        SQLHSTMT hstmt = SQL_NULL_HSTMT;    // Statement handle
        // Allocate Statement Handle
        retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
        if (!SQL_SUCCEEDED(retcode)) {
            printf("SQLAllocHandle(SQL_HANDLE_STMT) failed");
            return;
        }
        // Prepare Statement
        retcode = SQLPrepare(hstmt, (SQLCHAR*) sql, SQL_NTS);
        if (!SQL_SUCCEEDED(retcode)) {
            printf("SQLPrepare(hstmt, (SQLCHAR*) sql, SQL_NTS) failed");
            return;
        }
        // Execute Statement
        retcode = SQLExecute(hstmt);
        if (!SQL_SUCCEEDED(retcode)) {
            std::cout<<"SQLExecute(hstmt) failed"<<sql<<std::endl;
            printf("SQLExecute(hstmt) failed \n");
            return;
        }
        // Free Handle
        retcode = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        if (!SQL_SUCCEEDED(retcode)) {
            printf("SQLFreeHandle(SQL_HANDLE_STMT, hstmt) failed");
            return;
        }
    }

    void MOT::Init() {
        RETCODE retcode;//错误返回码
        // Allocate the ODBC Environment and save handle.
        //  1. 申请环境句柄
        retcode = SQLAllocHandle (SQL_HANDLE_ENV, nullptr, &motEnv);
        if(retcode < 0 ) {//错误处理
            LOG(FATAL) << "allocate ODBC Environment handle errors.";
            return ;
        }
        // Notify ODBC that this is an ODBC 3.0 application.
        //2. 设置环境属性
        retcode = SQLSetEnvAttr(motEnv, SQL_ATTR_ODBC_VERSION,
                                (SQLPOINTER) SQL_OV_ODBC3, SQL_IS_INTEGER);
        if(retcode < 0 ) {//错误处理
            LOG(FATAL) << "the ODBC is not version3.0 ";
            return ;
        }
        // Allocate an ODBC connection and connect.
        //3. 申请连接句柄
        retcode = SQLAllocHandle(SQL_HANDLE_DBC, motEnv, &motHdbc);
        if(retcode < 0 ) {//错误处理
            LOG(FATAL) << "allocate ODBC connection handle errors.";
            return ;
        }
        //4. 设置连接属性
        retcode = SQLSetConnectAttr(motHdbc,SQL_ATTR_AUTOCOMMIT, (SQLPOINTER) SQL_AUTOCOMMIT_ON, 0);
        if(retcode < 0 ) { //错误处理
            LOG(FATAL) << "SQLSetConnectAttr AutoCommit failed";
            return ;
        }
        retcode = SQLSetConnectAttr(motHdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);
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
        retcode=SQLConnect(motHdbc,(SQLCHAR*)DsnName,(SWORD)strlen(DsnName),
                           (SQLCHAR*)DsnUID, (SWORD)strlen(DsnUID),(SQLCHAR*)DsnAuthStr,
                           (SWORD)strlen(DsnAuthStr));
        if(retcode < 0 ) {//错误处理
            LOG(FATAL) << "connect to ODBC datasource errors.";
            return ;
        }
        else {
            LOG(INFO) << "connect to ODBC datasource done.";
        }
        ExecSQL(motHdbc, (SQLCHAR *)
            "CREATE Foreign TABLE IF NOT EXISTS usertable(key VARCHAR(32) PRIMARY KEY, filed0 VARCHAR(32), filed1 VARCHAR(32), " \
            "filed2 VARCHAR(32), filed3 VARCHAR(32), filed4 VARCHAR(32), filed5 VARCHAR(32), filed6 VARCHAR(32)" \
            "filed7 VARCHAR(32), filed8 VARCHAR(32), filed9 VARCHAR(32), txnid VARCHAR(32));");
    }

    void MOT::InsertData(uint64_t& tid) {
        if(tid > MultiModelWorkload::ctx.multiModelContext.kRecordCount) return;
        char genKey[100], sql[800];
        std::string data = Taas::RandomString(256);
        sprintf(genKey, "usertable_key:%064lu", tid);
        auto keyName = std::string(genKey);
        utils::ByteIteratorMap values;
        MultiModelWorkload::buildValues(values, keyName);
        sprintf(sql, R"(INSERT INTO usertable VALUES("%s", "%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%lu");)",
                genKey, values["filed0"].c_str(), values["filed1"].c_str(), values["filed2"].c_str(),
                values["filed3"].c_str(), values["filed4"].c_str(), values["filed5"].c_str(), values["filed6"].c_str(),
                values["filed7"].c_str(), values["filed8"].c_str(), values["filed9"].c_str(), tid);
        ExecSQL(motHdbc, (SQLCHAR *)sql);
    }

    void MOT::RunTxn(uint64_t& tid) { /// todo: create mot connection for every thread
        char genKey[100], sql[800];
        std::string value;
        int cnt, i;
        if(MultiModelWorkload::ctx.multiModelContext.kTestMode == Taas::MultiModel) {
            cnt = 4;
        }
        else if(MultiModelWorkload::ctx.multiModelContext.kTestMode == Taas::KV) {
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
                    sprintf(sql, "SELECT * FROM usertable WHERE key = '%s';", genKey);
                    ExecSQL(motHdbc, (SQLCHAR *)sql);
                } else {
                    utils::ByteIteratorMap values;
                    MultiModelWorkload::buildValues(values, keyName);
                    for (const auto &it: values) {
                        value += it.second + ",";
                    }
                }
            }
        }
    }

    void MOT::CloseDB() {
        SQLDisconnect(motHdbc);
        SQLFreeHandle(SQL_HANDLE_DBC,motHdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, motEnv);
    }





}