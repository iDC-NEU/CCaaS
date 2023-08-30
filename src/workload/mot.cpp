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

    std::vector<MOTTxn > MOT::txnVector;

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

    void MOT::Init(const Taas::Context& ctx_) {
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
        retcode=SQLConnect(motHdbc,(SQLCHAR*)DsnName,(SWORD)strlen(DsnName),(SQLCHAR*)DsnUID, (SWORD)strlen(DsnUID),(SQLCHAR*)DsnAuthStr, (SWORD)strlen(DsnAuthStr));
        if(retcode < 0 ) {//错误处理
            LOG(FATAL) << "connect to ODBC datasource errors.";
            return ;
        }
        else {
            LOG(INFO) << "connect to ODBC datasource done.";
        }
        ExecSQL(motHdbc, (SQLCHAR *)"CREATE Foreign TABLE IF NOT EXISTS taas_odbc(c_sk INTEGER PRIMARY KEY, c_name VARCHAR(32), c_tid VARCHAR(32));");
    }

    void MOT::GenerateTxn(uint64_t tid) {
        uint64_t num = MultiModelWorkload::ctx.multiModelContext.kOpNum;
        uint64_t randnum = Taas::RandomNumber(0, 100);
        MOTTxn res;
        if (randnum < MultiModelWorkload::ctx.multiModelContext.kWriteNum) {
            while (num--) {
                std::string name = Taas::RandomString(20);
                auto now = tid * MultiModelWorkload::ctx.multiModelContext.kOpNum + num;
                res.push_back("INSERT INTO taas_odbc VALUES(" + std::to_string(now) + ", '" + name
                              + "','tid" + std::to_string(tid) + "');");
            }
        } else {
            while (num--) {
                uint64_t number = Taas::RandomNumber(1, MultiModelWorkload::GetTxnId()*MultiModelWorkload::ctx.multiModelContext.kOpNum);
                res.push_back("SELECT * FROM taas_odbc WHERE c_tid = 'tid" + std::to_string(number) + "';");
            }
        }
        txnVector.push_back(std::move(res));
    }

    void MOT::CloseDB() {
        SQLDisconnect(motHdbc);
        SQLFreeHandle(SQL_HANDLE_DBC,motHdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, motEnv);
    }

    void MOT::RunTxn(const MOTTxn& txn) {
        std::string mot_tempstr;
        mot_tempstr="";
        for(const auto & i : txn) mot_tempstr += i;
        ExecSQL(motHdbc,(SQLCHAR*)(mot_tempstr.c_str()));
    }

    void MOT::InsertData(uint64_t tid) {

    }

}