//
// Created by user on 23-8-23.
//

#include "workload/multi_model_workload.h"
#include "tools/utilities.h"

#include<glog/logging.h>

namespace workload{
    uint64_t MultiModelWorkload::txn_id = 0, MultiModelWorkload::graph_vid = 0;
    Taas::MultiModelContext MultiModelWorkload::ctx;
    std::unique_ptr<util::thread_pool_light> MultiModelWorkload::thread_pool;
    std::mutex MultiModelWorkload::lock, MultiModelWorkload::_mutex, MultiModelWorkload::resLock;
    nebula::SessionPoolConfig MultiModelWorkload::nebulaSessionPoolConfig;
    std::unique_ptr<nebula::SessionPool> MultiModelWorkload::nebulaSessionPool;
    SQLHENV MultiModelWorkload::MotEnv = SQL_NULL_HENV;
    SQLHDBC MultiModelWorkload::MotHdbc = SQL_NULL_HDBC;

    std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<send_multimodel_params>>> send_multi_txn_queue;
    std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>>> client_listen_taas_message_queue;
    uint64_t commitTxnId;
    std::vector<uint64_t> execTimes;
    std::vector<bool> isExe(5,false);
    Taas::concurrent_unordered_map<uint64_t ,bool> multiModelTxnMap;// (txn id,commit cnt)
    Taas::concurrent_unordered_map<uint64_t, std::condition_variable*> cv;

    void MultiModelWorkload::StaticInit(const Taas::MultiModelContext& ctx_) {
        ctx = ctx_;
        txn_id = 0, graph_vid = 0;
        thread_pool = std::make_unique<util::thread_pool_light>(ctx.kClientNum);
        send_multi_txn_queue = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<send_multimodel_params>>>();
        client_listen_taas_message_queue = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>>>();
    }

    void MultiModelWorkload::NebulaInit() {
        nebula::ConnectionPool pool;
        auto connectConfig = nebula::Config{};
        connectConfig.maxConnectionPoolSize_ = 1;
        pool.init({ctx.kNebulaIP}, connectConfig);
        auto session = pool.getSession(ctx.kNebulaUser, ctx.kNebulaPwd);
        assert(session.valid());
        auto resp = session.execute(
                "CREATE SPACE IF NOT EXISTS "+ctx.kNebulaSpace +" (vid_type = FIXED_STRING(30));");
        assert(resp.errorCode == nebula::ErrorCode::SUCCEEDED);

        nebulaSessionPoolConfig.username_ = ctx.kNebulaUser;
        nebulaSessionPoolConfig.password_ = ctx.kNebulaPwd;
        nebulaSessionPoolConfig.addrs_ = {ctx.kNebulaIP};
        nebulaSessionPoolConfig.spaceName_ = ctx.kNebulaSpace;
        nebulaSessionPoolConfig.maxSize_ = 1;
        nebulaSessionPool = std::make_unique<nebula::SessionPool>(nebulaSessionPoolConfig);
        nebulaSessionPool->init();
//        std::vector<std::thread> threads;
        resp = nebulaSessionPool->execute("CREATE TAG IF NOT EXISTS person (name string, age int , tid string);");
        assert(resp.errorCode == nebula::ErrorCode::SUCCEEDED);
        resp = nebulaSessionPool->execute("CREATE TAG INDEX IF NOT EXISTS person_index on person(name(10));");
        assert(resp.errorCode == nebula::ErrorCode::SUCCEEDED);
        LOG(INFO) << "=== connect to nebula done ===";
//        for (auto& t : threads) {
//            t.join();
//        }
    }

    void MultiModelWorkload::MOTInit() {
        RETCODE retcode;//错误返回码
        // Allocate the ODBC Environment and save handle.
        //  1. 申请环境句柄
        retcode = SQLAllocHandle (SQL_HANDLE_ENV, NULL, &MotEnv);
        if(retcode < 0 ) {//错误处理
            LOG(FATAL) << "allocate ODBC Environment handle errors.";
            return ;
        }
        // Notify ODBC that this is an ODBC 3.0 application.
        //2. 设置环境属性
        retcode = SQLSetEnvAttr(MotEnv, SQL_ATTR_ODBC_VERSION,
                                (SQLPOINTER) SQL_OV_ODBC3, SQL_IS_INTEGER);
        if(retcode < 0 ) {//错误处理
            LOG(FATAL) << "the ODBC is not version3.0 ";
            return ;
        }
        // Allocate an ODBC connection and connect.
        //3. 申请连接句柄
        retcode = SQLAllocHandle(SQL_HANDLE_DBC, MotEnv, &MotHdbc);
        if(retcode < 0 ) {//错误处理
            LOG(FATAL) << "allocate ODBC connection handle errors.";
            return ;
        }
        //4. 设置连接属性
        retcode = SQLSetConnectAttr(MotHdbc,SQL_ATTR_AUTOCOMMIT, (SQLPOINTER) SQL_AUTOCOMMIT_ON, 0);
        if(retcode < 0 ) { //错误处理
            LOG(FATAL) << "SQLSetConnectAttr AutoCommit failed";
            return ;
        }
        retcode = SQLSetConnectAttr(MotHdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);
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
        retcode=SQLConnect(MotHdbc,(SQLCHAR*)DsnName,(SWORD)strlen(DsnName),(SQLCHAR*)DsnUID, (SWORD)strlen(DsnUID),(SQLCHAR*)DsnAuthStr, (SWORD)strlen(DsnAuthStr));
        if(retcode < 0 ) {//错误处理
            LOG(FATAL) << "connect to ODBC datasource errors.";
            return ;
        }
        else {
            LOG(INFO) << "connect to ODBC datasource done.";
        }
        MOTExec(MotHdbc, (SQLCHAR *)"CREATE Foreign TABLE IF NOT EXISTS taas_odbc(c_sk INTEGER PRIMARY KEY, c_name VARCHAR(32), c_tid VARCHAR(32));");
    }

    void MultiModelWorkload::MOTExec(SQLHDBC hdbc, SQLCHAR *sql) {
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

    void MultiModelWorkload::MOTClose() {
        SQLDisconnect(MotHdbc);
        SQLFreeHandle(SQL_HANDLE_DBC,MotHdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, MotEnv);
    }


    void MultiModelWorkload::RunMotTxn(MotTxn motTxn) {
        std::string mot_tempstr;
        mot_tempstr="";
        for(int i=0 ; i < (int)motTxn.size();i++) mot_tempstr += motTxn[i];
        MultiModelWorkload::MOTExec(MultiModelWorkload::MotHdbc,(SQLCHAR*)(mot_tempstr.c_str()));
    }

    void MultiModelWorkload::RunNebulaTxn(NebulaTxn nebulaTxn) {
        auto resp = MultiModelWorkload::nebulaSessionPool->execute(nebulaTxn);
        std::cout << "Nebula Txn done " << nebulaTxn << std::endl;
    }

    void MultiModelWorkload::RunMultiTxn(MultiModelTxn txn) {
        txn.stTime = Taas::now_to_us();
        uint64_t cnt = 0;
        std::thread t1,t2;
        if(MultiModelWorkload::ctx.useNebula){
            if (txn.nebulaTxn != ""){
                t1 = std::thread(RunNebulaTxn,txn.nebulaTxn);
                cnt++;
            }
        }
        if(MultiModelWorkload::ctx.useMot){
            if(txn.motTxn.size()!=0){
                t2 = std::thread(RunMotTxn,txn.motTxn);
                cnt++;
            }
        }
        auto msg = std::make_unique<proto::Message>();
        auto apply = msg->mutable_txn();
        if (txn.kvTxn.size() != 0) {
            cnt++;
            for (auto p: txn.kvTxn) {
                proto::Row *row = apply->add_row();
                std::string key = p.key;
                std::string data = p.value;
                row->set_key(key);
                row->set_data(data);
//                    std::cout << "nowkv = " << key << ":" << data << std::endl;
                if (data == "")row->set_op_type(proto::OpType::Read);
                else row->set_op_type(proto::OpType::Insert);
            }
        }
        txn.typeNumber =cnt;
        apply->set_csn(cnt);
        apply->set_client_ip(MultiModelWorkload::ctx.kMultiModelClient);
        apply->set_client_txn_id(txn.tid);
        apply->set_txn_type(proto::TxnType::ClientTxn);
        std::unique_ptr<std::string> serialized_txn_str_ptr(new std::string());
        // auto serialized_txn_str_ptr = std::make_unique<std::string>();
        auto res = Taas::Gzip(msg.get(), serialized_txn_str_ptr.get());
        assert(res);
        // auto tmp = serialized_txn_str_ptr.release();
        send_multi_txn_queue->enqueue(std::move(std::make_unique<send_multimodel_params>(
                txn.tid, serialized_txn_str_ptr.release())));
        send_multi_txn_queue->enqueue(std::move(std::make_unique<send_multimodel_params>(0, nullptr)));

        std::unique_lock<std::mutex> _lock(MultiModelWorkload::_mutex);
        std::condition_variable cv_tmp;
        std::condition_variable* cv_ptr = &cv_tmp;
        cv.insert(txn.tid, cv_ptr);
        cv_tmp.wait(_lock);
        if(MultiModelWorkload::ctx.useNebula) t1.join();
        if(MultiModelWorkload::ctx.useMot) t2.join();
        std::cout<<"done!" <<txn.tid <<std::endl;
        //一个multi事务结束
        std::mutex ato_mtx;
        ato_mtx.lock();
        ++commitTxnId;
        ato_mtx.unlock();
        txn.edTime = Taas::now_to_us();
        execTimes.emplace_back(txn.edTime-txn.stTime);
    }

}