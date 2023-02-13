//
// Created by 周慰星 on 11/8/22.
//

#include <ctime>
#include "sys/time.h"
#include "string"
#include "tools/utilities.h"
#include "epoch/epoch_manager.h"
#include "message/handler_receive.h"
#include "message/handler_send.h"

namespace Taas {

    using namespace std;

    bool EpochManager::timerStop = false;
    volatile bool EpochManager::merge_complete = false, EpochManager::abort_set_merge_complete = false,
        EpochManager::commit_complete = false, EpochManager::record_committed = false, EpochManager::is_current_epoch_abort = false;
    Context EpochManager::ctx;
    volatile uint64_t
            EpochManager::logical_epoch = 1, EpochManager::physical_epoch = 0, epoch_commit_time = 0;
    // cache_max_length
    uint64_t EpochManager::max_length = ctx.kCacheMaxLength;
// for concurrency , atomic_counters' num
    uint64_t EpochManager::pack_num = ctx.kIndexNum;

    AtomicCounters_Cache
            EpochManager::should_merge_txn_num(10, 2),
            EpochManager::merged_txn_num(10, 2),

            EpochManager::should_commit_txn_num(10, 2),
            EpochManager::committed_txn_num(10, 2),

            EpochManager::record_commit_txn_num(10, 2),
            EpochManager::record_committed_txn_num(10, 2);

    AtomicCounters
        EpochManager::server_state(2),
        EpochManager::epoch_log_lsn(10);

    std::unique_ptr<std::atomic<uint64_t>>
        EpochManager::should_receive_pack_num,
        EpochManager::online_server_num;

    std::vector<std::unique_ptr<concurrent_crdt_unordered_map<std::string, std::string, std::string>>>
            EpochManager::epoch_merge_map,
            EpochManager::local_epoch_abort_txn_set,
            EpochManager::epoch_abort_txn_set;

    std::vector<std::unique_ptr<concurrent_unordered_map<std::string, std::string>>>
            EpochManager::epoch_insert_set;

    concurrent_unordered_map<std::string, std::string>
            EpochManager::read_version_map,
            EpochManager::insert_set,
            EpochManager::abort_txn_set;

    std::vector<std::unique_ptr<std::vector<proto::Transaction>>>
            EpochManager::redo_log;

    std::vector<std::unique_ptr<std::atomic<uint64_t>>>
            EpochManager::received_epoch;

    std::vector<std::unique_ptr<concurrent_unordered_map<std::string, proto::Transaction>>>
            EpochManager::committed_txn_cache; ///committed_txn_cache[epoch][lsn]->txn 用于打包发送给mot

// EpochPhysicalTimerManagerThreadMain中得到的当前微秒级别的时间戳
    uint64_t start_time_ll;
    uint64_t start_physical_epoch = 1, new_start_physical_epoch = 1, new_sleep_time = 10000, start_merge_time = 0, commit_time = 0;
    struct timeval start_time;

// EpochManager是否初始化完成
    std::atomic<bool> init_ok(false);
    std::atomic<bool> is_epoch_advance_started(false), test_start(false);

// 接受client和peer txn node发来的写集，都放在listen_message_queue中
    BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>> listen_message_queue;
    BlockingConcurrentQueue<std::unique_ptr<send_params>> send_to_server_queue, send_to_client_queue, send_to_storage_queue;
    BlockingConcurrentQueue<std::unique_ptr<proto::Message>> request_queue, raft_message_queue;
    BlockingConcurrentQueue<std::unique_ptr<pack_params>> pack_txn_queue;
// client发来的写集会放到local_txn_queue中
    BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>> local_txn_queue, ///存放当前epoch由client发送过来的事务，存放每个epoch要进行写日志的事务，整个事务写日志
                                                                    merge_queue, ///存放每个epoch要进行merge的事务，分片
                                                                    commit_queue;///存放每个epoch要进行写日志的事务，分片写日志


    void InitEpochTimerManager(Context& ctx){
        MessageReceiveHandler::StaticInit(ctx);
        auto msg = make_unique<proto::Message>();
        auto txns = msg->storage_pull_response();
        if(txns.result() == proto::Result::Fail)
            merge_num.store(0);
        // txn_queue.init(3000000);
        //==========Cache===============

        EpochManager::max_length = ctx.kCacheMaxLength;
        EpochManager::pack_num = ctx.kIndexNum;
        //==========Logical Merge=============

        EpochManager::should_merge_txn_num.Init(EpochManager::max_length, EpochManager::pack_num),
        EpochManager::merged_txn_num.Init(EpochManager::max_length, EpochManager::pack_num),
        EpochManager::should_commit_txn_num.Init(EpochManager::max_length, EpochManager::pack_num),
        EpochManager::committed_txn_num.Init(EpochManager::max_length, EpochManager::pack_num),
        EpochManager::record_commit_txn_num.Init(EpochManager::max_length, EpochManager::pack_num),
        EpochManager::record_committed_txn_num.Init(EpochManager::max_length, EpochManager::pack_num);

        EpochManager::online_server_num = std::make_unique<std::atomic<uint64_t>>(ctx.kTxnNodeNum);
        EpochManager::should_receive_pack_num= std::make_unique<std::atomic<uint64_t>>(ctx.kTxnNodeNum - 1);

        EpochManager::server_state.Init((int)ctx.kServerIp.size() + 2);
        EpochManager::epoch_log_lsn.Init((int)EpochManager::max_length);

        //server state
        for(int j = 0; j < (int)ctx.kTxnNodeNum; j++) {
            EpochManager::server_state.SetCount(j, 1);
        }

        //remote cache
        EpochManager::received_epoch.reserve(EpochManager::max_length + 1);
        uint64_t val = 1;
        if(ctx.is_cache_server_available) {
            val = 0;
        }

        //epoch merge
        EpochManager::epoch_merge_map.resize(EpochManager::max_length);
        EpochManager::local_epoch_abort_txn_set.resize(EpochManager::max_length);
        EpochManager::epoch_abort_txn_set.resize(EpochManager::max_length);
        EpochManager::epoch_insert_set.resize(EpochManager::max_length);
        for(int i = 0; i < static_cast<int>(EpochManager::max_length); i ++) {
            EpochManager::received_epoch.emplace_back(std::make_unique<std::atomic<uint64_t>>(val));
            EpochManager::epoch_merge_map[i] = std::make_unique<concurrent_crdt_unordered_map<std::string, std::string, std::string>>();
            EpochManager::local_epoch_abort_txn_set[i] = std::make_unique<concurrent_crdt_unordered_map<std::string, std::string, std::string>>();
            EpochManager::epoch_abort_txn_set[i] = std::make_unique<concurrent_crdt_unordered_map<std::string, std::string, std::string>>();
            EpochManager::epoch_insert_set[i] = std::make_unique<concurrent_unordered_map<std::string, std::string>>();
        }

        //redo_log
        EpochManager::epoch_log_lsn.SetCount(0);
        EpochManager::committed_txn_cache.resize(EpochManager::max_length);
        EpochManager::redo_log.resize(EpochManager::max_length);
        for(int i = 0; i < static_cast<int>(EpochManager::max_length); ++i) {
            EpochManager::committed_txn_cache[i] = std::make_unique<concurrent_unordered_map<std::string, proto::Transaction>>();
            EpochManager::redo_log[i] = std::make_unique<std::vector<proto::Transaction>>();
        }

        EpochManager::AddPhysicalEpoch();
        init_ok.store(true);
    }


/**
 * @brief 根据配置信息计算得到间隔多长时间增加epoch号
 *
 * @param ctx XML中的配置信息
 * @return uint64_t 微妙级的时间戳
 */
    uint64_t GetSleeptime(Context& ctx){
        uint64_t sleep_time;
        // current_time由两部分组成，tv_sec + tv_usec，代表秒和毫秒数，合起来就是总的时间戳
        struct timeval current_time;
        uint64_t current_time_ll;
        gettimeofday(&current_time, NULL);
        // 得到目前的微秒级时间戳
        current_time_ll = current_time.tv_sec * 1000000 + current_time.tv_usec;
        sleep_time = current_time_ll - (start_time_ll + (long)(EpochManager::GetPhysicalEpoch() - start_physical_epoch) * ctx.kEpochSize_us);
        if(sleep_time >= ctx.kEpochSize_us){
            return 0;
        }
        else{
            return ctx.kEpochSize_us - sleep_time;
        }
    }

/**
 * @brief 打印目前的系统变量相关信息
 *
 * @param s 待输出的自定义字符串
 * @param epoch_mod epoch号
 */
    void OUTPUTLOG(string s, uint64_t& epoch_mod){
        epoch_mod %= EpochManager::max_length;
        //将当前光标往上移动一行
//    printf("\033[A");
//    //删除光标后面的内容
//    printf("\033[K");
//    if (EpochManager::local_should_exec_txn_num.GetCount(epoch_mod) == 0) {
//        printf("\rnoting to do in this epoch physical %50s %15lu logical %15lu", s.c_str(), EpochManager::GetPhysicalEpoch(), EpochManager::GetLogicalEpoch());
//        fflush(stdout);
//        return;
//    }
/**
 * LocalShouldExecTxnNum %15lu, LocalExecedTxnNum %15lu,
        PackedTxnNum %15lu, ShouldPackTxnNum %15lu,
        ReceivedPackNum %15lu, ShouldReceivePackNum %15lu,
        ReceivedTxnNum %15lu, enqueued_txn_num %15lu ShouldReceiveTxnNum %15lu, \
        */

        printf("%50s physical %15lu, logical %15lu, \
        epoch_mod %llu, %15lu, \
        MergedTxnNum %15lu, ShouldMergeTxnNum %15lu, \
        CommittedTxnNum %15lu, ShouldCommitTxnNum %15lu, \
        record_commit %15lu, record_committed %15lu, \
        merge_num %15lu, \
        time %lu \n",
               s.c_str(),
               EpochManager::GetPhysicalEpoch(), EpochManager::GetLogicalEpoch(),
               epoch_mod, EpochManager::GetPhysicalEpoch() - EpochManager::GetLogicalEpoch(),

               EpochManager::merged_txn_num.GetCount(epoch_mod), EpochManager::should_merge_txn_num.GetCount(epoch_mod),
               EpochManager::committed_txn_num.GetCount(epoch_mod), EpochManager::should_commit_txn_num.GetCount(epoch_mod),
               EpochManager::record_committed_txn_num.GetCount(epoch_mod), EpochManager::record_commit_txn_num.GetCount(epoch_mod),
               merge_num.load(),
               now_to_us());
        fflush(stdout);
    }


    void EpochLogicalTimerManagerThreadMain(uint64_t id, Context ctx){
        SetCPU();
        UNUSED_VALUE(id);
        uint64_t should_merge_txn_num = 0, should_commit_txn_num = 0, cnt = 0,
                epoch = 1, epoch_mod = 1, last_epoch_mod = 0,
                cache_server_available = 1, total_commit_txn_num = 0;
        while(!init_ok.load()) usleep(100);
        if(ctx.is_cache_server_available)
            cache_server_available = 0;
        if(ctx.kTxnNodeNum == 1) {
            while(!EpochManager::IsTimerStop()){
                while(EpochManager::GetPhysicalEpoch() <= EpochManager::GetLogicalEpoch() + ctx.kDelayEpochNum) usleep(100);
                while(!MessageReceiveHandler::IsRemoteShardingPackReceiveComplete(epoch, ctx)) {
                    cnt++;
                    if(cnt % 100 == 0){
                        OUTPUTLOG("============等待远端pack接收完成===== " , epoch_mod);
                    }
                    usleep(100);
                }
                while(!MessageReceiveHandler::IsRemoteShardingTxnReceiveComplete(epoch, ctx)) {
                    cnt++;
                    if (cnt % 100 == 0) {
                        OUTPUTLOG("======等待远端txn接收完成===== ", epoch_mod);
                    }
                    usleep(100);
                }

                while(!MessageReceiveHandler::IsEpochTxnEnqueued_MergeQueue(epoch, ctx)) {
                    cnt++;
                    if (cnt % 100 == 0) {
                        OUTPUTLOG("======等待txn进入merge_queue===== ", epoch_mod);
                    }
                    usleep(100);
                }
                while(EpochManager::should_merge_txn_num.GetCount(epoch) >
                      EpochManager::merged_txn_num.GetCount(epoch)) {
                    cnt++;
                    if(cnt % 100 == 0){
                        OUTPUTLOG("=============等待所有事务 本地和远端事务 merge完成======= " , epoch_mod);
                    }
                    usleep(100);
                }

//                while(!MessageReceiveHandler::IsBackUpSendFinish(epoch, ctx)) {
//                    cnt++;
//                    if(cnt % 100 == 0){
//                        OUTPUTLOG("=============等待local server 备份send完成======= " , epoch_mod);
//                    }
//                    usleep(100);
//                }

//                while(!MessageReceiveHandler::IsBackUpACKReceiveComplete(epoch, ctx)) {
//                    cnt++;
//                    if(cnt % 100 == 0){
//                        OUTPUTLOG("=============等待remote server 备份接收完成======= " , epoch_mod); ///接收到follower的ack
//                    }
//                    usleep(100);
//                }

                ///send abort set
                MessageSendHandler::SendTaskToPackThread(ctx, epoch, 0, proto::TxnType::AbortSet);///发送abort set 任务

//                while(!MessageReceiveHandler::IsAbortSetACKReceiveComplete(epoch, ctx)) {
//                    cnt++;
//                    if(cnt % 100 == 0){
//                        OUTPUTLOG("=============等待AbortSet 发送完成======= " , epoch_mod); ///接收到follower的ack
//                    }
//                    usleep(100);
//                }

//                while(!MessageReceiveHandler::IsRemoteAbortSetReceiveComplete(epoch, ctx)) {
//                    cnt++;
//                    if(cnt % 100 == 0){
//                        OUTPUTLOG("=============等待AbortSet 接收完成======= " , epoch_mod);
//                    }
//                    usleep(100);
//                }
                EpochManager::SetMergeComplete(true);
//        OUTPUTLOG("==进行一个Epoch的合并 merge 完成====== " , epoch_mod);
                // ======= Merge结束 开始Commit ============
                EpochManager::SetRecordCommitted(false);
                EpochManager::SetCommitComplete(false);
                EpochManager::SetAbortSetMergeComplete(true);

//        OUTPUTLOG("==进行一个Epoch的合并 merge 完成====== " , epoch_mod);
                while(!MessageReceiveHandler::IsEpochTxnEnqueued_LocalTxnQueue(epoch, ctx)) {
                    cnt++;
                    if (cnt % 100 == 0) {
                        OUTPUTLOG("======等待txn进入local_txn_queue===== ", epoch_mod);
                    }
                    usleep(100);
                }

                while(EpochManager::should_commit_txn_num.GetCount(epoch) >
                      EpochManager::committed_txn_num.GetCount(epoch)) {
                    cnt++;
                    if(cnt % 100 == 0){
                        OUTPUTLOG("==进行一个Epoch的合并 commit========= " , epoch_mod);
                    }
                    usleep(100);
                }

                ///send insert set
//            MessageSendHandler::SendTaskToPackThread(ctx, epoch, 0, proto::TxnType::InsertSet);///异步 发送insert set 任务
//            while(!MessageReceiveHandler::IsRemoteInsertSetReceiveComplete(epoch_mod, ctx)) {//异步
//                cnt++;
//                if(cnt % 100 == 0){
//                    OUTPUTLOG("=============等待InsertSet 接收完成======= " , epoch_mod); ///接收到follower的ack
//                }
//                usleep(100);
//            }
//            while(!MessageReceiveHandler::IsInsertSetACKReceiveComplete(epoch_mod, ctx)) {//异步
//                cnt++;
//                if(cnt % 100 == 0){
//                    OUTPUTLOG("=============等待InsertSet send完成======= " , epoch_mod); ///接收到follower的ack
//                }
//                usleep(100);
//            }


                EpochManager::SetRecordCommitted(true);
                total_commit_txn_num += EpochManager::record_committed_txn_num.GetCount(epoch);
                OUTPUTLOG("=============完成一个Epoch的合并===== ", epoch_mod);

                // ============= 结束处理 ==================
                EpochManager::ClearMergeEpochState(epoch); //清空当前epoch的merge信息
                EpochManager::SetCacheServerStored(epoch, cache_server_available);
                last_epoch_mod = epoch_mod;
                epoch ++;
                epoch_mod = epoch % EpochManager::max_length;
                EpochManager::ClearLog(epoch); //清空next epoch的redo_log信息
                merge_num.store(0);
                EpochManager::AddLogicalEpoch();
                epoch_commit_time = commit_time = now_to_us();

            }
        }
        else {
            while(!EpochManager::IsTimerStop()){
                while(EpochManager::GetPhysicalEpoch() <= EpochManager::GetLogicalEpoch() + ctx.kDelayEpochNum) usleep(100);
                while(!MessageReceiveHandler::IsRemoteShardingPackReceiveComplete(epoch, ctx)) {
                    cnt++;
                    if(cnt % 100 == 0){
                        OUTPUTLOG("============等待远端pack接收完成===== " , epoch_mod);
                    }
                    usleep(100);
                }
                while(!MessageReceiveHandler::IsRemoteShardingTxnReceiveComplete(epoch, ctx)) {
                    cnt++;
                    if (cnt % 100 == 0) {
                        OUTPUTLOG("======等待远端txn接收完成===== ", epoch_mod);
                    }
                    usleep(100);
                }

                while(!MessageReceiveHandler::IsEpochTxnEnqueued_MergeQueue(epoch, ctx)) {
                    cnt++;
                    if (cnt % 100 == 0) {
                        OUTPUTLOG("======等待txn进入merge_queue===== ", epoch_mod);
                    }
                    usleep(100);
                }
                while(EpochManager::should_merge_txn_num.GetCount(epoch) >
                      EpochManager::merged_txn_num.GetCount(epoch)) {
                    cnt++;
                    if(cnt % 100 == 0){
                        OUTPUTLOG("=============等待所有事务 本地和远端事务 merge完成======= " , epoch_mod);
                    }
                    usleep(100);
                }

                while(!MessageReceiveHandler::IsBackUpSendFinish(epoch, ctx)) {
                    cnt++;
                    if(cnt % 100 == 0){
                        OUTPUTLOG("=============等待local server 备份send完成======= " , epoch_mod);
                    }
                    usleep(100);
                }

                while(!MessageReceiveHandler::IsBackUpACKReceiveComplete(epoch, ctx)) {
                    cnt++;
                    if(cnt % 100 == 0){
                        OUTPUTLOG("=============等待remote server 备份接收完成======= " , epoch_mod); ///接收到follower的ack
                    }
                    usleep(100);
                }

                ///send abort set
                MessageSendHandler::SendTaskToPackThread(ctx, epoch, 0, proto::TxnType::AbortSet);///发送abort set 任务

                while(!MessageReceiveHandler::IsAbortSetACKReceiveComplete(epoch, ctx)) {
                    cnt++;
                    if(cnt % 100 == 0){
                        OUTPUTLOG("=============等待AbortSet 发送完成======= " , epoch_mod); ///接收到follower的ack
                    }
                    usleep(100);
                }

                while(!MessageReceiveHandler::IsRemoteAbortSetReceiveComplete(epoch, ctx)) {
                    cnt++;
                    if(cnt % 100 == 0){
                        OUTPUTLOG("=============等待AbortSet 接收完成======= " , epoch_mod);
                    }
                    usleep(100);
                }
                EpochManager::SetMergeComplete(true);
//        OUTPUTLOG("==进行一个Epoch的合并 merge 完成====== " , epoch_mod);
                // ======= Merge结束 开始Commit ============
                EpochManager::SetRecordCommitted(false);
                EpochManager::SetCommitComplete(false);
                EpochManager::SetAbortSetMergeComplete(true);

//        OUTPUTLOG("==进行一个Epoch的合并 merge 完成====== " , epoch_mod);
                while(!MessageReceiveHandler::IsEpochTxnEnqueued_LocalTxnQueue(epoch, ctx)) {
                    cnt++;
                    if (cnt % 100 == 0) {
                        OUTPUTLOG("======等待txn进入local_txn_queue===== ", epoch_mod);
                    }
                    usleep(100);
                }

                while(EpochManager::should_commit_txn_num.GetCount(epoch) >
                      EpochManager::committed_txn_num.GetCount(epoch)) {
                    cnt++;
                    if(cnt % 100 == 0){
                        OUTPUTLOG("==进行一个Epoch的合并 commit========= " , epoch_mod);
                    }
                    usleep(100);
                }

                ///send insert set
//            MessageSendHandler::SendTaskToPackThread(ctx, epoch, 0, proto::TxnType::InsertSet);///异步 发送insert set 任务
//            while(!MessageReceiveHandler::IsRemoteInsertSetReceiveComplete(epoch_mod, ctx)) {//异步
//                cnt++;
//                if(cnt % 100 == 0){
//                    OUTPUTLOG("=============等待InsertSet 接收完成======= " , epoch_mod); ///接收到follower的ack
//                }
//                usleep(100);
//            }
//            while(!MessageReceiveHandler::IsInsertSetACKReceiveComplete(epoch_mod, ctx)) {//异步
//                cnt++;
//                if(cnt % 100 == 0){
//                    OUTPUTLOG("=============等待InsertSet send完成======= " , epoch_mod); ///接收到follower的ack
//                }
//                usleep(100);
//            }


                EpochManager::SetRecordCommitted(true);
                total_commit_txn_num += EpochManager::record_committed_txn_num.GetCount(epoch);
                OUTPUTLOG("=============完成一个Epoch的合并===== ", epoch_mod);

                // ============= 结束处理 ==================
                EpochManager::ClearMergeEpochState(epoch); //清空当前epoch的merge信息
                EpochManager::SetCacheServerStored(epoch, cache_server_available);
                last_epoch_mod = epoch_mod;
                epoch ++;
                epoch_mod = epoch % EpochManager::max_length;
                EpochManager::ClearLog(epoch); //清空next epoch的redo_log信息
                merge_num.store(0);
                EpochManager::AddLogicalEpoch();
                epoch_commit_time = commit_time = now_to_us();

            }
        }

    }

/**
 * @brief 按照物理时间戳推荐物理epoch号
 *
 * @param id
 * @param ctx
 */
    void EpochPhysicalTimerManagerThreadMain(uint64_t id, Context ctx) {
        SetCPU();
        UNUSED_VALUE(id);
        InitEpochTimerManager(ctx);
        //==========同步============
        zmq::message_t message;
        zmq::context_t context(1);
        zmq::socket_t request_puller(context, ZMQ_PULL);
        request_puller.bind("tcp://*:5546");
//    request_puller.recv(&message);
        gettimeofday(&start_time, NULL);
        start_time_ll = start_time.tv_sec * 1000000 + start_time.tv_usec;
        if(ctx.is_sync_start) {
            uint64_t sleep_time = static_cast<uint64_t>((((start_time.tv_sec / 60) + 1) * 60) * 1000000);
            usleep(sleep_time - start_time_ll);
            gettimeofday(&start_time, NULL);
            start_time_ll = start_time.tv_sec * 1000000 + start_time.tv_usec;
        }

        uint64_t cache_server_available = 1;
        if(ctx.is_cache_server_available)
            cache_server_available = 0;
        test_start.store(true);

        epoch_commit_time = commit_time = now_to_us();
        is_epoch_advance_started.store(true);
        uint64_t epoch = EpochManager::GetPhysicalEpoch(), epoch_mod = EpochManager::GetPhysicalEpoch();
        printf("EpochTimerManager 同步完成，数据库开始正常运行\n");
        while(!EpochManager::IsTimerStop()){
//        OUTPUTLOG("====开始一个Physiacal Epoch, physical info" , EpochManager::GetLogicalEpoch());
            usleep(GetSleeptime(ctx));
//        epoch ++;
//        epoch_mod = epoch % EpochManager::max_length;
//        EpochManager::ClearEpochState(epoch_mod);
//        EpochManager::SetCacheServerStored(epoch_mod, cache_server_available);

            EpochManager::AddPhysicalEpoch();

            if((EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength) ==  ((EpochManager::GetPhysicalEpoch() + 55) % ctx.kCacheMaxLength) ) {
                printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
                printf("+++++++++++++++Fata : Cache Size exceeded!!! +++++++++++++++++++++\n");
                printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
                assert(false);
            }
        }
        printf("EpochTimerManager End!!!\n");
    }
}