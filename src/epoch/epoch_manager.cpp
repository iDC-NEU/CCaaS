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
    Context EpochManager::ctx;
    volatile uint64_t EpochManager::logical_epoch = 1, EpochManager::physical_epoch = 0, EpochManager::push_down_epoch = 1;
    uint64_t EpochManager::max_length = ctx.kCacheMaxLength, EpochManager::pack_num = ctx.kIndexNum;

    std::vector<std::unique_ptr<std::atomic<bool>>> EpochManager::merge_complete, EpochManager::abort_set_merge_complete,
            EpochManager::commit_complete, EpochManager::record_committed, EpochManager::is_current_epoch_abort;

    AtomicCounters_Cache
            EpochManager::should_merge_txn_num(10, 2),
            EpochManager::merged_txn_num(10, 2),
            EpochManager::should_commit_txn_num(10, 2),
            EpochManager::committed_txn_num(10, 2),
            EpochManager::record_commit_txn_num(10, 2),
            EpochManager::record_committed_txn_num(10, 2);

    AtomicCounters
            EpochManager::epoch_log_lsn(10);

    AtomicCounters_Cache
        EpochManager::server_state(10, 2);

    std::vector<std::unique_ptr<std::atomic<uint64_t>>>
        EpochManager::should_receive_pack_num,
        EpochManager::online_server_num;

    std::vector<std::unique_ptr<concurrent_crdt_unordered_map<std::string, std::string, std::string>>>
            EpochManager::epoch_merge_map, ///epoch merge   row_header
            EpochManager::local_epoch_abort_txn_set,
            EpochManager::epoch_abort_txn_set; /// for epoch final check

    std::vector<std::unique_ptr<concurrent_unordered_map<std::string, std::string>>>
            EpochManager::epoch_insert_set;

    concurrent_unordered_map<std::string, std::string>
            EpochManager::read_version_map, ///read validate for higher isolation
            EpochManager::insert_set,   ///插入集合，用于判断插入是否可以执行成功 check key exits?
            EpochManager::abort_txn_set; /// 所有abort的事务，不区分epoch

    std::vector<std::unique_ptr<std::vector<proto::Transaction>>>
            EpochManager::redo_log;

    std::vector<std::unique_ptr<std::atomic<uint64_t>>>
            EpochManager::received_epoch;

    std::vector<std::unique_ptr<concurrent_unordered_map<std::string, proto::Transaction>>>
            EpochManager::committed_txn_cache; ///committed_txn_cache[epoch][lsn]->txn 用于打包发送给mot

    tikv_client::TransactionClient* EpochManager::tikv_client_ptr = nullptr;

// EpochPhysicalTimerManagerThreadMain中得到的当前微秒级别的时间戳
    uint64_t start_time_ll, start_physical_epoch = 1;
    struct timeval start_time;

// EpochManager是否初始化完成
    std::atomic<bool> init_ok(false);
    std::atomic<bool> is_epoch_advance_started(false), test_start(false);

// 接受client和peer txn node发来的写集，都放在listen_message_queue中
    BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>> listen_message_queue;
    BlockingConcurrentQueue<std::unique_ptr<send_params>> send_to_server_queue, send_to_client_queue, send_to_storage_queue;
    BlockingConcurrentQueue<std::unique_ptr<proto::Message>> request_queue, raft_message_queue;

// client发来的写集会放到local_txn_queue中
    BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>> merge_queue;///存放要进行merge的事务，分片
    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>>
        epoch_local_txn_queue,///存放epoch由client发送过来的事务，存放每个epoch要进行写日志的事务，整个事务写日志
        epoch_commit_queue,///存放每个epoch要进行写日志的事务，分片写日志
        epoch_redo_log_queue;///存放完成的事务 发送给tikv或其他只提供接口的系统，进行日志下推, 按照epoch先后进行

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
        //==========Logical Epoch Merge State=============
        EpochManager::merge_complete.resize(EpochManager::max_length);
        EpochManager::abort_set_merge_complete.resize(EpochManager::max_length);
        EpochManager::commit_complete.resize(EpochManager::max_length);
        EpochManager::record_committed.resize(EpochManager::max_length);
        EpochManager::is_current_epoch_abort.resize(EpochManager::max_length);

        EpochManager::should_merge_txn_num.Init(EpochManager::max_length, EpochManager::pack_num),
        EpochManager::merged_txn_num.Init(EpochManager::max_length, EpochManager::pack_num),
        EpochManager::should_commit_txn_num.Init(EpochManager::max_length, EpochManager::pack_num),
        EpochManager::committed_txn_num.Init(EpochManager::max_length, EpochManager::pack_num),
        EpochManager::record_commit_txn_num.Init(EpochManager::max_length, EpochManager::pack_num),
        EpochManager::record_committed_txn_num.Init(EpochManager::max_length, EpochManager::pack_num);
        EpochManager::epoch_log_lsn.Init(EpochManager::max_length);

        EpochManager::epoch_merge_map.resize(EpochManager::max_length);
        EpochManager::local_epoch_abort_txn_set.resize(EpochManager::max_length);
        EpochManager::epoch_abort_txn_set.resize(EpochManager::max_length);
        EpochManager::epoch_insert_set.resize(EpochManager::max_length);

        epoch_local_txn_queue.resize(EpochManager::max_length);
        epoch_commit_queue.resize(EpochManager::max_length);
        epoch_redo_log_queue.resize(EpochManager::max_length);
        //redo_log
        EpochManager::committed_txn_cache.resize(EpochManager::max_length);
        EpochManager::redo_log.resize(EpochManager::max_length);

        //cluster state
        EpochManager::online_server_num.reserve(EpochManager::max_length + 1);
        EpochManager::should_receive_pack_num.reserve(EpochManager::max_length + 1);
        EpochManager::server_state.Init(EpochManager::max_length,(int)ctx.kServerIp.size() + 2);
        //cache server
        EpochManager::received_epoch.reserve(EpochManager::max_length + 1);
        uint64_t val = 1;
        if(ctx.is_cache_server_available) {
            val = 0;
        }
        for(int i = 0; i < static_cast<int>(EpochManager::max_length); i ++) {
            EpochManager::merge_complete[i] = std::make_unique<std::atomic<bool>>(false);
            EpochManager::abort_set_merge_complete[i] = std::make_unique<std::atomic<bool>>(false);
            EpochManager::commit_complete[i] = std::make_unique<std::atomic<bool>>(false);
            EpochManager::record_committed[i] = std::make_unique<std::atomic<bool>>(false);
            EpochManager::is_current_epoch_abort[i] = std::make_unique<std::atomic<bool>>(false);

            EpochManager::received_epoch.emplace_back(std::make_unique<std::atomic<uint64_t>>(val));
            EpochManager::epoch_merge_map[i] = std::make_unique<concurrent_crdt_unordered_map<std::string, std::string, std::string>>();
            EpochManager::local_epoch_abort_txn_set[i] = std::make_unique<concurrent_crdt_unordered_map<std::string, std::string, std::string>>();
            EpochManager::epoch_abort_txn_set[i] = std::make_unique<concurrent_crdt_unordered_map<std::string, std::string, std::string>>();
            EpochManager::epoch_insert_set[i] = std::make_unique<concurrent_unordered_map<std::string, std::string>>();
            EpochManager::committed_txn_cache[i] = std::make_unique<concurrent_unordered_map<std::string, proto::Transaction>>();
            EpochManager::redo_log[i] = std::make_unique<std::vector<proto::Transaction>>();

            epoch_local_txn_queue[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
            epoch_commit_queue[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
            epoch_redo_log_queue[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();

            //cluster state
            EpochManager::online_server_num[i] = std::make_unique<std::atomic<uint64_t>>(ctx.kTxnNodeNum);
            EpochManager::should_receive_pack_num[i] = std::make_unique<std::atomic<uint64_t>>(ctx.kTxnNodeNum - 1);
            //server state
            for(int j = 0; j < (int)ctx.kTxnNodeNum; j++) {
                EpochManager::server_state.SetCount(i, j, 1);
            }

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
        struct timeval current_time{};
        uint64_t current_time_ll;
        gettimeofday(&current_time, nullptr);
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
    void OUTPUTLOG(const string& s, uint64_t& epoch_mod){
        epoch_mod %= EpochManager::max_length;
        printf("%50s physical %8lu, logical %8lu, \
        epoch_mod %8lu, disstance %8lu, \
        MergedTxnNum %8lu, ShouldMergeTxnNum %8lu, \
        CommittedTxnNum %8lu, ShouldCommitTxnNum %8lu, \
        RecordCommit %8lu, RecordCommitted %8lu, \
\
        ShouldReceiveShardingPackNum %8lu, ReceivedShardingPackNum %8lu\
        ShouldReceiveShardingTxnNum %8lu, ReceivedShardingTxnNum %8lu\
\
        ShouldReceiveBackUpPackNum %8lu, ReceivedBackUpPackNum %8lu\
        ShouldReceiveBackUpTxnNum %8lu, ReceivedBackUpTxnNum %8lu\
        ReceivedBackupACKNum %8lu\
\
        ReceivedInsertSetNum %8lu, ReceivedAbortSetNum %8lu\
        ReceivedInsertSetACKNum %8lu, ReceivedAbortSetACKNum %8lu\
\
        merge_num %8lu, \
        time %lu \n",
               s.c_str(),
               EpochManager::GetPhysicalEpoch(), EpochManager::GetLogicalEpoch(),
               epoch_mod, EpochManager::GetPhysicalEpoch() - EpochManager::GetLogicalEpoch(),

               EpochManager::merged_txn_num.GetCount(epoch_mod), EpochManager::should_merge_txn_num.GetCount(epoch_mod),
               EpochManager::committed_txn_num.GetCount(epoch_mod), EpochManager::should_commit_txn_num.GetCount(epoch_mod),
               EpochManager::record_committed_txn_num.GetCount(epoch_mod), EpochManager::record_commit_txn_num.GetCount(epoch_mod),
               MessageReceiveHandler::sharding_should_receive_pack_num.GetCount(epoch_mod), MessageReceiveHandler::sharding_received_pack_num.GetCount(epoch_mod),
               MessageReceiveHandler::sharding_should_receive_txn_num.GetCount(epoch_mod), MessageReceiveHandler::sharding_received_txn_num.GetCount(epoch_mod),

               MessageReceiveHandler::backup_should_receive_pack_num.GetCount(epoch_mod), MessageReceiveHandler::backup_received_pack_num.GetCount(epoch_mod),
               MessageReceiveHandler::backup_should_receive_txn_num.GetCount(epoch_mod), MessageReceiveHandler::backup_received_txn_num.GetCount(epoch_mod),
               MessageReceiveHandler::backup_received_ack_num.GetCount(epoch_mod),

               MessageReceiveHandler::insert_set_received_num.GetCount(epoch_mod), MessageReceiveHandler::sharding_received_abort_set_num.GetCount(epoch_mod),
               MessageReceiveHandler::insert_set_received_ack_num.GetCount(epoch_mod), MessageReceiveHandler::sharding_abort_set_received_ack_num.GetCount(epoch_mod),

               merge_num.load(),
               now_to_us());
        fflush(stdout);
    }

    bool CheckEpochMergeState(uint64_t &epoch, Context& ctx) {
        for(auto i = epoch; i < EpochManager::GetPhysicalEpoch(); i ++) {
            if( (ctx.kTxnNodeNum == 1 ||
                    (MessageReceiveHandler::IsRemoteShardingPackReceiveComplete(i, ctx) &&
                    MessageReceiveHandler::IsRemoteShardingTxnReceiveComplete(i, ctx) &&
                    MessageReceiveHandler::IsShardingACKReceiveComplete(i, ctx) &&
                    MessageReceiveHandler::IsBackUpSendFinish(i, ctx) &&
                    MessageReceiveHandler::IsBackUpACKReceiveComplete(i, ctx) )
                ) &&
                MessageReceiveHandler::IsEpochTxnEnqueued_MergeQueue(i, ctx) &&
                EpochManager::should_merge_txn_num.GetCount(i) <= EpochManager::merged_txn_num.GetCount(i)
            ) {
                EpochManager::SetShardingMergeComplete(i, true);
            }
        }
        while(EpochManager::IsShardingMergeComplete(epoch)) epoch++;
        return true;
    }

    bool CheckEpochAbortSetState(uint64_t &epoch, Context& ctx) {
        for(auto i = epoch; i < EpochManager::GetPhysicalEpoch(); i ++) {
            if( (ctx.kTxnNodeNum == 1 ||
                    (MessageReceiveHandler::IsAbortSetACKReceiveComplete(i, ctx) &&
                    MessageReceiveHandler::IsRemoteAbortSetReceiveComplete(i, ctx) )
                ) &&
                EpochManager::IsShardingMergeComplete(i)
               ) {
                EpochManager::SetAbortSetMergeComplete(i, true);
            }
        }
        while(EpochManager::IsAbortSetMergeComplete(epoch)) epoch++;
        return true;
    }

    bool CheckEpochCommitState(uint64_t &epoch, Context& ctx) {
        for(auto i = epoch; i < EpochManager::GetPhysicalEpoch(); i ++) {
            if( EpochManager::IsAbortSetMergeComplete(i) &&
                MessageReceiveHandler::IsEpochTxnEnqueued_LocalTxnQueue(i, ctx) &&
                EpochManager::should_commit_txn_num.GetCount(i) <= EpochManager::committed_txn_num.GetCount(i)
                ) {
                EpochManager::SetCommitComplete(i, true);
            }
        }
        while(EpochManager::IsCommitComplete(epoch)) epoch++;
        return true;
    }

    bool CheckAndSetRedoLogPushDownState(uint64_t& epoch, Context& ctx) {
        if(EpochManager::IsCommitComplete(epoch) &&
            EpochManager::record_committed_txn_num.GetCount(epoch) >= EpochManager::record_commit_txn_num.GetCount(epoch) &&
            MessageReceiveHandler::IsRedoLogPushDownACKReceiveComplete(epoch, ctx)) {
            EpochManager::SetRecordCommitted(epoch, true);
            epoch ++;
            return true;
        }
        return false;
    }

    void EpochLogicalTimerManagerThreadMain(uint64_t id, Context ctx){
        SetCPU();
        uint64_t epoch = 1, cache_server_available = 1, total_commit_txn_num = 0,
                merge_epoch = 1, abort_set_epoch = 1, commit_epoch = 1, redo_log_epoch = 1, clear_epoch = 1;
        bool sleep_flag = false;
        while(!init_ok.load()) usleep(20);
        if(ctx.is_cache_server_available) {
            cache_server_available = 0;
        }
        while(!EpochManager::IsTimerStop()){
            while(EpochManager::GetPhysicalEpoch() <= EpochManager::GetLogicalEpoch() + ctx.kDelayEpochNum) usleep(20);
            sleep_flag = false;
            sleep_flag = sleep_flag | CheckEpochMergeState(merge_epoch, ctx);
            sleep_flag = sleep_flag | CheckEpochAbortSetState(abort_set_epoch, ctx);
            sleep_flag = sleep_flag | CheckEpochCommitState(commit_epoch, ctx);
            sleep_flag = sleep_flag | CheckAndSetRedoLogPushDownState(redo_log_epoch, ctx);

            while(epoch < commit_epoch) {
                total_commit_txn_num += EpochManager::record_committed_txn_num.GetCount(epoch);
                OUTPUTLOG("=============完成一个Epoch的合并===== ", epoch);
                epoch ++;
                EpochManager::ClearLog(epoch); //清空next epoch的redo_log信息
                EpochManager::AddLogicalEpoch();
            }

            while(clear_epoch < redo_log_epoch) {
                OUTPUTLOG("=============完成一个Epoch的Log Push Down===== ", clear_epoch);
                EpochManager::ClearMergeEpochState(clear_epoch); //清空当前epoch的merge信息
                EpochManager::SetCacheServerStored(clear_epoch, cache_server_available);
                clear_epoch ++;
                EpochManager::AddPushDownEpoch();
            }
            if(!sleep_flag) usleep(20);
        }
        printf("total commit txn num: %lu\n", total_commit_txn_num);
    }

/**
 * @brief 按照物理时间戳推荐物理epoch号
 *
 * @param ctx
 */
    void EpochPhysicalTimerManagerThreadMain(uint64_t id, Context ctx) {
        SetCPU();
        InitEpochTimerManager(ctx);
        //==========同步============
        zmq::message_t message;
        zmq::context_t context(1);
        zmq::socket_t request_puller(context, ZMQ_PULL);
        request_puller.bind("tcp://*:5546");
//    request_puller.recv(&message);
        gettimeofday(&start_time, nullptr);
        start_time_ll = start_time.tv_sec * 1000000 + start_time.tv_usec;
        if(ctx.is_sync_start) {
            auto sleep_time = static_cast<uint64_t>((((start_time.tv_sec / 60) + 1) * 60) * 1000000);
            usleep(sleep_time - start_time_ll);
            gettimeofday(&start_time, nullptr);
            start_time_ll = start_time.tv_sec * 1000000 + start_time.tv_usec;
        }

        test_start.store(true);
        is_epoch_advance_started.store(true);

        printf("EpochTimerManager 同步完成，数据库开始正常运行\n");
        while(!EpochManager::IsTimerStop()){
            usleep(GetSleeptime(ctx));
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