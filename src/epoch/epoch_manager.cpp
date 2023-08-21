//
// Created by 周慰星 on 11/8/22.
//


#include "tools/utilities.h"
#include "epoch/epoch_manager.h"
#include "message/message.h"
#include "message/epoch_message_receive_handler.h"
#include "storage/redo_loger.h"
#include "transaction/merge.h"
#include "storage/mot.h"

#include "sys/time.h"
#include "string"
#include "algorithm"
#include "storage/tikv.h"

namespace Taas {

    using namespace std;
    const uint64_t sleep_time = 100, logical_sleep_timme = 50, storage_sleep_time = 50, merge_sleep_time = 50, message_sleep_time = 50;
    uint64_t cache_server_available = 1, total_commit_txn_num = 0;
    std::atomic<uint64_t> merge_epoch = 1, abort_set_epoch = 1,
            commit_epoch = 1, redo_log_epoch = 1, clear_epoch = 1;

    bool EpochManager::timerStop = false;
    Context EpochManager::ctx;
    std::atomic<uint64_t> EpochManager::logical_epoch(1), EpochManager::physical_epoch(0), EpochManager::push_down_epoch(1);
    uint64_t EpochManager::max_length = 10000;
    //epoch merge state
    std::vector<std::unique_ptr<std::atomic<bool>>> EpochManager::merge_complete, EpochManager::abort_set_merge_complete,
            EpochManager::commit_complete, EpochManager::record_committed, EpochManager::is_current_epoch_abort;
    //cluster state
    std::vector<std::unique_ptr<std::atomic<uint64_t>>> EpochManager::online_server_num;
    AtomicCounters_Cache EpochManager::server_state(10, 2);
    //cache server
    std::vector<std::unique_ptr<std::atomic<uint64_t>>> EpochManager::cache_server_received_epoch;

// EpochPhysicalTimerManagerThreadMain中得到的当前微秒级别的时间戳
    uint64_t start_time_ll, start_physical_epoch = 1;
    struct timeval start_time;

// EpochManager是否初始化完成
    std::atomic<int> init_ok_num(0);
    std::atomic<bool> is_epoch_advance_started(false), test_start(false);

    void InitEpochTimerManager(const Context& ctx){
        Merger::StaticInit(ctx);
        MessageQueue::StaticInitMessageQueue(ctx);
        EpochMessageSendHandler::StaticInit(ctx);
        EpochMessageReceiveHandler::StaticInit(ctx);
        RedoLoger::StaticInit(ctx);

        EpochManager::max_length = ctx.kCacheMaxLength;
        //==========Logical Epoch Merge State=============
        EpochManager::merge_complete.resize(EpochManager::max_length);
        EpochManager::abort_set_merge_complete.resize(EpochManager::max_length);
        EpochManager::commit_complete.resize(EpochManager::max_length);
        EpochManager::record_committed.resize(EpochManager::max_length);
        EpochManager::is_current_epoch_abort.resize(EpochManager::max_length);
        //cluster state
        EpochManager::online_server_num.resize(EpochManager::max_length + 1);
//        EpochManager::should_receive_pack_num.resize(EpochManager::max_length + 1);
        EpochManager::server_state.Init(EpochManager::max_length,ctx.kTxnNodeNum + 2, 1);
        //cache server
        EpochManager::cache_server_received_epoch.resize(EpochManager::max_length + 1);
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
            //cluster state
            EpochManager::online_server_num[i] = std::make_unique<std::atomic<uint64_t>>();
            EpochManager::online_server_num[i]->store(ctx.kTxnNodeNum);
            //cache server
            EpochManager::cache_server_received_epoch[i] =std::make_unique<std::atomic<uint64_t>>(val);

        }
        init_ok_num.fetch_add(1);
    }


/**
 * @brief 根据配置信息计算得到间隔多长时间增加epoch号
 *
 * @param ctx XML中的配置信息
 * @return uint64_t 微妙级的时间戳
 */
    uint64_t GetSleeptime(Context& ctx){
        uint64_t sleep_time_temp;
        // current_time由两部分组成，tv_sec + tv_usec，代表秒和毫秒数，合起来就是总的时间戳
        struct timeval current_time{};
        uint64_t current_time_ll;
        gettimeofday(&current_time, nullptr);
        // 得到目前的微秒级时间戳
        current_time_ll = current_time.tv_sec * 1000000 + current_time.tv_usec;
        sleep_time_temp = current_time_ll - (start_time_ll + (long)(EpochManager::GetPhysicalEpoch() - start_physical_epoch) * ctx.kEpochSize_us);
        if(sleep_time_temp >= ctx.kEpochSize_us){
            return 0;
        }
        else{
            return ctx.kEpochSize_us - sleep_time_temp;
        }
    }

    std::string PrintfToString(const char* format, ...) {
        char buffer[2048]; // 假设输出不超过1024个字符
        va_list args;
        va_start(args, format);
        std::vsnprintf(buffer, sizeof(buffer), format, args);
        va_end(args);

        return std::string(buffer);
    }

    void OUTPUTLOG(const Context& ctx, const string& s, uint64_t& epoch_){
        auto epoch_mod = epoch_ % EpochManager::max_length;
        LOG(INFO) << PrintfToString("%60s \n\
        physical                     %6lu, logical                      %6lu,   \
        pushdown_mot                 %6lu, pushdownepoch                %6lu  \n\
        merge_epoch                  %6lu, abort_set_epoch              %6lu    \
        commit_epoch                 %6lu, redo_log_epoch               %6lu  \n\
        clear_epoch                  %6lu,                                        \
        epoch_mod                    %6lu, disstance                    %6lu  \n\
        ShardingPackReceiveOK?       %6lu, ShardingTxnReceiveOK?        %6lu    \
        ShardingSendOK?              %6lu, ShardingACKReceiveOK?        %6lu  \n\
        backupSendOK?                %6lu, backupACKReceiveOK?          %6lu,   \
        EnqueueMergeQueue            %6lu, MergeOk                      %6lu  \n\
        IsShardingMergeComplete      %6lu, IsAbortSetMergeComplete      %6lu    \
        IsCommitComplete             %6lu, SetRecordCommitted           %6lu  \n\
\
        MergedTxnNum                 %6lu, ShouldMergeTxnNum            %6lu,   \
        CommittedTxnNum              %6lu, ShouldCommitTxnNum           %6lu,   \
        RecordCommit                 %6lu, RecordCommitted              %6lu, \n\
        ShouldReceiveShardingPackNum %6lu, ReceivedShardingPackNum      %6lu    \
        ShouldReceiveShardingTxnNum  %6lu, ReceivedShardingTxnNum       %6lu  \n\
        ShouldReceiveBackUpPackNum   %6lu, ReceivedBackUpPackNum        %6lu    \
        ShouldReceiveBackUpTxnNum    %6lu, ReceivedBackUpTxnNum         %6lu  \n\
        ShouldReceiveInsertsetNum    %6lu, ReceivedInsertSetNum         %6lu    \
        ShouldReceiveAbortSetNum     %6lu, ReceivedAbortSetNum          %6lu  \n\
        ReceivedShardingACKNum       %6lu, ReceivedBackupACKNum         %6lu    \
        ReceivedInsertSetACKNum      %6lu, ReceivedAbortSetACKNum       %6lu  \n\
        merge_num                    %6lu, time          %lu \n",
       s.c_str(),
       EpochManager::GetPhysicalEpoch(),                                                  EpochManager::GetLogicalEpoch(),
       MOT::pushed_down_epoch.load(),                                                EpochManager::GetPushDownEpoch(),
       merge_epoch.load(), abort_set_epoch.load(), commit_epoch.load(), redo_log_epoch.load(),clear_epoch.load(),
       epoch_mod,                                                                         EpochManager::GetPhysicalEpoch() - EpochManager::GetLogicalEpoch(),
       (uint64_t)EpochMessageReceiveHandler::IsShardingPackReceiveComplete(ctx, epoch_mod),(uint64_t)EpochMessageReceiveHandler::IsShardingTxnReceiveComplete(ctx, epoch_mod),
       (uint64_t)EpochMessageReceiveHandler::IsShardingSendFinish(epoch_mod),                        (uint64_t)EpochMessageReceiveHandler::IsShardingACKReceiveComplete(ctx, epoch_mod),
       (uint64_t)EpochMessageReceiveHandler::IsBackUpSendFinish(epoch_mod),                 (uint64_t)EpochMessageReceiveHandler::IsBackUpACKReceiveComplete(ctx, epoch_mod),
       (uint64_t)EpochMessageReceiveHandler::IsEpochTxnHandleComplete(epoch_mod), (uint64_t)Merger::IsEpochMergeComplete(ctx, epoch_mod),
       (uint64_t)EpochManager::IsShardingMergeComplete(epoch_mod),                  (uint64_t)EpochManager::IsAbortSetMergeComplete(epoch_mod),
       (uint64_t)EpochManager::IsCommitComplete(epoch_mod),                         (uint64_t)EpochManager::IsRecordCommitted(epoch_mod),

       Merger::epoch_merged_txn_num.GetCount(epoch_mod),                            Merger::epoch_should_merge_txn_num.GetCount(epoch_mod),
       Merger::epoch_committed_txn_num.GetCount(epoch_mod),                         Merger::epoch_should_commit_txn_num.GetCount(epoch_mod),
       Merger::epoch_record_committed_txn_num.GetCount(epoch_mod),                  Merger::epoch_record_commit_txn_num.GetCount(epoch_mod),

       EpochMessageReceiveHandler::sharding_should_receive_pack_num.GetCount(epoch_mod), EpochMessageReceiveHandler::sharding_received_pack_num.GetCount(epoch_mod),
       EpochMessageReceiveHandler::sharding_should_receive_txn_num.GetCount(epoch_mod),  EpochMessageReceiveHandler::sharding_received_txn_num.GetCount(epoch_mod),

       EpochMessageReceiveHandler::backup_should_receive_pack_num.GetCount(epoch_mod),   EpochMessageReceiveHandler::backup_received_pack_num.GetCount(epoch_mod),
       EpochMessageReceiveHandler::backup_should_receive_txn_num.GetCount(epoch_mod),    EpochMessageReceiveHandler::backup_received_txn_num.GetCount(epoch_mod),

       EpochMessageReceiveHandler::insert_set_should_receive_num.GetCount(epoch_mod),          EpochMessageReceiveHandler::insert_set_received_num.GetCount(epoch_mod),
       EpochMessageReceiveHandler::abort_set_should_receive_num.GetCount(epoch_mod),  EpochMessageReceiveHandler::abort_set_received_num.GetCount(epoch_mod),

       EpochMessageReceiveHandler::sharding_received_ack_num.GetCount(epoch_mod),        EpochMessageReceiveHandler::backup_received_ack_num.GetCount(epoch_mod),
       EpochMessageReceiveHandler::insert_set_received_ack_num.GetCount(epoch_mod),      EpochMessageReceiveHandler::abort_set_received_ack_num.GetCount(epoch_mod),

       (uint64_t)0,
        now_to_us()) << PrintfToString("\n Epoch: %lu ClearEpoch: %lu, SuccessTxnNumber %lu, ToTalSuccessLatency %lu, SuccessAvgLatency %lf, TotalCommitTxnNum %lu, TotalCommitlatency %lu, TotalCommitAvglatency %lf \n",
                                       epoch_, clear_epoch.load(),
                                       EpochMessageSendHandler::TotalSuccessTxnNUm.load(), EpochMessageSendHandler::TotalSuccessLatency.load(),
                                       (((double)EpochMessageSendHandler::TotalSuccessLatency.load()) / ((double)EpochMessageSendHandler::TotalSuccessTxnNUm.load())),
                                       EpochMessageSendHandler::TotalTxnNum.load(),///receive from client
                                       EpochMessageSendHandler::TotalLatency.load(),
                                       (((double)EpochMessageSendHandler::TotalLatency.load()) / ((double)EpochMessageSendHandler::TotalTxnNum.load())))
          << PrintfToString("EpochMerge MergeTxnNumber %lu, ToTalMergeLatency %lu, FailedReadCheckTxnNum %lu, MergeAvgLatency %lf \n",
                            Merger::total_merge_txn_num.load(), Merger::total_merge_latency.load(),
                            Merger::total_read_version_check_failed_txn_num.load(),
                            (((double)Merger::total_merge_latency.load()) / ((double)Merger::total_merge_txn_num.load())))
          << PrintfToString("EpochCommit CommitTxnNumber %lu, ToTalMCommitLatency %lu, FailedTxnNUm %lu, SuccessTxnNum %lu, TotalCommitAvgLatency %lf SuccessCommitLatency %lf\n",
                            Merger::total_commit_txn_num.load(), Merger::total_commit_latency.load(),
                            Merger::total_failed_txn_num.load(), Merger::success_commit_txn_num.load(),
                            (((double)Merger::total_commit_latency.load()) / ((double)Merger::total_commit_txn_num.load())),
                            (((double)Merger::success_commit_txn_num.load()) / ((double)Merger::success_commit_latency.load())))
          << PrintfToString("Storage Push Down TiKVTotalTxnNumber %lu ,TiKVSuccessTxnNum %lu, TiKVFailedTxnNum %lu \n",
                            TiKV::total_commit_txn_num.load(), TiKV::total_commit_txn_num.load() - TiKV::failed_commit_txn_num.load(), TiKV::failed_commit_txn_num.load())
          << "**************************************************************************************************************************************************************************************\n";
    }

    bool CheckRedoLogPushDownState(const Context& ctx) {
        auto i = redo_log_epoch.load();
        auto clear = i;
        shared_ptr<proto::Transaction> empty_txn_ptr;
        while(!EpochManager::IsTimerStop()) {
            while(i >= commit_epoch.load()) usleep(logical_sleep_timme);
            while(!EpochManager::IsCommitComplete(i)) usleep(logical_sleep_timme);
            while(!RedoLoger::CheckPushDownComplete(ctx, i)) usleep(logical_sleep_timme);
            EpochMessageSendHandler::SendTxnToServer(ctx, i,
                                                     i, empty_txn_ptr, proto::TxnType::EpochLogPushDownComplete);
            while(!EpochMessageReceiveHandler::IsRedoLogPushDownACKReceiveComplete(ctx, i)) usleep(logical_sleep_timme);

            {
                if(i % ctx.print_mode_size == 0)
                    LOG(INFO) << PrintfToString("=-=-=-=-=-=-= 完成一个Epoch的 Log Push Down Epoch: %8lu ClearEpoch: %8lu =-=-=-=-=-=-=\n", commit_epoch.load(), i);

                EpochManager::ClearMergeEpochState(i); //清空当前epoch的merge信息
                EpochMessageReceiveHandler::StaticClear(ctx, i);//清空current epoch的receive cache num信息
                Merger::ClearMergerEpochState(ctx, i);
                RedoLoger::ClearRedoLog(ctx, i);
                redo_log_epoch.fetch_add(1);
                clear_epoch.fetch_add(1);
                EpochManager::AddPushDownEpoch();
                i ++;
            }
        }
        return true;
    }

    void EpochLogicalTimerManagerThreadMain(const Context& ctx) {
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        if(ctx.is_cache_server_available) {
            cache_server_available = 0;
        }
        uint64_t epoch = 1;
        OUTPUTLOG(ctx, "===== Start Epoch的合并 ===== ", epoch);
        while(!EpochManager::IsTimerStop()){

//            while(EpochManager::GetPhysicalEpoch() <= EpochManager::GetLogicalEpoch() + ctx.kDelayEpochNum) {
//                usleep(logical_sleep_timme);
//                ShardingEpochManager::CheckEpochMergeState(ctx);
//            }
//
//            while(!ShardingEpochManager::CheckEpochAbortMergeState(ctx)) {
////                OUTPUTLOG(ctx, "=====CheckEpochAbortMergeState===== ", epoch);
//                usleep(logical_sleep_timme);
//                ShardingEpochManager::CheckEpochMergeState(ctx);
//            }
//
//            while(!ShardingEpochManager::CheckEpochCommitState(ctx)) {
////                OUTPUTLOG(ctx, "=====CheckEpochCommitState===== ", epoch);
//                usleep(logical_sleep_timme);
//                ShardingEpochManager::CheckEpochMergeState(ctx);
//                ShardingEpochManager::CheckEpochAbortMergeState(ctx);
//            }
//            EpochManager::CheckRedoLogPushDownState();
//            //clear cache  move to mot.cpp  MOT::SendToMOThreadMain_usleep();
        }
        printf("total commit txn num: %lu\n", total_commit_txn_num);
    }

    void EpochPhysicalTimerManagerThreadMain(Context ctx) {
        InitEpochTimerManager(ctx);
        //==========同步============
        zmq::message_t message;
        zmq::context_t context(1);
        zmq::socket_t request_puller(context, ZMQ_PULL);
        request_puller.bind("tcp://*:5546");
    //    request_puller.recv(&message);
        gettimeofday(&start_time, nullptr);
        start_time_ll = start_time.tv_sec * 1000000 + start_time.tv_usec;
        if(ctx.is_sync_start && ctx.taas_mode != TaasMode::TwoPC) {
            auto sleep_time_temp = static_cast<uint64_t>((((start_time.tv_sec / 60) + 1) * 60) * 1000000);
            usleep(sleep_time_temp - start_time_ll);
            gettimeofday(&start_time, nullptr);
            start_time_ll = start_time.tv_sec * 1000000 + start_time.tv_usec;
        }
        EpochManager::SetPhysicalEpoch(1);
        EpochManager::SetLogicalEpoch(1);
        auto epoch_ = EpochManager::GetPhysicalEpoch();
        auto logical = EpochManager::GetLogicalEpoch();
        test_start.store(true);
        is_epoch_advance_started.store(true);

        printf("=============  EpochTimerManager 同步完成，数据库开始正常运行 ============= \n");
        auto startTime = now_to_us();
        while(!EpochManager::IsTimerStop()){
            usleep(GetSleeptime(ctx));
            EpochManager::AddPhysicalEpoch();
            epoch_ ++;
            logical = EpochManager::GetLogicalEpoch();
            if(epoch_ % ctx.print_mode_size == 0) {
                LOG(INFO) << "============= Start Physical Epoch : " << epoch_ << ", logical : " << logical << "Time : " << now_to_us() - startTime << "=============\n";
                OUTPUTLOG(ctx, "============= Epoch INFO ============= ", logical);
            }
            EpochManager::EpochCacheSafeCheck();
        }
        OUTPUTLOG(ctx, "============= Epoch INFO ============= ", logical);
        LOG(INFO) << "Start Physical epoch : " << epoch_ << ", logical : " << logical << "Time : " << now_to_us() - startTime;
        printf("EpochTimerManager End!!!\n");
    }


    void EpochManager::SetServerOnLine(uint64_t& epoch_, const std::string& ip) {
        for(int i = 0; i < (int)ctx.kServerIp.size(); i++) {
            if(ip == ctx.kServerIp[i]) {
                server_state.SetCount(epoch_, i, 1);
                    EpochMessageReceiveHandler::sharding_should_receive_pack_num.Clear(epoch_, 1);///relate to server state
                    EpochMessageReceiveHandler::backup_should_receive_pack_num.Clear(epoch_, 1);///relate to server state
                    EpochMessageReceiveHandler::insert_set_should_receive_num.Clear(epoch_, 1);///relate to server state
                    EpochMessageReceiveHandler::abort_set_should_receive_num.Clear(epoch_, 1);///relate to server state
            }
        }
    }

    void EpochManager::SetServerOffLine(uint64_t& epoch_, const std::string& ip) {
        for(int i = 0; i < (int)ctx.kServerIp.size(); i++) {
            if(ip == ctx.kServerIp[i]) {
                server_state.SetCount(epoch_, i, 0);
                    EpochMessageReceiveHandler::sharding_should_receive_pack_num.Clear(epoch_, 0);///relate to server state
                    EpochMessageReceiveHandler::backup_should_receive_pack_num.Clear(epoch_, 0);///relate to server state
                    EpochMessageReceiveHandler::insert_set_should_receive_num.Clear(epoch_, 0);///relate to server state
                    EpochMessageReceiveHandler::abort_set_should_receive_num.Clear(epoch_, 0);///relate to server state
            }
        }
    }
}