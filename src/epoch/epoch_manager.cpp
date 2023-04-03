//
// Created by 周慰星 on 11/8/22.
//

#include "sys/time.h"
#include "string"
#include "tools/utilities.h"
#include "epoch/epoch_manager.h"
#include "message/message.h"
#include "message/handler_receive.h"
#include "storage/redo_loger.h"
#include "transaction/merge.h"

namespace Taas {

    using namespace std;

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

    void InitEpochTimerManager(Context& ctx){
        Merger::StaticInit(ctx);
        MessageQueue::StaticInitMessageQueue(ctx);
        MessageSendHandler::StaticInit(ctx);
        MessageReceiveHandler::StaticInit(ctx);

        EpochManager::max_length = ctx.kCacheMaxLength;
        //==========Logical Epoch Merge State=============
        EpochManager::merge_complete.resize(EpochManager::max_length);
        EpochManager::abort_set_merge_complete.resize(EpochManager::max_length);
        EpochManager::commit_complete.resize(EpochManager::max_length);
        EpochManager::record_committed.resize(EpochManager::max_length);
        EpochManager::is_current_epoch_abort.resize(EpochManager::max_length);
        //cluster state
        EpochManager::online_server_num.reserve(EpochManager::max_length + 1);
//        EpochManager::should_receive_pack_num.reserve(EpochManager::max_length + 1);
        EpochManager::server_state.Init(EpochManager::max_length,(int)ctx.kServerIp.size() + 2, 1);
        //cache server
        EpochManager::cache_server_received_epoch.reserve(EpochManager::max_length + 1);
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
            EpochManager::online_server_num[i] = std::make_unique<std::atomic<uint64_t>>(ctx.kTxnNodeNum);
            //cache server
            EpochManager::cache_server_received_epoch.emplace_back(std::make_unique<std::atomic<uint64_t>>(val));

        }

        EpochManager::AddPhysicalEpoch();
        init_ok_num.fetch_add(1);
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
        printf("%60s \n\
        physical                     %6lu, logical                      %6lu,   \
        epoch_mod                    %6lu, disstance %6lu, \n\
        ShardingPackReceiveOK?       %6lu, ShardingTxnReceiveOK?        %6lu    \
        ShardingSendOK?              %6lu, ShardingACKReceiveOK?        %6lu    \
        backupSendOK?                %6lu, backupACKReceiveOK?          %6lu,   \
        IsShardingMergeComplete      %6lu, IsAbortSetMergeComplete      %6lu    \
        IsCommitComplete             %6lu, SetRecordCommitted           %6lu  \n\
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
               epoch_mod,                                                                         EpochManager::GetPhysicalEpoch() - EpochManager::GetLogicalEpoch(),

               (uint64_t)EpochManager::IsShardingMergeComplete(epoch_mod),                  (uint64_t)EpochManager::IsAbortSetMergeComplete(epoch_mod),
               (uint64_t)EpochManager::IsCommitComplete(epoch_mod),                         (uint64_t)EpochManager::IsRecordCommitted(epoch_mod),
               MessageReceiveHandler::IsRemoteShardingPackReceiveComplete(epoch_mod),               MessageReceiveHandler::IsRemoteShardingTxnReceiveComplete(epoch_mod),
               MessageReceiveHandler::IsShardingSendFinish(epoch_mod),                              MessageReceiveHandler::IsShardingACKReceiveComplete(epoch_mod),
               MessageReceiveHandler::IsBackUpSendFinish(epoch_mod),                                MessageReceiveHandler::IsBackUpACKReceiveComplete(epoch_mod),
               Merger::epoch_merged_txn_num.GetCount(epoch_mod),                            Merger::epoch_should_merge_txn_num.GetCount(epoch_mod),
               Merger::epoch_committed_txn_num.GetCount(epoch_mod),                         Merger::epoch_should_commit_txn_num.GetCount(epoch_mod),
               Merger::epoch_record_committed_txn_num.GetCount(epoch_mod),                  Merger::epoch_record_commit_txn_num.GetCount(epoch_mod),

               MessageReceiveHandler::sharding_should_receive_pack_num.GetCount(epoch_mod), MessageReceiveHandler::sharding_received_pack_num.GetCount(epoch_mod),
               MessageReceiveHandler::sharding_should_receive_txn_num.GetCount(epoch_mod),  MessageReceiveHandler::sharding_received_txn_num.GetCount(epoch_mod),

               MessageReceiveHandler::backup_should_receive_pack_num.GetCount(epoch_mod),   MessageReceiveHandler::backup_received_pack_num.GetCount(epoch_mod),
               MessageReceiveHandler::backup_should_receive_txn_num.GetCount(epoch_mod),    MessageReceiveHandler::backup_received_txn_num.GetCount(epoch_mod),

               MessageReceiveHandler::insert_set_should_receive_num.GetCount(epoch_mod),          MessageReceiveHandler::insert_set_received_num.GetCount(epoch_mod),
               MessageReceiveHandler::sharding_should_receive_abort_set_num.GetCount(epoch_mod),  MessageReceiveHandler::sharding_received_abort_set_num.GetCount(epoch_mod),

               MessageReceiveHandler::sharding_received_ack_num.GetCount(epoch_mod),        MessageReceiveHandler::backup_received_ack_num.GetCount(epoch_mod),
               MessageReceiveHandler::insert_set_received_ack_num.GetCount(epoch_mod),      MessageReceiveHandler::sharding_abort_set_received_ack_num.GetCount(epoch_mod),

               (uint64_t)0,
               now_to_us());
//        fflush(stdout);
    }

    bool CheckEpochMergeState(uint64_t &epoch, Context& ctx) {
        auto epoch_max = EpochManager::GetPhysicalEpoch();
        if(epoch >= epoch_max) return false;
        for(auto i = epoch; i < epoch_max; i ++) {
            if(EpochManager::IsShardingMergeComplete(i)) continue;
            if( (ctx.kTxnNodeNum == 1 ||
                    (MessageReceiveHandler::IsRemoteShardingPackReceiveComplete(i, ctx) &&
                    MessageReceiveHandler::IsRemoteShardingTxnReceiveComplete(i, ctx) &&
                    MessageReceiveHandler::IsShardingACKReceiveComplete(i, ctx) &&
                    MessageReceiveHandler::IsBackUpSendFinish(i, ctx) &&
                    MessageReceiveHandler::IsBackUpACKReceiveComplete(i, ctx) )
                ) &&
                MessageReceiveHandler::IsEpochTxnEnqueued_MergeQueue(i, ctx) &&
                Merger::IsEpochMergeComplete(i, ctx)
            ) {
                EpochManager::SetShardingMergeComplete(i, true);
            }
        }
        while(EpochManager::IsShardingMergeComplete(epoch) && epoch < epoch_max) epoch++;
        return true;
    }

    bool CheckEpochAbortSetState(uint64_t &epoch, Context& ctx) {
        auto epoch_max = EpochManager::GetPhysicalEpoch();
        if(epoch >= epoch_max) return false;
        for(auto i = epoch; i < epoch_max; i ++) {
            if(EpochManager::IsAbortSetMergeComplete(i)) continue;
            if(     (ctx.kTxnNodeNum == 1 ||
                    (MessageReceiveHandler::IsAbortSetACKReceiveComplete(i, ctx) &&
                        MessageReceiveHandler::IsRemoteAbortSetReceiveComplete(i, ctx) )) &&
                EpochManager::IsShardingMergeComplete(i)
               ) {
                EpochManager::SetAbortSetMergeComplete(i, true);
            }
        }
        while(EpochManager::IsAbortSetMergeComplete(epoch) && epoch < epoch_max) epoch++;
        return true;
    }

    bool CheckEpochCommitState(uint64_t &epoch, Context& ctx) {
        auto epoch_max = EpochManager::GetPhysicalEpoch();
        if(epoch >= epoch_max) return false;
        for(auto i = epoch; i < epoch_max; i ++) {
            if(EpochManager::IsCommitComplete(i)) continue;
            if( EpochManager::IsShardingMergeComplete(epoch) &&
                EpochManager::IsAbortSetMergeComplete(i) &&
                MessageReceiveHandler::IsEpochTxnEnqueued_LocalTxnQueue(i, ctx) &&
                    Merger::IsEpochCommitComplete(epoch, ctx)
                ) {
                EpochManager::SetCommitComplete(i, true);
            }
        }
        while(EpochManager::IsCommitComplete(epoch) && epoch < epoch_max) epoch++;
        return true;
    }

    bool CheckAndSetRedoLogPushDownState(uint64_t& epoch, Context& ctx) {
        if(epoch >= EpochManager::GetPhysicalEpoch()) return false;
        if(EpochManager::IsCommitComplete(epoch) &&
            MessageReceiveHandler::IsRedoLogPushDownACKReceiveComplete(epoch, ctx)) {
            EpochManager::SetRecordCommitted(epoch, true);
            epoch ++;
            return true;
        }
        return false;
    }

    void EpochLogicalTimerManagerThreadMain(Context ctx) {
        SetCPU();
        uint64_t epoch = 1, cache_server_available = 1, total_commit_txn_num = 0,
                merge_epoch = 1, abort_set_epoch = 1, commit_epoch = 1, redo_log_epoch = 1, clear_epoch = 1;
        bool sleep_flag;
        while(!EpochManager::IsInitOK()) usleep(1000);
        if(ctx.is_cache_server_available) {
            cache_server_available = 0;
        }
        OUTPUTLOG("=====start Epoch的合并===== ", epoch);
        while(!EpochManager::IsTimerStop()){
            while(EpochManager::GetPhysicalEpoch() <= EpochManager::GetLogicalEpoch() + ctx.kDelayEpochNum) usleep(20);
            sleep_flag = false;
            sleep_flag = sleep_flag | CheckEpochMergeState(merge_epoch, ctx);
            sleep_flag = sleep_flag | CheckEpochAbortSetState(abort_set_epoch, ctx);
            sleep_flag = sleep_flag | CheckEpochCommitState(commit_epoch, ctx);
            sleep_flag = sleep_flag | CheckAndSetRedoLogPushDownState(redo_log_epoch, ctx);

            while(epoch < commit_epoch) {
                total_commit_txn_num += Merger::epoch_record_committed_txn_num.GetCount(epoch);
                if(epoch % ctx.print_mode_size == 0) {
                    printf("*************       完成一个Epoch的合并     Epoch: %8lu *************\n", epoch);
                }
                epoch ++;
                EpochManager::AddLogicalEpoch();
            }

            while(clear_epoch < redo_log_epoch) {
                if(clear_epoch % ctx.print_mode_size == 0) {
                    printf("=-=-=-=-=-=-=完成一个Epoch的 Log Push Down Epoch: %8lu =-=-=-=-=-=-=\n", clear_epoch);
                }
                EpochManager::ClearMergeEpochState(clear_epoch); //清空当前epoch的merge信息
                EpochManager::SetCacheServerStored(clear_epoch, cache_server_available);

                MessageReceiveHandler::StaticClear(clear_epoch, ctx);//清空current epoch的receive cache num信息
                Merger::ClearMergerEpochState(clear_epoch, ctx);

                RedoLoger::ClearRedoLog(clear_epoch, ctx);

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
    void EpochPhysicalTimerManagerThreadMain(Context ctx) {
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
        auto epoch = EpochManager::GetPhysicalEpoch(), logical = EpochManager::GetLogicalEpoch();
        test_start.store(true);
        is_epoch_advance_started.store(true);

        printf("EpochTimerManager 同步完成，数据库开始正常运行\n");
        while(!EpochManager::IsTimerStop()){
            usleep(GetSleeptime(ctx));
            EpochManager::AddPhysicalEpoch();
            epoch ++;
            if(epoch % ctx.print_mode_size == 0) {
                logical = EpochManager::GetLogicalEpoch();
                OUTPUTLOG("=============start Epoch============= ", logical);
            }
            EpochManager::EpochCacheSafeCheck();
        }
        printf("EpochTimerManager End!!!\n");
    }


    void EpochManager::SetServerOnLine(uint64_t& epoch, const std::string& ip) {
        for(int i = 0; i < (int)ctx.kServerIp.size(); i++) {
            if(ip == ctx.kServerIp[i]) {
                server_state.SetCount(epoch, i, 1);
                    MessageReceiveHandler::sharding_should_receive_pack_num.Clear(epoch, 1);///relate to server state
                    MessageReceiveHandler::backup_should_receive_pack_num.Clear(epoch, 1);///relate to server state
                    MessageReceiveHandler::insert_set_should_receive_num.Clear(epoch, 1);///relate to server state
                    MessageReceiveHandler::sharding_should_receive_abort_set_num.Clear(epoch, 1);///relate to server state
            }
        }
    }

    void EpochManager::SetServerOffLine(uint64_t& epoch, const std::string& ip) {
        for(int i = 0; i < (int)ctx.kServerIp.size(); i++) {
            if(ip == ctx.kServerIp[i]) {
                server_state.SetCount(epoch, i, 0);
                    MessageReceiveHandler::sharding_should_receive_pack_num.Clear(epoch, 0);///relate to server state
                    MessageReceiveHandler::backup_should_receive_pack_num.Clear(epoch, 0);///relate to server state
                    MessageReceiveHandler::insert_set_should_receive_num.Clear(epoch, 0);///relate to server state
                    MessageReceiveHandler::sharding_should_receive_abort_set_num.Clear(epoch, 0);///relate to server state
            }
        }
    }
}