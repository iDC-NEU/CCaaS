//
// Created by 周慰星 on 11/8/22.
//

#ifndef TAAS_EPOCH_MANANGER_H
#define TAAS_EPOCH_MANANGER_H
#include <memory>
#include <utility>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <unistd.h>
#include "tools/atomic_counters.h"
#include "tools/concurrent_hash_map.h"
#include "tools/context.h"
#include "tools/blocking_concurrent_queue.hpp"
#include "proto/message.pb.h"
#include "zmq.hpp"
#include "tikv_client.h"

namespace Taas {
    template<typename T>
    using  BlockingConcurrentQueue = moodycamel::BlockingConcurrentQueue<T>;

    struct pack_params {
        uint64_t id{};/// send to whom
        uint64_t time{};
        std::string ip; /// send to whom
        uint64_t epoch{};
        proto::TxnType type{};
        std::unique_ptr<std::string> str;
        std::unique_ptr<proto::Transaction> txn;
        explicit pack_params(uint64_t id_, uint64_t time_, std::string ip_, uint64_t e = 0, proto::TxnType ty = proto::TxnType::NullMark,
                             std::unique_ptr<std::string> && s = nullptr, std::unique_ptr<proto::Transaction> &&t = nullptr):
                             id(id_), time(time_), ip(std::move(ip_)), epoch(e), type(ty), str(std::move(s)), txn(std::move(t)){}
        pack_params()= default;
    };

    struct send_params {
        uint64_t id{}; /// send to whom
        uint64_t time{};
        std::string ip; /// send to whom
        uint64_t epoch{};
        proto::TxnType type{};
        std::unique_ptr<std::string> str;
        std::unique_ptr<proto::Transaction> txn;
        send_params(uint64_t id_, uint64_t time_, std::string ip_, uint64_t e = 0, proto::TxnType ty = proto::TxnType::NullMark,
                std::unique_ptr<std::string> && s = nullptr, std::unique_ptr<proto::Transaction> &&t = nullptr):
        id(id_), time(time_), ip(std::move(ip_)), epoch(e), type(ty), str(std::move(s)), txn(std::move(t)){}
        send_params()= default;
    };

    // 原子变量
    static std::atomic<uint64_t> merge_num;
    ///message transmit
    extern std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>>> listen_message_queue;
    extern std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<send_params>>> send_to_server_queue, send_to_client_queue, send_to_storage_queue;
    extern std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<proto::Message>>> request_queue, raft_message_queue;
    ///merge_queue 存放需要进行merge的子事务 不区分epoch
    extern std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>> merge_queue;
    ///epoch_local_txn_queue 本地接收到的完整的事务
    ///epoch_commit_queue 当前epoch的涉及当前分片的要进行validate和commit的子事务
    extern std::vector<std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>> epoch_local_txn_queue, ///stroe whole txn receive from client (used to push log down to storage)
            epoch_commit_queue,///store sharding transactions receive from servers, wait to validate
            epoch_redo_log_queue; ///store transactions receive from clients, wait to push down

    extern std::atomic<bool> init_ok, is_epoch_advance_started, test_start;

    extern void InitEpochTimerManager(Context& ctx);

    //epoch work threads
    extern void EpochLogicalTimerManagerThreadMain(Context ctx);
    extern void EpochPhysicalTimerManagerThreadMain(Context ctx);
    extern void StateChecker(Context ctx);
    extern void WorkerThreadMain(uint64_t id, Context ctx);

    //message transport threads
    extern void SendServerThreadMain(Context ctx);
    extern void ListenServerThreadMain(Context ctx);
    extern void SendClientThreadMain(Context ctx);
    extern void ListenClientThreadMain(Context ctx);
    extern void ListenStorageThreadMain(Context ctx);
    extern void SendStoragePUBThreadMain(Context ctx);
    extern void SendStoragePUBThreadMain2(Context ctx);




    class EpochManager {
    private:
        static bool timerStop;
        static std::atomic<uint64_t> logical_epoch, physical_epoch, push_down_epoch;
    public:

        static Context ctx;
        static uint64_t max_length;
        static uint64_t pack_num;
        static std::vector<std::unique_ptr<std::atomic<bool>>>
                    merge_complete, abort_set_merge_complete,
                    commit_complete, record_committed,
                    is_current_epoch_abort;
        static AtomicCounters_Cache ///epoch, index, value
        ///epoch事务合并状态
        should_merge_txn_num, merged_txn_num,
        ///epoch事务提交状态
        should_commit_txn_num, committed_txn_num,
        ///epoch日志写入状态
        record_commit_txn_num, record_committed_txn_num;
        static AtomicCounters ///epoch, value        for epoch log (each epoch has single one counter)
        epoch_log_lsn;

        static AtomicCounters_Cache ///epoch, index, value  for 集群状态
        server_state; /// epoch, csn(atomic increase)

        static  std::vector<std::unique_ptr<std::atomic<uint64_t>>> should_receive_pack_num, online_server_num;

        static std::vector<std::unique_ptr<concurrent_crdt_unordered_map<std::string, std::string, std::string>>> epoch_merge_map,
                local_epoch_abort_txn_set, epoch_abort_txn_set;
        static std::vector<std::unique_ptr<concurrent_unordered_map<std::string, std::string>>> epoch_insert_set;

        static concurrent_unordered_map<std::string, std::string> read_version_map, insert_set, abort_txn_set;

        static std::vector<std::unique_ptr<std::vector<proto::Transaction>>> redo_log; // [epoch_no]<no, serialize(PB::txn)>
        static std::vector<std::unique_ptr<concurrent_unordered_map<std::string, proto::Transaction>>> committed_txn_cache;

        ///fault tolerance cache server mod
        static std::vector<std::unique_ptr<std::atomic<uint64_t>>>  received_epoch;

        static tikv_client::TransactionClient* tikv_client_ptr;

        static void SetTimerStop(bool value) {timerStop = value;}
        static bool IsTimerStop() {return timerStop;}

        static bool IsShardingMergeComplete(uint64_t epoch) {return merge_complete[epoch % max_length]->load();}
        static void SetShardingMergeComplete(uint64_t epoch, bool value) {merge_complete[epoch % max_length]->store(value);}

        static bool IsAbortSetMergeComplete(uint64_t epoch) {return abort_set_merge_complete[epoch % max_length]->load();}
        static void SetAbortSetMergeComplete(uint64_t epoch, bool value) {abort_set_merge_complete[epoch % max_length]->store(value);}

        static bool IsCommitComplete(uint64_t epoch) {return commit_complete[epoch % max_length]->load();}
        static void SetCommitComplete(uint64_t epoch, bool value) {commit_complete[epoch % max_length]->store(value);}

        static bool IsRecordCommitted(uint64_t epoch){ return record_committed[epoch % max_length]->load();}
        static void SetRecordCommitted(uint64_t epoch, bool value){ record_committed[epoch % max_length]->store(value);}

        static bool IsCurrentEpochAbort(uint64_t epoch){ return is_current_epoch_abort[epoch % max_length]->load();}
        static void SetCurrentEpochAbort(uint64_t epoch, bool value){ is_current_epoch_abort[epoch % max_length]->store(value);}

        static void SetPhysicalEpoch(uint64_t value){ physical_epoch.store(value);}
        static uint64_t AddPhysicalEpoch(){
            return physical_epoch.fetch_add(1);
        }
        static uint64_t GetPhysicalEpoch(){ return physical_epoch.load();}

        static void SetLogicalEpoch(uint64_t value){ logical_epoch.store(value);}
        static uint64_t AddLogicalEpoch(){
            return logical_epoch.fetch_add(1);
        }
        static uint64_t GetLogicalEpoch(){ return logical_epoch.load();}

        static void SetPushDownEpoch(uint64_t value){ push_down_epoch.store(value);}
        static uint64_t AddPushDownEpoch(){
            return push_down_epoch.fetch_add(1);
        }
        static uint64_t GetPushDownEpoch(){ return push_down_epoch.load();}


        static void ClearMergeEpochState(uint64_t epoch_mod) {
            epoch_mod %= max_length;

            merge_complete[epoch_mod]->store(false);
            abort_set_merge_complete[epoch_mod]->store(false);
            commit_complete[epoch_mod]->store(false);
            record_committed[epoch_mod]->store(false);
            is_current_epoch_abort[epoch_mod]->store(false);

            epoch_merge_map[epoch_mod]->clear();
            epoch_insert_set[epoch_mod]->clear();
            epoch_abort_txn_set[epoch_mod]->clear();
            local_epoch_abort_txn_set[epoch_mod]->clear();

            should_merge_txn_num.Clear(epoch_mod), merged_txn_num.Clear(epoch_mod),
            should_commit_txn_num.Clear(epoch_mod), committed_txn_num.Clear(epoch_mod);
            record_commit_txn_num.Clear(epoch_mod), record_committed_txn_num.Clear(epoch_mod);
        }

        static void ClearLog(uint64_t epoch_mod) {
            epoch_mod %= max_length;
            redo_log[epoch_mod]->clear();
            committed_txn_cache[epoch_mod]->clear();
            epoch_log_lsn.SetCount(epoch_mod, 0);
        }

        static void EpochCacheSafeCheck() {
            if((GetLogicalEpoch() % ctx.kCacheMaxLength) ==  ((GetPhysicalEpoch() + 55) % ctx.kCacheMaxLength) ) {
                printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
                printf("+++++++++++++++Fata : Cache Size exceeded!!! +++++++++++++++++++++\n");
                printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
                assert(false);
            }
        }

        static uint64_t AddShouldReceivePackNum(uint64_t epoch, uint64_t value) {
            return should_receive_pack_num[epoch % max_length]->fetch_add(value);
        }
        static uint64_t SubShouldReceivePackNum(uint64_t epoch, uint64_t value) {
            return should_receive_pack_num[epoch % max_length]->fetch_sub(value);
        }
        static void StoreShouldReceivePackNum(uint64_t epoch, uint64_t value) {
            should_receive_pack_num[epoch % max_length]->store(value);
        }
        static uint64_t GetShouldReceivePackNum(uint64_t epoch) {
            return should_receive_pack_num[epoch % max_length]->load();
        }

        static uint64_t AddOnLineServerNum(uint64_t epoch, uint64_t value) {
            return online_server_num[epoch % max_length]->fetch_add(value);
        }
        static uint64_t SubOnLineServerNum(uint64_t epoch, uint64_t value) {
            return online_server_num[epoch % max_length]->fetch_sub(value);
        }
        static void StoreOnLineServerNum(uint64_t epoch, uint64_t value) {
            online_server_num[epoch % max_length]->store(value);
        }
        static uint64_t GetOnLineServerNum(uint64_t epoch) {
            return online_server_num[epoch % max_length]->load();
        }

        static void SetServerOnLine(const std::string& ip) {
            for(int i = 0; i < (int)ctx.kServerIp.size(); i++) {
                if(ip == ctx.kServerIp[i]) {
                    server_state.SetCount(i, 1);
                }
            }
        }

        static void SetServerOffLine(const std::string& ip) {
            for(int i = 0; i < (int)ctx.kServerIp.size(); i++) {
                if(ip == ctx.kServerIp[i]) {
                    server_state.SetCount(i, 0);
                }
            }
        }

        static void SetCacheServerStored(uint64_t epoch, uint64_t value) {
            received_epoch[epoch % max_length]->store(value);
        }

    };
}


#endif //TAAS_EPOCH_MANANGER_H
