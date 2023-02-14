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

namespace Taas {
    template<typename T>
    using  BlockingConcurrentQueue = moodycamel::BlockingConcurrentQueue<T>;

    struct pack_params {
        uint64_t id{};/// send to whom
        uint64_t time{};
        std::string ip; /// send to whom
        uint64_t epoch{};
        proto::TxnType type;
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
        proto::TxnType type;
        std::unique_ptr<std::string> str;
        std::unique_ptr<proto::Transaction> txn;
        send_params(uint64_t id_, uint64_t time_, std::string ip_, uint64_t e = 0, proto::TxnType ty = proto::TxnType::NullMark,
                std::unique_ptr<std::string> && s = nullptr, std::unique_ptr<proto::Transaction> &&t = nullptr):
        id(id_), time(time_), ip(std::move(ip_)), epoch(e), type(ty), str(std::move(s)), txn(std::move(t)){}
        send_params()= default;
    };

    // 原子变量
    static std::atomic<uint64_t> merge_num;

    extern BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>> listen_message_queue;
    extern BlockingConcurrentQueue<std::unique_ptr<send_params>> send_to_server_queue, send_to_client_queue, send_to_storage_queue;
    ///local_txn_queue 本地接收到的完整的事务
    ///merge_queue 当前epoch的涉及当前分片的子事务
    ///commit_queue 当前epoch的能提交的涉及当前分片的子事务
    extern BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>> local_txn_queue, merge_queue, commit_queue;
    extern BlockingConcurrentQueue<std::unique_ptr<pack_params>> pack_txn_queue;
    extern BlockingConcurrentQueue<std::unique_ptr<proto::Message>> request_queue, raft_message_queue;
    extern std::atomic<bool> init_ok, is_epoch_advance_started, test_start;

    extern uint64_t now_to_us();
    extern void InitEpochTimerManager(Context& ctx);
    extern void EpochLogicalTimerManagerThreadMain(uint64_t id, Context ctx);
    extern void EpochPhysicalTimerManagerThreadMain(uint64_t id, Context ctx);
    extern void MergeWorkerThreadMain(uint64_t id, Context ctx);

    extern void PackThreadMain(uint64_t id, Context ctx);

    extern void MessageCacheThreadMain(uint64_t id, Context ctx);

    extern void SendServerThreadMain(uint64_t id, Context ctx);
    extern void ListenServerThreadMain(uint64_t id, Context ctx);

    extern void SendClientThreadMain(uint64_t id, Context ctx);
    extern void ListenClientThreadMain(uint64_t id, const Context& ctx);

    extern void ListenStorageThreadMain(uint64_t id, Context ctx);

    extern void SendStoragePUBThreadMain(uint64_t id, Context ctx);
    extern void SendStoragePUBThreadMain2(uint64_t id, Context ctx);




    class EpochManager {
    private:
        static bool timerStop;
        static volatile bool merge_complete, abort_set_merge_complete, commit_complete, record_committed, is_current_epoch_abort;
        static volatile uint64_t logical_epoch, physical_epoch;
    public:
        static Context ctx;
        // cache_max_length
        static uint64_t max_length;
        // for concurrency , atomic_counters' num
        static uint64_t pack_num;
        static AtomicCounters_Cache ///epoch, index, value
        ///epoch事务合并状态
        should_merge_txn_num, merged_txn_num,
        ///epoch事务提交状态
        should_commit_txn_num, committed_txn_num,
        ///epoch日志写入状态
        record_commit_txn_num, record_committed_txn_num
        ///debug用
        ;

        static AtomicCounters ///index, value
        /// 集群状态
        server_state, epoch_log_lsn; /// epoch, csn(atomic increase)

        static std::unique_ptr<std::atomic<uint64_t>> should_receive_pack_num, online_server_num;

        static std::vector<std::unique_ptr<concurrent_crdt_unordered_map<std::string, std::string, std::string>>> epoch_merge_map,
                local_epoch_abort_txn_set, epoch_abort_txn_set;
        static std::vector<std::unique_ptr<concurrent_unordered_map<std::string, std::string>>> epoch_insert_set;

        static concurrent_unordered_map<std::string, std::string> read_version_map, insert_set, abort_txn_set;

        static std::vector<std::unique_ptr<std::vector<proto::Transaction>>> redo_log; // [epoch_no]<no, serialize(PB::txn)>
        static std::vector<std::unique_ptr<concurrent_unordered_map<std::string, proto::Transaction>>> committed_txn_cache;

        ///cache server
        static std::vector<std::unique_ptr<std::atomic<uint64_t>>>  received_epoch;

        static void SetTimerStop(bool value) {timerStop = value;}
        static bool IsTimerStop() {return timerStop;}

        static bool IsMergeComplete() {return merge_complete;}
        static void SetMergeComplete(bool value) {merge_complete = value;}

        static bool IsAbortSetMergeComplete() {return abort_set_merge_complete;}
        static void SetAbortSetMergeComplete(bool value) {abort_set_merge_complete = value;}

        static bool IsCommitComplete() {return commit_complete;}
        static void SetCommitComplete(bool value) {commit_complete = value;}

        static bool IsRecordCommitted(){ return record_committed;}
        static void SetRecordCommitted(bool value){ record_committed = value;}

        static bool IsCurrentEpochAbort(){ return is_current_epoch_abort;}
        static void SetCurrentEpochAbort(bool value){ is_current_epoch_abort = value;}

        static void SetPhysicalEpoch(int value){ physical_epoch = value;}
        static uint64_t AddPhysicalEpoch(){ return ++ physical_epoch;}
        static uint64_t GetPhysicalEpoch(){ return physical_epoch;}

        static void SetLogicalEpoch(int value){ logical_epoch = value;}
        static uint64_t AddLogicalEpoch(){ return ++ logical_epoch;}
        static uint64_t GetLogicalEpoch(){ return logical_epoch;}


        static void ClearMergeEpochState(uint64_t epoch_mod) {
            merge_complete = commit_complete = record_committed = false;

            epoch_mod %= max_length;
            epoch_merge_map[epoch_mod]->clear();
            epoch_insert_set[epoch_mod]->clear();
            epoch_abort_txn_set[epoch_mod]->clear();
            local_epoch_abort_txn_set[epoch_mod]->clear();

            should_merge_txn_num.Clear(epoch_mod), merged_txn_num.Clear(epoch_mod),
            should_commit_txn_num.Clear(epoch_mod), committed_txn_num.Clear(epoch_mod);
            record_commit_txn_num.Clear(epoch_mod), record_committed_txn_num.Clear(epoch_mod);
        }

        static void ClearLog(uint64_t epoch_mod) {
            redo_log[epoch_mod]->clear();
            committed_txn_cache[epoch_mod]->clear();
            epoch_log_lsn.SetCount(epoch_mod, 0);
        }


        static uint64_t AddShouldReceivePackNum(uint64_t value) {
            return should_receive_pack_num->fetch_add(value);
        }
        static uint64_t SubShouldReceivePackNum(uint64_t value) {
            return should_receive_pack_num->fetch_sub(value);
        }
        static void StoreShouldReceivePackNum(uint64_t value) {
            should_receive_pack_num->store(value);
        }
        static uint64_t GetShouldReceivePackNum() {
            return should_receive_pack_num->load();
        }

        static uint64_t AddOnLineServerNum(uint64_t value) {
            return online_server_num->fetch_add(value);
        }
        static uint64_t SubOnLineServerNum(uint64_t value) {
            return online_server_num->fetch_sub(value);
        }
        static void StoreOnLineServerNum(uint64_t value) {
            online_server_num->store(value);
        }
        static uint64_t GetOnLineServerNum() {
            return online_server_num->load();
        }

        static void SetServerOnLine(std::string ip) {
            for(int i = 0; i < (int)ctx.kServerIp.size(); i++) {
                if(ip == ctx.kServerIp[i]) {
                    server_state.SetCount(i, 1);
                }
            }
        }

        static void SetServerOffLine(std::string ip) {
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
