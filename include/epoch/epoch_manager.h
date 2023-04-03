//
// Created by 周慰星 on 11/8/22.
//

#ifndef TAAS_EPOCH_MANANGER_H
#define TAAS_EPOCH_MANANGER_H
#include <unistd.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>
#include <cassert>

#include "tools/atomic_counters.h"
#include "tools/context.h"

namespace Taas {

    extern std::atomic<int> init_ok_num;
    extern std::atomic<bool> is_epoch_advance_started, test_start;
    extern void InitEpochTimerManager(Context& ctx);
    extern void EpochLogicalTimerManagerThreadMain(Context ctx);
    extern void EpochPhysicalTimerManagerThreadMain(Context ctx);

    class EpochManager {
    private:
        static bool timerStop;
        static std::atomic<uint64_t> logical_epoch, physical_epoch, push_down_epoch;
    public:

        static Context ctx;
        static uint64_t max_length;
        static std::vector<std::unique_ptr<std::atomic<bool>>>
                    merge_complete, abort_set_merge_complete,
                    commit_complete, record_committed,
                    is_current_epoch_abort;

        ///epoch, index, value  for 集群状态
        static AtomicCounters_Cache server_state; /// epoch, csn(atomic increase)
        static  std::vector<std::unique_ptr<std::atomic<uint64_t>>> online_server_num;
        ///fault tolerance: cache server mod
        static std::vector<std::unique_ptr<std::atomic<uint64_t>>>  cache_server_received_epoch;


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
        }

        static void EpochCacheSafeCheck() {
            if((GetLogicalEpoch() % ctx.kCacheMaxLength) ==  ((GetPhysicalEpoch() + 55) % ctx.kCacheMaxLength) ||
                GetPushDownEpoch() % ctx.kCacheMaxLength ==  ((GetPhysicalEpoch() + 55) % ctx.kCacheMaxLength)) {
                printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
                printf("+++++++++++++++Fata : Cache Size exceeded!!! +++++++++++++++++++++\n");
                printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
                assert(false);
            }
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
        static void SetServerOnLine(uint64_t epoch, const std::string& ip);
        static void SetServerOffLine(uint64_t epoch, const std::string& ip);

        static void SetCacheServerStored(uint64_t epoch, uint64_t value) {
            cache_server_received_epoch[epoch % max_length]->store(value);
        }

        static bool IsInitOK() {
            return init_ok_num.load() >= 1;
        }

    };
}


#endif //TAAS_EPOCH_MANANGER_H
