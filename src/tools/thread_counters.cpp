//
// Created by user on 24-3-14.
//
#include "tools/thread_counters.h"
#include "epoch/epoch_manager.h"

namespace Taas{

    std::atomic<uint64_t> ThreadCounters::inc_id(0);
    Context ThreadCounters::ctx;
    std::vector<std::vector<bool>> ThreadCounters::is_local_shard;

    std::vector<std::shared_ptr<AtomicCounters_Cache>>
            ThreadCounters::shard_should_send_txn_num_local_vec,
            ThreadCounters::shard_send_txn_num_local_vec,
            ThreadCounters::shard_should_handle_local_txn_num_local_vec,
            ThreadCounters::shard_handled_local_txn_num_local_vec,
            ThreadCounters::shard_should_handle_remote_txn_num_local_vec,
            ThreadCounters::shard_handled_remote_txn_num_local_vec,
            ThreadCounters::shard_received_txn_num_local_vec,

            ThreadCounters::remote_server_should_send_txn_num_local_vec,
            ThreadCounters::remote_server_send_txn_num_local_vec,
            ThreadCounters::remote_server_should_handle_txn_num_local_vec,
            ThreadCounters::remote_server_handled_txn_num_local_vec,
            ThreadCounters::remote_server_received_txn_num_local_vec,


            ThreadCounters::backup_should_send_txn_num_local_vec,
            ThreadCounters::backup_send_txn_num_local_vec,
            ThreadCounters::backup_received_txn_num_local_vec;

    std::vector<uint64_t>
            ThreadCounters::shard_send_ack_epoch_num,
            ThreadCounters::remote_server_send_ack_epoch_num,
            ThreadCounters::backup_send_ack_epoch_num,
            ThreadCounters::backup_insert_set_send_ack_epoch_num,
            ThreadCounters::abort_set_send_ack_epoch_num;

    std::vector<std::unique_ptr<std::atomic<bool>>>
            ThreadCounters::epoch_shard_handle_complete,
            ThreadCounters::epoch_shard_send_complete,
            ThreadCounters::epoch_shard_receive_complete,
            ThreadCounters::epoch_remote_server_handle_complete,
            ThreadCounters::epoch_remote_server_send_complete,
            ThreadCounters::epoch_remote_server_receive_complete,
            ThreadCounters::epoch_back_up_complete,
            ThreadCounters::epoch_abort_set_merge_complete,
            ThreadCounters::epoch_insert_set_complete;

    AtomicCounters_Cache
            ThreadCounters::shard_should_receive_pack_num(10, 1),
            ThreadCounters::shard_received_pack_num(10, 1),
            ThreadCounters::shard_should_receive_txn_num(10, 1),
            ThreadCounters::shard_received_ack_num(10, 1),

            ThreadCounters::remote_server_should_receive_pack_num(10, 1),
            ThreadCounters::remote_server_received_pack_num(10, 1),
            ThreadCounters::remote_server_should_receive_txn_num(10, 1),
            ThreadCounters::remote_server_received_ack_num(10, 1),

            ThreadCounters::backup_should_receive_pack_num(10, 1),
            ThreadCounters::backup_received_pack_num(10, 1),
            ThreadCounters::backup_should_receive_txn_num(10, 1),
            ThreadCounters::backup_received_ack_num(10, 1),

            ThreadCounters::insert_set_should_receive_num(10, 1),
            ThreadCounters::insert_set_received_num(10, 1),
            ThreadCounters::insert_set_received_ack_num(10, 1),

            ThreadCounters::abort_set_should_receive_num(10, 1),
            ThreadCounters::abort_set_received_num(10, 1),
            ThreadCounters::abort_set_received_ack_num(10, 1),

            ThreadCounters::meta_info_received_num(10, 1),

            ThreadCounters::redo_log_push_down_ack_num(10, 1),
            ThreadCounters::redo_log_push_down_local_epoch(10, 1);






    std::vector<std::shared_ptr<AtomicCounters_Cache>>
            ThreadCounters::epoch_should_read_validate_txn_num_local_vec,
            ThreadCounters::epoch_read_validated_txn_num_local_vec,
            ThreadCounters::epoch_should_merge_txn_num_local_vec,
            ThreadCounters::epoch_merged_txn_num_local_vec,
            ThreadCounters::epoch_should_commit_txn_num_local_vec,
            ThreadCounters::epoch_committed_txn_num_local_vec,
            ThreadCounters::epoch_record_commit_txn_num_local_vec,
            ThreadCounters::epoch_record_committed_txn_num_local_vec,
            ThreadCounters::epoch_result_return_txn_num_local_vec,
            ThreadCounters::epoch_result_returned_txn_num_local_vec;

    std::vector<std::unique_ptr<std::atomic<bool>>>
            ThreadCounters::epoch_read_validate_complete,
            ThreadCounters::epoch_merge_complete,
            ThreadCounters::epoch_commit_complete,
            ThreadCounters::epoch_record_committed,
            ThreadCounters::epoch_result_returned;

    std::atomic<uint64_t>
            ThreadCounters::total_merge_txn_num(0),
            ThreadCounters::total_merge_latency(0),
            ThreadCounters::total_commit_txn_num(0),
            ThreadCounters::total_commit_latency(0),
            ThreadCounters::success_commit_txn_num(0),
            ThreadCounters::success_commit_latency(0),
            ThreadCounters::total_read_version_check_failed_txn_num(0),
            ThreadCounters::total_failed_txn_num(0);







    void ThreadCounters::ThreadCountersInit(const Context& context) {
        thread_id = inc_id.fetch_add(1);
        shard_num = TaasContext::kShardNum;
        replica_num = TaasContext::kReplicaNum;
        server_num = TaasContext::kTxnNodeNum;
        max_length = TaasContext::kCacheMaxLength;
        local_server_id = TaasContext::txn_node_ip_index;

        shard_should_send_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        shard_send_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num);
        shard_should_handle_local_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        shard_handled_local_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        shard_should_handle_remote_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        shard_handled_remote_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        shard_received_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),


        remote_server_should_send_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        remote_server_send_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        remote_server_should_handle_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        remote_server_handled_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        remote_server_received_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),

        backup_should_send_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        backup_send_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        backup_received_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num);

        shard_should_send_txn_num_local_vec[thread_id] = shard_should_send_txn_num_local;
        shard_send_txn_num_local_vec[thread_id] = shard_send_txn_num_local;
        shard_should_handle_local_txn_num_local_vec[thread_id] = shard_should_handle_local_txn_num_local;
        shard_handled_local_txn_num_local_vec[thread_id] = shard_handled_local_txn_num_local;
        shard_should_handle_remote_txn_num_local_vec[thread_id] = shard_should_handle_remote_txn_num_local;
        shard_handled_remote_txn_num_local_vec[thread_id] = shard_handled_remote_txn_num_local;
        shard_received_txn_num_local_vec[thread_id] = shard_received_txn_num_local;


        remote_server_should_send_txn_num_local_vec[thread_id] = remote_server_should_send_txn_num_local;
        remote_server_send_txn_num_local_vec[thread_id] = remote_server_send_txn_num_local;
        remote_server_should_handle_txn_num_local_vec[thread_id] = remote_server_should_handle_txn_num_local;
        remote_server_handled_txn_num_local_vec[thread_id] = remote_server_handled_txn_num_local;
        remote_server_received_txn_num_local_vec[thread_id] = remote_server_received_txn_num_local;


        backup_should_send_txn_num_local_vec[thread_id] = backup_should_send_txn_num_local;
        backup_send_txn_num_local_vec[thread_id] = backup_send_txn_num_local;
        backup_received_txn_num_local_vec[thread_id] = backup_received_txn_num_local;




        epoch_should_read_validate_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        epoch_read_validated_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num);
        epoch_should_merge_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        epoch_merged_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        epoch_should_commit_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        epoch_committed_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        epoch_record_commit_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num),
        epoch_record_committed_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, server_num);
        epoch_result_return_txn_num_local  = std::make_shared<AtomicCounters_Cache>(max_length, server_num);
        epoch_result_returned_txn_num_local  = std::make_shared<AtomicCounters_Cache>(max_length, server_num);


        epoch_should_read_validate_txn_num_local_vec[thread_id] = epoch_should_read_validate_txn_num_local;
        epoch_read_validated_txn_num_local_vec[thread_id] = epoch_read_validated_txn_num_local;
        epoch_should_merge_txn_num_local_vec[thread_id] = epoch_should_merge_txn_num_local;
        epoch_merged_txn_num_local_vec[thread_id] = epoch_merged_txn_num_local;
        epoch_should_commit_txn_num_local_vec[thread_id] = epoch_should_commit_txn_num_local;
        epoch_committed_txn_num_local_vec[thread_id] = epoch_committed_txn_num_local;
        epoch_record_commit_txn_num_local_vec[thread_id] = epoch_record_commit_txn_num_local;
        epoch_record_committed_txn_num_local_vec[thread_id] = epoch_record_committed_txn_num_local;
        epoch_result_return_txn_num_local_vec[thread_id] = epoch_result_return_txn_num_local;
        epoch_result_returned_txn_num_local_vec[thread_id] = epoch_result_returned_txn_num_local;

    }

    bool ThreadCounters::StaticInit() {
        auto thread_total_num = TaasContext::kMergeThreadNum * 2
                + TaasContext::kEpochMessageThreadNum + TaasContext::kEpochTxnThreadNum;
        auto max_length = TaasContext::kCacheMaxLength;
        auto shard_num = TaasContext::kShardNum;
        auto replica_num = TaasContext::kReplicaNum;
        auto server_num = TaasContext::kTxnNodeNum;

        is_local_shard.resize(TaasContext::kTxnNodeNum);
        for(auto &i : is_local_shard) {
            i.resize(TaasContext::kShardNum);
        }
        for(uint64_t server_id = 0; server_id < TaasContext::kTxnNodeNum; server_id ++) {
            for(uint64_t i = 0; i < TaasContext::kShardNum; i ++) {
                for(uint64_t j = 0; j < TaasContext::kReplicaNum; j ++ ) {
                    if((i + TaasContext::kTxnNodeNum - j) % TaasContext::kTxnNodeNum == server_id) {
                        is_local_shard[server_id][i] = true;
                    }
                }
            }
        }

        shard_should_send_txn_num_local_vec.resize(thread_total_num);
        shard_send_txn_num_local_vec.resize(thread_total_num);
        shard_should_handle_local_txn_num_local_vec.resize(thread_total_num);
        shard_handled_local_txn_num_local_vec.resize(thread_total_num);
        shard_should_handle_remote_txn_num_local_vec.resize(thread_total_num);
        shard_handled_remote_txn_num_local_vec.resize(thread_total_num);
        shard_received_txn_num_local_vec.resize(thread_total_num);

        remote_server_should_send_txn_num_local_vec.resize(thread_total_num);
        remote_server_send_txn_num_local_vec.resize(thread_total_num);
        remote_server_should_handle_txn_num_local_vec.resize(thread_total_num);
        remote_server_handled_txn_num_local_vec.resize(thread_total_num);
        remote_server_received_txn_num_local_vec.resize(thread_total_num);


        backup_should_send_txn_num_local_vec.resize(thread_total_num);
        backup_send_txn_num_local_vec.resize(thread_total_num);
        backup_received_txn_num_local_vec.resize(thread_total_num);

        shard_send_ack_epoch_num.resize(server_num + 1);
        remote_server_send_ack_epoch_num.resize(server_num + 1);
        backup_send_ack_epoch_num.resize(server_num + 1);
        backup_insert_set_send_ack_epoch_num.resize(server_num + 1);
        abort_set_send_ack_epoch_num.resize(server_num + 1);
        for(int i = 0; i <= (int) server_num; i ++ ) { /// start at 1, not 0
            shard_send_ack_epoch_num[i] = 1;
            remote_server_send_ack_epoch_num[i] = 1;
            backup_send_ack_epoch_num[i] = 1;
            backup_insert_set_send_ack_epoch_num[i] = 1;
            abort_set_send_ack_epoch_num[i] = 1;
        }

        epoch_shard_handle_complete.resize(max_length);
        epoch_shard_send_complete.resize(max_length);
        epoch_shard_receive_complete.resize(max_length);
        epoch_remote_server_handle_complete.resize(max_length);
        epoch_remote_server_send_complete.resize(max_length);
        epoch_remote_server_receive_complete.resize(max_length);
        epoch_back_up_complete.resize(max_length);
        epoch_abort_set_merge_complete.resize(max_length);
        epoch_insert_set_complete.resize(max_length);
        for(int i = 0; i < static_cast<int>(max_length); i ++) {
            epoch_shard_handle_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_shard_send_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_shard_receive_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_remote_server_handle_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_remote_server_send_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_remote_server_receive_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_back_up_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_abort_set_merge_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_insert_set_complete[i] = std::make_unique<std::atomic<bool>>(false);
        }

        shard_should_receive_pack_num.Init(max_length, server_num, 1),
        shard_received_pack_num.Init(max_length, server_num),
        shard_should_receive_txn_num.Init(max_length, server_num, 0),
        shard_received_ack_num.Init(max_length, server_num),

        remote_server_should_receive_pack_num.Init(max_length, server_num, 1),
        remote_server_received_pack_num.Init(max_length, server_num),
        remote_server_should_receive_txn_num.Init(max_length, server_num, 0),
        remote_server_received_ack_num.Init(max_length, server_num, 0),

        backup_should_receive_pack_num.Init(max_length, server_num, 1),
        backup_received_pack_num.Init(max_length, server_num),
        backup_should_receive_txn_num.Init(max_length, server_num, 0),
        backup_received_ack_num.Init(max_length, server_num),

        insert_set_should_receive_num.Init(max_length, server_num, 1),
        insert_set_received_num.Init(max_length, server_num),
        insert_set_received_ack_num.Init(max_length, server_num),

        abort_set_should_receive_num.Init(max_length, server_num, 1),
        abort_set_received_num.Init(max_length, server_num);
        abort_set_received_ack_num.Init(max_length, server_num);

        redo_log_push_down_ack_num.Init(max_length, server_num);
        redo_log_push_down_local_epoch.Init(max_length, server_num);





        ///Merge
        epoch_should_read_validate_txn_num_local_vec.resize(thread_total_num);
        epoch_read_validated_txn_num_local_vec.resize(thread_total_num);
        epoch_should_merge_txn_num_local_vec.resize(thread_total_num);
        epoch_merged_txn_num_local_vec.resize(thread_total_num);
        epoch_should_commit_txn_num_local_vec.resize(thread_total_num);
        epoch_committed_txn_num_local_vec.resize(thread_total_num);
        epoch_record_commit_txn_num_local_vec.resize(thread_total_num);
        epoch_record_committed_txn_num_local_vec.resize(thread_total_num);
        epoch_result_return_txn_num_local_vec.resize(thread_total_num);
        epoch_result_returned_txn_num_local_vec.resize(thread_total_num);

        ///epoch merge state
        epoch_read_validate_complete.resize(TaasContext::kCacheMaxLength);
        epoch_merge_complete.resize(TaasContext::kCacheMaxLength);
        epoch_commit_complete.resize(TaasContext::kCacheMaxLength);
        epoch_record_committed.resize(TaasContext::kCacheMaxLength);
        epoch_result_returned.resize(TaasContext::kCacheMaxLength);
        for(int i = 0; i < static_cast<int>(TaasContext::kCacheMaxLength); i ++) {
            epoch_read_validate_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_merge_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_commit_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_record_committed[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_result_returned[i] = std::make_unique<std::atomic<bool>>(false);
        }
        return true;
    }

    bool ThreadCounters::StaticClear(uint64_t& epoch) {
        auto epoch_mod_temp = epoch % TaasContext::kCacheMaxLength;
        auto cache_clear_epoch_num_mod = epoch % TaasContext::kCacheMaxLength;

        ///Message handle
        shard_should_receive_pack_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
        shard_received_pack_num.Clear(cache_clear_epoch_num_mod, 0),
        shard_should_receive_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        shard_received_ack_num.Clear(cache_clear_epoch_num_mod, 0),

        remote_server_should_receive_pack_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
        remote_server_received_pack_num.Clear(cache_clear_epoch_num_mod, 0),
        remote_server_should_receive_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        remote_server_received_ack_num.Clear(cache_clear_epoch_num_mod, 0),

        backup_should_receive_pack_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
        backup_received_pack_num.Clear(cache_clear_epoch_num_mod, 0),
        backup_should_receive_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        backup_received_ack_num.Clear(cache_clear_epoch_num_mod, 0),

        insert_set_should_receive_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
        insert_set_received_num.Clear(cache_clear_epoch_num_mod, 0),
        insert_set_received_ack_num.Clear(cache_clear_epoch_num_mod, 0),
        abort_set_should_receive_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
        abort_set_received_num.Clear(cache_clear_epoch_num_mod, 0);
        abort_set_received_ack_num.Clear(cache_clear_epoch_num_mod, 0);
        redo_log_push_down_ack_num.Clear(cache_clear_epoch_num_mod, 0);
        redo_log_push_down_local_epoch.Clear(cache_clear_epoch_num_mod, 0);

        epoch_shard_handle_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_shard_send_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_shard_receive_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_remote_server_handle_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_remote_server_send_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_remote_server_receive_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_back_up_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_abort_set_merge_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_insert_set_complete[cache_clear_epoch_num_mod]->store(false);


        ///Merge
        epoch_read_validate_complete[epoch_mod_temp]->store(false);
        epoch_merge_complete[epoch_mod_temp]->store(false);
        epoch_commit_complete[epoch_mod_temp]->store(false);
        epoch_record_committed[epoch_mod_temp]->store(false);
        epoch_result_returned[epoch_mod_temp]->store(false);


        ClearAllThreadLocalCountNum(epoch, shard_should_send_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, shard_send_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, shard_should_handle_local_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, shard_handled_local_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, shard_should_handle_remote_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, shard_handled_remote_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, shard_received_txn_num_local_vec);


        ClearAllThreadLocalCountNum(epoch, remote_server_should_send_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, remote_server_send_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, remote_server_should_handle_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, remote_server_handled_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, remote_server_received_txn_num_local_vec);


        ClearAllThreadLocalCountNum(epoch, backup_should_send_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, backup_send_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, backup_received_txn_num_local_vec);


        ClearAllThreadLocalCountNum(epoch, epoch_should_read_validate_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_read_validated_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_should_merge_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_merged_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_should_commit_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_committed_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_record_commit_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_record_committed_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_result_return_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_result_returned_txn_num_local_vec);

        return true;
    }




    bool ThreadCounters::CheckEpochShardSendComplete(const uint64_t& epoch) {
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        if(epoch_shard_send_complete[epoch_mod]->load()) {
            return true;
        }
        if (epoch < EpochManager::GetPhysicalEpoch() &&
            IsShardACKReceiveComplete(epoch) &&
            IsShardSendFinish(epoch)
                ) {
            epoch_shard_send_complete[epoch_mod]->store(true);
            return true;
        }
        return false;
    }
    bool ThreadCounters::CheckEpochShardReceiveComplete(const uint64_t& epoch) {
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        if (epoch_shard_receive_complete[epoch_mod]->load()) return true;
        if (epoch < EpochManager::GetPhysicalEpoch() &&
            IsShardPackReceiveComplete(epoch) &&
            IsShardTxnReceiveComplete(epoch)) {
            epoch_shard_receive_complete[epoch_mod]->store(true);
            return true;
        }
        return false;
    }

    bool ThreadCounters::IsShardSendFinish(const uint64_t &epoch, const uint64_t &shard_id) {
        return epoch < EpochManager::GetPhysicalEpoch() &&

               GetAllThreadLocalCountNum(epoch, shard_id, shard_send_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, shard_id, shard_should_send_txn_num_local_vec) &&

               GetAllThreadLocalCountNum(epoch, shard_id, shard_handled_local_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, shard_id, shard_should_handle_local_txn_num_local_vec);
    }
    bool ThreadCounters::IsShardSendFinish(const uint64_t &epoch) {
        return epoch < EpochManager::GetPhysicalEpoch() &&

               GetAllThreadLocalCountNum(epoch, shard_send_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, shard_should_send_txn_num_local_vec) &&

               GetAllThreadLocalCountNum(epoch, shard_handled_local_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, shard_should_handle_local_txn_num_local_vec)
                ;
    }
    bool ThreadCounters::IsShardTxnReceiveComplete(const uint64_t &epoch) {
        if(GetAllThreadLocalCountNum(epoch, shard_received_txn_num_local_vec) < shard_should_receive_txn_num.GetCount(epoch))
            return false;
        return true;
    }
    bool ThreadCounters::IsShardTxnReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
        return GetAllThreadLocalCountNum(epoch, shard_received_txn_num_local_vec) >= shard_should_receive_txn_num.GetCount(epoch, id);
    }
    bool ThreadCounters::IsShardPackReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < TaasContext::kTxnNodeNum; i ++) {
            if(i == TaasContext::txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(shard_received_pack_num.GetCount(epoch, i) < shard_should_receive_pack_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadCounters::IsShardPackReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
        return shard_received_pack_num.GetCount(epoch, id) >= shard_should_receive_pack_num.GetCount(epoch, id);
    }








    bool ThreadCounters::CheckEpochRemoteServerSendComplete(const uint64_t& epoch) {
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        if(epoch_remote_server_send_complete[epoch_mod]->load()) {
            return true;
        }
        if (epoch < EpochManager::GetPhysicalEpoch() &&
            IsRemoteServerACKReceiveComplete(epoch) &&
            IsRemoteServerSendFinish(epoch)
                ) {
            epoch_remote_server_send_complete[epoch_mod]->store(true);
            return true;
        }
        return false;
    }
    bool ThreadCounters::CheckEpochRemoteServerReceiveComplete(const uint64_t& epoch) {
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        if (epoch_remote_server_receive_complete[epoch_mod]->load()) return true;
        if (epoch < EpochManager::GetPhysicalEpoch() &&
            IsRemoteServerPackReceiveComplete(epoch) &&
            IsRemoteServerTxnReceiveComplete(epoch)) {
            epoch_remote_server_receive_complete[epoch_mod]->store(true);
            return true;
        }
        return false;
    }

    bool ThreadCounters::IsRemoteServerSendFinish(const uint64_t &epoch, const uint64_t &shard_id) {
        return epoch < EpochManager::GetPhysicalEpoch() &&

               GetAllThreadLocalCountNum(epoch, shard_id, remote_server_send_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, shard_id, remote_server_should_send_txn_num_local_vec) &&

               GetAllThreadLocalCountNum(epoch, shard_id, remote_server_handled_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, shard_id, remote_server_should_handle_txn_num_local_vec);
    }
    bool ThreadCounters::IsRemoteServerSendFinish(const uint64_t &epoch) {
        return epoch < EpochManager::GetPhysicalEpoch() &&

               GetAllThreadLocalCountNum(epoch, remote_server_send_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, remote_server_should_send_txn_num_local_vec) &&

               GetAllThreadLocalCountNum(epoch, remote_server_handled_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, remote_server_should_handle_txn_num_local_vec)
                ;
    }
    bool ThreadCounters::IsRemoteServerTxnReceiveComplete(const uint64_t &epoch) {
        if(GetAllThreadLocalCountNum(epoch, remote_server_received_txn_num_local_vec) < remote_server_should_receive_txn_num.GetCount(epoch))
            return false;
        return true;
    }
    bool ThreadCounters::IsRemoteServerTxnReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
        return GetAllThreadLocalCountNum(epoch, remote_server_received_txn_num_local_vec) >= remote_server_should_receive_txn_num.GetCount(epoch, id);
    }
    bool ThreadCounters::IsRemoteServerPackReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < TaasContext::kTxnNodeNum; i ++) {
            if(i == TaasContext::txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(remote_server_received_pack_num.GetCount(epoch, i) < remote_server_should_receive_pack_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadCounters::IsRemoteServerPackReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
        return remote_server_received_pack_num.GetCount(epoch, id) >= remote_server_should_receive_pack_num.GetCount(epoch, id);
    }








    bool ThreadCounters::CheckEpochBackUpComplete(const uint64_t& epoch) {
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        if (epoch_back_up_complete[epoch_mod]->load()) return true;
        if(epoch < EpochManager::GetPhysicalEpoch() && IsBackUpACKReceiveComplete(epoch)
           &&IsBackUpSendFinish(epoch)) {
            epoch_back_up_complete[epoch_mod]->store(true);
            return true;
        }
        return false;
    }

    bool ThreadCounters::IsBackUpSendFinish(const uint64_t &epoch) {
        return epoch < EpochManager::GetPhysicalEpoch() &&
               GetAllThreadLocalCountNum(epoch, backup_send_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, backup_should_send_txn_num_local_vec) &&

               GetAllThreadLocalCountNum(epoch, shard_handled_local_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, shard_should_handle_local_txn_num_local_vec)
                ;
    }
    bool ThreadCounters::IsBackUpTxnReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < TaasContext::kTxnNodeNum; i ++) {
            if(i == TaasContext::txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(GetAllThreadLocalCountNum(epoch, i, backup_received_txn_num_local_vec) < backup_should_receive_txn_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadCounters::IsBackUpTxnReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
        return GetAllThreadLocalCountNum(epoch, id, backup_received_txn_num_local_vec) >= backup_should_receive_txn_num.GetCount(epoch, id);
    }
    bool ThreadCounters::IsBackUpPackReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < TaasContext::kTxnNodeNum; i ++) {
            if(i == TaasContext::txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(backup_received_pack_num.GetCount(epoch, i) < backup_should_receive_pack_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadCounters::IsBackUpPackReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
        return backup_received_pack_num.GetCount(epoch, id) >= backup_should_receive_pack_num.GetCount(epoch, id);
    }



    bool ThreadCounters::CheckEpochAbortSetMergeComplete(const uint64_t& epoch) {
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        if(epoch_abort_set_merge_complete[epoch_mod]->load()) return true;
        if(epoch < EpochManager::GetPhysicalEpoch() &&
           IsAbortSetACKReceiveComplete(epoch) &&
           IsAbortSetReceiveComplete(epoch)
                ) {
            epoch_abort_set_merge_complete[epoch_mod]->store(true);
            return true;
        }
        return false;
    }
    bool ThreadCounters::CheckEpochInsertSetMergeComplete(const uint64_t& epoch) {
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        if(epoch_insert_set_complete[epoch_mod]->load()) return true;
        if(epoch < EpochManager::GetPhysicalEpoch() &&
           IsInsertSetACKReceiveComplete(epoch) &&
           IsInsertSetReceiveComplete(epoch)
                ) {
            epoch_insert_set_complete[epoch_mod]->store(true);
            return true;
        }
        return false;
    }

    bool ThreadCounters::IsAbortSetReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
        return abort_set_received_num.GetCount(epoch, id) >= abort_set_should_receive_num.GetCount(epoch, id);
    }
    bool ThreadCounters::IsAbortSetReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < TaasContext::kTxnNodeNum; i ++) {
            if(i == TaasContext::txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(abort_set_received_num.GetCount(epoch, i) < abort_set_should_receive_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadCounters::IsInsertSetReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
        return insert_set_received_num.GetCount(epoch, id) >= insert_set_should_receive_num.GetCount(epoch, id);
    }
    bool ThreadCounters::IsInsertSetReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < TaasContext::kTxnNodeNum; i ++) {
            if(i == TaasContext::txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(insert_set_received_num.GetCount(epoch, i) < insert_set_should_receive_num.GetCount(epoch, i)) return false;
        }
        return true;
    }





    bool ThreadCounters::IsShardACKReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < TaasContext::kTxnNodeNum; i ++) {
            if(i == TaasContext::txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(shard_received_ack_num.GetCount(epoch, i) < shard_should_receive_pack_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadCounters::IsRemoteServerACKReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < TaasContext::kTxnNodeNum; i ++) {
            if(i == TaasContext::txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(remote_server_received_ack_num.GetCount(epoch, i) < remote_server_should_receive_pack_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadCounters::IsBackUpACKReceiveComplete(const uint64_t &epoch) {
        uint64_t to_id ;
        for(uint64_t i = 0; i < TaasContext::kBackUpNum; i ++) { /// send to i+1, i+2...i+kBackNum-1
            to_id = (TaasContext::txn_node_ip_index + i + 1) % TaasContext::kTxnNodeNum;
            if(to_id == (uint64_t)TaasContext::txn_node_ip_index || EpochManager::server_state.GetCount(epoch, to_id) == 0) continue;
            if(backup_received_ack_num.GetCount(epoch, to_id) < backup_should_receive_pack_num.GetCount(epoch, to_id)) return false;
        }
        return true;
    }
    bool ThreadCounters::IsAbortSetACKReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < TaasContext::kTxnNodeNum; i ++) {
            if(i == TaasContext::txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(abort_set_received_ack_num.GetCount(epoch, i) < abort_set_should_receive_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadCounters::IsInsertSetACKReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < TaasContext::kTxnNodeNum; i ++) {
            if(i == TaasContext::txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(insert_set_received_ack_num.GetCount(epoch, i) < insert_set_should_receive_num.GetCount(epoch, i)) return false;
        }
        return true;
    }
    bool ThreadCounters::IsRedoLogPushDownACKReceiveComplete(const uint64_t &epoch) {
        for(uint64_t i = 0; i < TaasContext::kTxnNodeNum; i ++) {
            if(i == TaasContext::txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
            if(redo_log_push_down_ack_num.GetCount(epoch, i) < EpochManager::server_state.GetCount(epoch, i)) return false;
        }
        return true;
    }



    bool ThreadCounters::CheckEpochClientTxnHandleComplete(const uint64_t &epoch) {
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        if(epoch_shard_handle_complete[epoch_mod]->load()) {
            return true;
        }
        else {
            if(epoch < EpochManager::GetPhysicalEpoch() &&
            GetAllThreadLocalCountNum(epoch, shard_handled_local_txn_num_local_vec) >=
            GetAllThreadLocalCountNum(epoch, shard_should_handle_local_txn_num_local_vec)) {
                epoch_shard_handle_complete[epoch_mod]->store(true);
                return true;
            }
            return false;
        }
    }

    bool ThreadCounters::CheckEpochShardTxnHandleComplete(const uint64_t &epoch) {
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        if(epoch_remote_server_handle_complete[epoch_mod]->load()) {
            return true;
        }
        else {
            if(epoch < EpochManager::GetPhysicalEpoch() &&
               GetAllThreadLocalCountNum(epoch, remote_server_handled_txn_num_local_vec) >=
               GetAllThreadLocalCountNum(epoch, remote_server_should_handle_txn_num_local_vec)) {
                epoch_remote_server_handle_complete[epoch_mod]->store(true);
                return true;
            }
            return false;
        }
    }






















    bool ThreadCounters::CheckEpochReadValidateComplete(const uint64_t& epoch) {
        if(epoch_read_validate_complete[epoch % TaasContext::kCacheMaxLength]->load()) {
            return true;
        }
        if (epoch < EpochManager::GetPhysicalEpoch() && IsReadValidateComplete(epoch)) {
            epoch_read_validate_complete[epoch % TaasContext::kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }
    bool ThreadCounters::CheckEpochMergeComplete(const uint64_t& epoch) {
        if(epoch_merge_complete[epoch % TaasContext::kCacheMaxLength]->load()) {
            return true;
        }
        if (epoch < EpochManager::GetPhysicalEpoch() && IsMergeComplete(epoch)) {
            epoch_merge_complete[epoch % TaasContext::kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }
    bool ThreadCounters::CheckEpochCommitComplete(const uint64_t& epoch) {
        if (epoch_commit_complete[epoch % TaasContext::kCacheMaxLength]->load()) return true;
        if (epoch < EpochManager::GetPhysicalEpoch() && IsCommitComplete(epoch)) {
            epoch_commit_complete[epoch % TaasContext::kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }
    bool ThreadCounters::CheckEpochRecordCommitted(const uint64_t& epoch) {
        if (epoch_record_committed[epoch % TaasContext::kCacheMaxLength]->load()) return true;
        if (epoch < EpochManager::GetPhysicalEpoch() && IsCommitComplete(epoch) && IsRecordCommitted(epoch)) {
            epoch_record_committed[epoch % TaasContext::kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }

    bool ThreadCounters::CheckEpochResultReturned(const uint64_t& epoch) {
        if (epoch_result_returned[epoch % TaasContext::kCacheMaxLength]->load()) return true;
        if (epoch < EpochManager::GetPhysicalEpoch() && IsRecordCommitted(epoch) && IsResultReturned(epoch)) {
            epoch_result_returned[epoch % TaasContext::kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }





    bool ThreadCounters::IsReadValidateComplete(const uint64_t& epoch) {
        if(GetAllThreadLocalCountNum(epoch, epoch_should_read_validate_txn_num_local_vec) >
           GetAllThreadLocalCountNum(epoch, epoch_read_validated_txn_num_local_vec))
            return false;
        return true;
    }
    bool ThreadCounters::IsMergeComplete(const uint64_t& epoch) {
        if(GetAllThreadLocalCountNum(epoch,epoch_should_read_validate_txn_num_local_vec) >
           GetAllThreadLocalCountNum(epoch,epoch_read_validated_txn_num_local_vec))
                return false;
        if(GetAllThreadLocalCountNum(epoch, epoch_should_merge_txn_num_local_vec) >
           GetAllThreadLocalCountNum(epoch, epoch_merged_txn_num_local_vec))
                return false;
        return true;
    }
    bool ThreadCounters::IsCommitComplete(const uint64_t & epoch) {
        if(GetAllThreadLocalCountNum(epoch, epoch_should_commit_txn_num_local_vec) >
           GetAllThreadLocalCountNum(epoch, epoch_committed_txn_num_local_vec))
            return false;
        return true;
    }
    bool ThreadCounters::IsRecordCommitted(const uint64_t & epoch) {
        if(GetAllThreadLocalCountNum(epoch, epoch_record_commit_txn_num_local_vec) >
           GetAllThreadLocalCountNum(epoch, epoch_record_committed_txn_num_local_vec))
            return false;
        return true;
    }

    bool ThreadCounters::IsResultReturned(const uint64_t & epoch) {
        if(GetAllThreadLocalCountNum(epoch, epoch_result_return_txn_num_local_vec) >
           GetAllThreadLocalCountNum(epoch, epoch_result_returned_txn_num_local_vec))
            return false;
        return true;
    }

    void ThreadCounters::ClearAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        for(const auto& i : vec) {
            if(i != nullptr)
                i->Clear(epoch);
        }
    }

    uint64_t ThreadCounters::GetAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        uint64_t ans = 0;
        for(const auto& i : vec) {
            if(i != nullptr)
                ans += i->GetCount(epoch);
        }
        return ans;
    }
    uint64_t ThreadCounters::GetAllThreadLocalCountNum(const uint64_t &epoch, const uint64_t &shard_id, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        uint64_t ans = 0;
        for(const auto& i : vec) {
            if(i != nullptr)
                ans += i->GetCount(epoch, shard_id);
        }
        return ans;
    }

}