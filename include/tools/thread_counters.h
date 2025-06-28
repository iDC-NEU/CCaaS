//
// Created by zwx on 24-3-14.
//

#ifndef TAAS_THTEAD_COUNTERS_H
#define TAAS_THTEAD_COUNTERS_H

#pragma once

#include "context.h"
#include "atomic_counters.h"
#include "atomic_counters_cache.h"

namespace Taas {

    class ThreadCounters{
    public:
        uint64_t thread_id = 0, max_length = 0, shard_num = 0, local_server_id, replica_num = 1, server_num = 1;

        static Context ctx;
        static std::atomic<uint64_t> inc_id;
        static std::vector<std::vector<bool>> is_local_shard;

    ///message handling
    public:
        std::shared_ptr<AtomicCounters_Cache>
                shard_should_send_txn_num_local,
                shard_send_txn_num_local,
                shard_should_handle_local_txn_num_local,
                shard_handled_local_txn_num_local,
                shard_should_handle_remote_txn_num_local,
                shard_handled_remote_txn_num_local,
                shard_received_txn_num_local,

                remote_server_should_send_txn_num_local,
                remote_server_send_txn_num_local,
                remote_server_should_handle_txn_num_local,
                remote_server_handled_txn_num_local,
                remote_server_received_txn_num_local,

                backup_should_send_txn_num_local,
                backup_send_txn_num_local,
                backup_received_txn_num_local;

        static std::vector<std::shared_ptr<AtomicCounters_Cache>>
                shard_should_send_txn_num_local_vec,
                shard_send_txn_num_local_vec,
                shard_should_handle_local_txn_num_local_vec,
                shard_handled_local_txn_num_local_vec,
                shard_should_handle_remote_txn_num_local_vec,
                shard_handled_remote_txn_num_local_vec,
                shard_received_txn_num_local_vec,


                remote_server_should_send_txn_num_local_vec,
                remote_server_send_txn_num_local_vec,
                remote_server_should_handle_txn_num_local_vec,
                remote_server_handled_txn_num_local_vec,
                remote_server_received_txn_num_local_vec,


                backup_should_send_txn_num_local_vec,
                backup_send_txn_num_local_vec,
                backup_received_txn_num_local_vec;

        static std::vector<uint64_t>
                shard_send_ack_epoch_num,
                remote_server_send_ack_epoch_num,
                backup_send_ack_epoch_num,
                backup_insert_set_send_ack_epoch_num,
                abort_set_send_ack_epoch_num; /// check and reply ack

        static std::vector<std::unique_ptr<std::atomic<bool>>>
                epoch_shard_handle_complete,
                epoch_shard_send_complete,
                epoch_shard_receive_complete,
                epoch_remote_server_handle_complete,
                epoch_remote_server_send_complete,
                epoch_remote_server_receive_complete,
                epoch_back_up_complete,
                epoch_abort_set_merge_complete,
                epoch_insert_set_complete;

        static AtomicCounters_Cache
            shard_should_receive_pack_num,
            shard_received_pack_num,
            shard_should_receive_txn_num,
            shard_received_ack_num,

            remote_server_should_receive_pack_num,
            remote_server_received_pack_num,
            remote_server_should_receive_txn_num,
            remote_server_received_ack_num,

            backup_should_receive_pack_num,
            backup_received_pack_num,
            backup_should_receive_txn_num,
            backup_received_ack_num,

            insert_set_should_receive_num,
            insert_set_received_num,
            insert_set_received_ack_num,

            abort_set_should_receive_num,
            abort_set_received_num,
            abort_set_received_ack_num,

            meta_info_received_num,

            redo_log_push_down_ack_num,
            redo_log_push_down_local_epoch;

        static bool CheckEpochShardSendComplete(const uint64_t& epoch) ;
        static bool CheckEpochShardReceiveComplete(const uint64_t& epoch) ;

        static bool IsShardSendFinish(const uint64_t &epoch, const uint64_t &shard_id) ;
        static bool IsShardSendFinish(const uint64_t &epoch) ;
        static bool IsShardTxnReceiveComplete(const uint64_t &epoch) ;
        static bool IsShardTxnReceiveComplete(const uint64_t &epoch, const uint64_t &id) ;
        static bool IsShardPackReceiveComplete(const uint64_t &epoch) ;
        static bool IsShardPackReceiveComplete(const uint64_t &epoch, const uint64_t &id) ;




        static bool CheckEpochRemoteServerSendComplete(const uint64_t& epoch) ;
        static bool CheckEpochRemoteServerReceiveComplete(const uint64_t& epoch) ;

        static bool IsRemoteServerSendFinish(const uint64_t &epoch, const uint64_t &shard_id) ;
        static bool IsRemoteServerSendFinish(const uint64_t &epoch) ;
        static bool IsRemoteServerTxnReceiveComplete(const uint64_t &epoch) ;
        static bool IsRemoteServerTxnReceiveComplete(const uint64_t &epoch, const uint64_t &id) ;
        static bool IsRemoteServerPackReceiveComplete(const uint64_t &epoch) ;
        static bool IsRemoteServerPackReceiveComplete(const uint64_t &epoch, const uint64_t &id) ;




        static bool CheckEpochBackUpComplete(const uint64_t& epoch) ;

        static bool IsBackUpSendFinish(const uint64_t &epoch) ;
        static bool IsBackUpTxnReceiveComplete(const uint64_t &epoch) ;
        static bool IsBackUpTxnReceiveComplete(const uint64_t &epoch, const uint64_t &id) ;
        static bool IsBackUpPackReceiveComplete(const uint64_t &epoch) ;
        static bool IsBackUpPackReceiveComplete(const uint64_t &epoch, const uint64_t &id) ;



        static bool CheckEpochAbortSetMergeComplete(const uint64_t& epoch) ;
        static bool CheckEpochInsertSetMergeComplete(const uint64_t& epoch) ;

        static bool IsAbortSetReceiveComplete(const uint64_t &epoch, const uint64_t &id) ;
        static bool IsAbortSetReceiveComplete(const uint64_t &epoch);
        static bool IsInsertSetReceiveComplete(const uint64_t &epoch, const uint64_t &id) ;
        static bool IsInsertSetReceiveComplete(const uint64_t &epoch) ;


        static bool IsShardACKReceiveComplete(const uint64_t &epoch) ;
        static bool IsRemoteServerACKReceiveComplete(const uint64_t &epoch) ;
        static bool IsBackUpACKReceiveComplete(const uint64_t &epoch) ;
        static bool IsAbortSetACKReceiveComplete(const uint64_t &epoch) ;
        static bool IsInsertSetACKReceiveComplete(const uint64_t &epoch) ;
        static bool IsRedoLogPushDownACKReceiveComplete(const uint64_t &epoch) ;



        static bool CheckEpochClientTxnHandleComplete(const uint64_t &epoch) ;
        static bool CheckEpochShardTxnHandleComplete(const uint64_t &epoch) ;




    ///Merge
    public:
        std::shared_ptr<AtomicCounters_Cache>
                epoch_should_read_validate_txn_num_local,
                epoch_read_validated_txn_num_local,
                epoch_should_merge_txn_num_local,
                epoch_merged_txn_num_local,
                epoch_should_commit_txn_num_local,
                epoch_committed_txn_num_local,
                epoch_record_commit_txn_num_local,
                epoch_record_committed_txn_num_local,
                epoch_result_return_txn_num_local,
                epoch_result_returned_txn_num_local;

        std::atomic<uint64_t>
                total_merge_txn_num_local,
                total_merge_latency_local,
                total_commit_txn_num_local,
                total_commit_latency_local,
                success_commit_txn_num_local,
                success_commit_latency_local,
                total_read_version_check_failed_txn_num_local,
                total_failed_txn_num_local;
    public:
        static std::vector<std::shared_ptr<AtomicCounters_Cache>>
                epoch_should_read_validate_txn_num_local_vec,
                epoch_read_validated_txn_num_local_vec,
                epoch_should_merge_txn_num_local_vec,
                epoch_merged_txn_num_local_vec,
                epoch_should_commit_txn_num_local_vec,
                epoch_committed_txn_num_local_vec,
                epoch_record_commit_txn_num_local_vec,
                epoch_record_committed_txn_num_local_vec,
                epoch_result_return_txn_num_local_vec,
                epoch_result_returned_txn_num_local_vec;

        static std::vector<std::unique_ptr<std::atomic<bool>>>
                epoch_read_validate_complete,
                epoch_merge_complete,
                epoch_commit_complete,
                epoch_record_committed,
                epoch_result_returned;

        static std::atomic<uint64_t>
                total_merge_txn_num,
                total_merge_latency,
                total_commit_txn_num,
                total_commit_latency,
                success_commit_txn_num,
                success_commit_latency,
                total_read_version_check_failed_txn_num,
                total_failed_txn_num;


        static bool CheckEpochReadValidateComplete(const uint64_t& epoch);
        static bool CheckEpochMergeComplete(const uint64_t& epoch) ;
        static bool CheckEpochCommitComplete(const uint64_t& epoch) ;
        static bool CheckEpochRecordCommitted(const uint64_t& epoch) ;
        static bool CheckEpochResultReturned(const uint64_t& epoch) ;

        static bool IsReadValidateComplete(const uint64_t& epoch) ;
        static bool IsMergeComplete(const uint64_t& epoch) ;
        static bool IsCommitComplete(const uint64_t & epoch) ;
        static bool IsRecordCommitted(const uint64_t & epoch) ;
        static bool IsResultReturned(const uint64_t & epoch) ;




    public:

        void ThreadCountersInit(const Context& context);
        static bool StaticInit();
        static bool StaticClear(uint64_t& epoch);

        static void ClearAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) ;
        static uint64_t GetAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) ;
        static uint64_t GetAllThreadLocalCountNum(const uint64_t &epoch, const uint64_t &shard_id, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec);

    };

}

#endif //TAAS_THTEAD_COUNTERS_H
