//
// Created by user on 23-4-4.
//

#ifndef TAAS_BACKUP_H
#define TAAS_BACKUP_H

#pragma once

#include "tools/atomic_counters.h"
#include "tools/context.h"

namespace Taas {
    class BackUp {
        static AtomicCounters_Cache
        ///backup txn counters
        backup_should_send_txn_num, backup_send_txn_num,
                backup_should_receive_pack_num, backup_received_pack_num,
                backup_should_receive_txn_num, backup_received_txn_num,
        ///backup ack
        backup_received_ack_num;
        static Context ctx;
        static void Init(const Context& ctx);
    };
}



#endif //TAAS_BACKUP_H
