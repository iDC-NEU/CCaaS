//
// Created by user on 23-4-4.
//

#include "storage/backup.h"

namespace Taas {
    AtomicCounters_Cache
        ///backup send counters
        BackUp::backup_should_send_txn_num(10, 1),
        BackUp::backup_send_txn_num(10, 1),
        BackUp::backup_received_ack_num(10, 1),
        ///backup receive
        BackUp::backup_should_receive_pack_num(10, 1),
        BackUp::backup_received_pack_num(10, 1),
        BackUp::backup_should_receive_txn_num(10, 1),
        BackUp::backup_received_txn_num(10, 1);

    void Taas::BackUp::Init(Taas::Context &ctx) {

    }
}

