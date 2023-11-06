//
// Created by zwx on 23-8-29.
//

#ifndef TAAS_MOT_H
#define TAAS_MOT_H

#pragma once

#include "tools/context.h"
#include "txn_type.h"
#include "multi_model_workload.h"


namespace workload {
    class MOT {
    public:
        static void Init();
        static void InsertData(const uint64_t&  tid);
        static void RunTxn(const uint64_t& tid, const std::shared_ptr<std::atomic<uint64_t>>& sunTxnNum, std::shared_ptr<std::atomic<uint64_t>>& txn_num);
        static void CloseDB();
    };
}



#endif //TAAS_MOT_H
