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
        static std::vector<MOTTxn > txnVector;

        static void Init(const Taas::Context& ctx);
        static void InsertData(uint64_t tid);
        static void GenerateTxn(uint64_t tid);
        static void CloseDB();
        static void RunTxn(const MOTTxn& txn);
    };
}



#endif //TAAS_MOT_H
