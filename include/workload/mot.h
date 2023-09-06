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
        static void InsertData(uint64_t&  tid);
        static void RunTxn(uint64_t& tid);
        static void CloseDB();
    };
}



#endif //TAAS_MOT_H
