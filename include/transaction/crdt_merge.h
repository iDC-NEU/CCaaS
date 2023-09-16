//
// Created by 周慰星 on 11/8/22.
//

#ifndef TAAS_CRDT_MERGE_H
#define TAAS_CRDT_MERGE_H

#pragma once

#include "tools/context.h"
#include "proto/transaction.pb.h"

namespace Taas{

class CRDTMerge{
public:
    static Context ctx;
    static bool ValidateReadSet(std::shared_ptr<proto::Transaction> txn_ptr);
    static bool ValidateWriteSet(std::shared_ptr<proto::Transaction> txn_ptr);
    static bool MultiMasterCRDTMerge(std::shared_ptr<proto::Transaction> txn_ptr);
    static bool Commit(std::shared_ptr<proto::Transaction> txn_ptr);

};

}



#endif //TAAS_CRDT_MERGE_H
