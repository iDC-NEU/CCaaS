//
// Created by 周慰星 on 11/8/22.
//

#ifndef TAAS_CRDT_MERGE_H
#define TAAS_CRDT_MERGE_H

#pragma once

#include "proto/transaction.pb.h"

namespace Taas{

class Context;

class CRDTMerge{
public:
    static bool ValidateReadSet(const Context& ctx, std::shared_ptr<proto::Transaction> txn_ptr);
    static bool ValidateWriteSet(const Context& ctx, std::shared_ptr<proto::Transaction> txn_ptr);
    static bool MultiMasterCRDTMerge(const Context& ctx, std::shared_ptr<proto::Transaction> txn_ptr);
    static bool Commit(const Context& ctx, std::shared_ptr<proto::Transaction> txn_ptr);

};

}



#endif //TAAS_CRDT_MERGE_H
