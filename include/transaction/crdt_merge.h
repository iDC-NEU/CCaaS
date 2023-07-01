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
    static bool ValidateReadSet(const Context& ctx, proto::Transaction& txn);
    static bool ValidateWriteSet(const Context& ctx, proto::Transaction& txn);
    static bool MultiMasterCRDTMerge(const Context& ctx, proto::Transaction& txn);
    static bool Commit(const Context& ctx, proto::Transaction& txn);

};

}



#endif //TAAS_CRDT_MERGE_H
