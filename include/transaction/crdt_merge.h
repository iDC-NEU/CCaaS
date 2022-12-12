//
// Created by 周慰星 on 11/8/22.
//

#ifndef TAAS_CRDT_MERGE_H
#define TAAS_CRDT_MERGE_H

#include "proto/transaction.pb.h"

namespace Taas{

class Context;

class CRDTMerge{
public:
    static bool ValidateReadSet(Context& ctx, proto::Transaction& txn);
    static bool ValidateWriteSet(Context& ctx, proto::Transaction& txn);
    static bool LocalCRDTMerge(Context& ctx, proto::Transaction& txn);
    static bool MultiMasterCRDTMerge(Context& ctx, proto::Transaction& txn);
    static bool Commit(Context& ctx, proto::Transaction& txn);
    static void RedoLog(Context& ctx, proto::Transaction& txn);

};

}



#endif //TAAS_CRDT_MERGE_H
