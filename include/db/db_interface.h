//
// Created by user on 23-7-13.
//

#pragma once

#include "leveldb_connection.h"
#include "phmap_connection.h"
#include "rocksdb_connection.h"

namespace Taas {
    using DBConnection = PHMapConnection;

    template<class T>
    concept db_like = requires(T t,
            DBConnection::WriteBatch b,
            const std::function<bool(DBConnection::WriteBatch*)>& callback,
            std::string* getValue) {
        b.Put("key", "value");
        T::NewConnection("dbName");
        t.syncWriteBatch(callback);
        t.syncPut("key", "value");
        *getValue = t.getDBName();
        t.get("key", getValue);
    };
    static_assert(db_like<DBConnection>);

    inline bool IsDBHashMap() { return std::is_same<DBConnection, PHMapConnection>::value; }
}