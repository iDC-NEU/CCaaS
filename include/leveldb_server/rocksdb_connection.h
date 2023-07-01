//
// Created by peng on 2/20/23.
// Created by zwx on 23-6-30.
//


#ifndef TAAS_ROCKSDBCONNECTION_H
#define TAAS_ROCKSDBCONNECTION_H

#pragma once

#include "rocksdb/db.h"
#include "glog/logging.h"
#include "tools/utilities.h"

#include <memory>

namespace Taas {
    class RocksDBConnection {
    public:
        RocksDBConnection(const RocksDBConnection&) = delete;

        RocksDBConnection(RocksDBConnection&&) = delete;

        virtual ~RocksDBConnection() = default;

        // create a new db connection
        static std::unique_ptr<RocksDBConnection> NewConnection(const std::string& dbName) {
            auto t1 = now_to_us();
            rocksdb::Options options;
            options.create_if_missing = true;
            rocksdb::DB* db;
            rocksdb::Status status = rocksdb::DB::Open(options, dbName, &db);
            if (!status.ok()) {
                LOG(WARNING) << "Can not open database: " << dbName;
                return nullptr;
            }
            std::unique_ptr<RocksDBConnection> dbc(new RocksDBConnection());
            dbc->dbName = dbName;
            dbc->db.reset(db);
            auto t2 = now_to_us();
            printf("connect to leveldb cost %lu \n", t2 - t1);
            return dbc;
        }

        [[nodiscard]] const std::string& getDBName() const { return dbName; }

        bool asyncPut(std::string_view key, std::string_view value) {
            auto status = db->Put(asyncWrite,
                                  rocksdb::Slice{key.data(), key.size()},
                                  rocksdb::Slice{value.data(), value.size()});
            return status.ok();
        }

        bool syncPut(std::string_view key, std::string_view value) {
            auto status = db->Put(syncWrite,
                                  rocksdb::Slice{key.data(), key.size()},
                                  rocksdb::Slice{value.data(), value.size()});
            return status.ok();
        }

        // It is not an error if "key" did not exist in the database.
        bool asyncDelete(std::string_view key) {
            auto status = db->Delete(asyncWrite, rocksdb::Slice{key.data(), key.size()});
            return status.ok();
        }

        // It is not an error if "key" did not exist in the database.
        bool syncDelete(std::string_view key) {
            auto status = db->Delete(syncWrite, rocksdb::Slice{key.data(), key.size()});
            return status.ok();
        }

        // batch.Put(key, value);
        // batch.Delete(key);
        bool syncWriteBatch(const std::function<bool(rocksdb::WriteBatch* batch)>& callback) {
            rocksdb::WriteBatch batch;
            if (!callback(&batch)) {
                return false;
            }
            auto status = db->Write(syncWrite, &batch);
            return status.ok();
        }

        bool get(std::string_view key, std::string* value) const {
            rocksdb::Status status = db->Get(rocksdb::ReadOptions(), rocksdb::Slice{key.data(), key.size()}, value);
            return status.ok();
        }

    protected:
        RocksDBConnection() {
            syncWrite.sync = true;
            asyncWrite.sync= false;
        }

    private:
        rocksdb::WriteOptions syncWrite;
        rocksdb::WriteOptions asyncWrite;
        std::string dbName;
        std::unique_ptr<rocksdb::DB> db;
    };
}
#endif //TAAS_ROCKSDBCONNECTION_H
