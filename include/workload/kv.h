//
// Created by zwx on 23-8-23.
//

#ifndef KVPair_H
#define KVPair_H

#pragma once

#ifdef _DEBUG
#include <stdio.h>
	#define xPrintf(...) printf(__VA_ARGS__)
#else
#define xPrintf(...)
#endif

#include <memory>
#include <utility>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <unistd.h>


namespace workload {

    typedef struct KeyValue{
        std::string key;
        std::string value;

        KeyValue() {}

        KeyValue(KeyValue &&rhs) noexcept: key(std::move(rhs.key)), value(std::move(rhs.value)) {}

        KeyValue(const KeyValue &rhs) : key(rhs.key), value(rhs.value) {}

        explicit KeyValue(std::pair<std::string, std::string> kv)
                : key(std::move(kv.first)), value(std::move(kv.second)) {}

        void clear() {
            key.clear();
            value.clear();
        }

        void __clear() {
            clear();
        }

        auto &operator=(const KeyValue &rhs) {
            this->key = rhs.key;
            this->value = rhs.value;
            return *this;
        }

        auto &operator=(KeyValue &&rhs) {
            this->key = std::move(rhs.key);
            this->value = std::move(rhs.value);
            return *this;
        }

        bool operator==(const KeyValue &rhs) const {
            if (key != rhs.key) {
                return false;
            }
            return value == rhs.value;
        }

        bool operator<(const KeyValue &rhs) const {
            if (key != rhs.key) {
                return key < rhs.key;
            }
            return value < rhs.value;
        }
    }KeyValue;


}
#endif //KVPair_H
