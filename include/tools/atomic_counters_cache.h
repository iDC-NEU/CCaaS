//
// Created by zwx on 24-3-14.
//

#ifndef TAAS_ATOMIC_COUNTERS_CACHE_H
#define TAAS_ATOMIC_COUNTERS_CACHE_H

#pragma once

#include <vector>
#include <atomic>
#include <memory>

namespace Taas {

    class AtomicCounters_Cache {
    private:
        std::vector<std::unique_ptr<std::vector<std::unique_ptr<std::atomic<uint64_t>>>>> vec;
        uint64_t _size{}, epoch_mod{}, _length{};
    public:

        AtomicCounters_Cache() = default;

        explicit AtomicCounters_Cache(uint64_t length = 1000, uint64_t size = 2);

        void Init(uint64_t length = 1000, uint64_t size = 2, uint64_t value = 0);

        uint64_t IncCount(const uint64_t &epoch, const uint64_t &index, const uint64_t &value) {
            return (*vec[epoch % _length])[index % _size]->fetch_add(value);
        }

        uint64_t DecCount(const uint64_t &epoch, const uint64_t &index, const uint64_t &value) {
            return (*vec[epoch % _length])[index % _size]->fetch_sub(value);
        }

        void SetCount(const uint64_t &epoch, const uint64_t &index, const uint64_t &value) {
            (*vec[epoch % _length])[index % _size]->store(value);
        }

        void SetCount(const uint64_t &epoch, const uint64_t &value) {
            auto &v = (*vec[epoch % _length]);
            for (auto &i: v) {
                i->store(value);
            }
        }

        uint64_t GetCount(const uint64_t &epoch, const uint64_t &index) {
            return (*vec[epoch % _length])[index % _size]->load();
        }

        uint64_t GetCount(const uint64_t &epoch) {
            auto &v = (*vec[epoch % _length]);
            uint64_t ans = 0;
            for (auto &i: v) {
                ans += i->load();
            }
            return ans;
        }

        void Clear(const uint64_t &epoch, const uint64_t &value = 0) {
            (void) value;
            auto &v = (*vec[epoch % _length]);
            for (auto &i: v) {
                i->store(value);
            }
        }

        void Clear() {
            for (auto &v : vec) {
                for (auto &i: *v) {
                    i->store(0);
                }
            }
        }

    };
}


#endif //TAAS_ATOMIC_COUNTERS_CACHE_H
