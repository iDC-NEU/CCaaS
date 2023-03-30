//
// Created by 周慰星 on 11/8/22.
//

#ifndef TAAS_ATOMIC_COUNTERS_H
#define TAAS_ATOMIC_COUNTERS_H
#include <vector>
#include <atomic>
#include <memory>

namespace Taas {
    /**
     * @brief 原子计数器，对std::atomic<uint64_t>的简单封装
     *
     */
    class AtomicCounters {
    private:
        std::vector<std::unique_ptr<std::atomic<uint64_t> > > vec;
        uint64_t _size;
    public:

        AtomicCounters() {}

        AtomicCounters(uint64_t size = 8);

        void Init(uint64_t size = 8);

        uint64_t IncCount(uint64_t index, uint64_t value) {
            return vec[index % _size]->fetch_add(value);
        }

        uint64_t DecCount(uint64_t index, uint64_t value) {
            return vec[index % _size]->fetch_sub(value);
        }

        void SetCount(uint64_t value) {
            for (auto &i: vec) {
                i->store(value);
            }
        }

        void SetCount(uint64_t index, uint64_t value) {
            vec[index % _size]->store(value);
        }

        uint64_t GetCount() {
            uint64_t ans = 0;
            for (auto &i: vec) {
                ans += i->load();
            }
            return ans;
        }

        uint64_t GetCount(uint64_t index) {
            return vec[index % _size]->load();
        }

        void Clear(uint64_t value = 0) {
            (void) value;
            for (auto &i: vec) {
                i->store(0);
            }
        }

        void Resize(uint64_t size) {
            if (size <= _size) return;
            vec.reserve(size);
            for (uint64_t i = _size; i < size; i++) {
                vec.emplace_back(std::make_unique<std::atomic<uint64_t>>(0));
            }
        }
    };

    class AtomicCounters_Cache {
    private:
        std::vector<std::unique_ptr<std::vector<std::unique_ptr<std::atomic<uint64_t>>>>> vec;
        uint64_t _size, epoch_mod{}, _length;
    public:

        AtomicCounters_Cache() {}

        AtomicCounters_Cache(uint64_t length = 1000, uint64_t size = 8);

//        void Init(uint64_t length = 1000, uint64_t size = 8);
        void Init(uint64_t length = 1000, uint64_t size = 8, uint64_t value = 0);

        uint64_t IncCount(uint64_t epoch, uint64_t index, uint64_t value) {
            return (*vec[epoch % _length])[index % _size]->fetch_add(value);
        }

        uint64_t DecCount(uint64_t epoch, uint64_t index, uint64_t value) {
            return (*vec[epoch % _length])[index % _size]->fetch_sub(value);
        }

        void SetCount(uint64_t epoch, uint64_t index, uint64_t value) {
            (*vec[epoch % _length])[index % _size]->store(value);
        }

        void SetCount(uint64_t epoch, uint64_t value) {
            auto &v = (*vec[epoch % _length]);
            for (auto &i: v) {
                i->store(value);
            }
        }

        uint64_t GetCount(uint64_t epoch, uint64_t index) {
            return (*vec[epoch % _length])[index % _size]->load();
        }

        uint64_t GetCount(uint64_t epoch) {
            auto &v = (*vec[epoch % _length]);
            uint64_t ans = 0;
            for (auto &i: v) {
                ans += i->load();
            }
            return ans;
        }

        void Clear(uint64_t epoch, uint64_t value = 0) {
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

}// namespace Taas
#endif //TAAS_ATOMIC_COUNTERS_H
