//
// Created by zwx on 23-8-30.
//

//
// Created by peng on 10/18/22.
//

#pragma once

#include "generator/generator.h"
#include "glog/logging.h"
#include <memory>

namespace utils {
    class RandomUINT64 : public NumberGenerator {
    public:
        static auto NewRandomUINT64() {
            return std::make_unique<RandomUINT64>();
        }

        // [lb, ub]
        explicit RandomUINT64(uint64_t lb=0, uint64_t ub=std::numeric_limits<uint64_t>::max())
                : uniform(lb, ub) { }

        uint64_t nextValue() override {
            return uniform(*::utils::GetThreadLocalRandomGenerator());
        }

        double mean() override {
            CHECK(false) << "@todo implement ZipfianGenerator.mean()";
            return -1;
        }
    private:
        std::uniform_int_distribution<uint64_t> uniform;
    };
}

