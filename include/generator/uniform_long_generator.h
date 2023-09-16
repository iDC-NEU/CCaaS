//
// Created by zwx on 23-8-30.
//
//
// Created by peng on 10/18/22.
//

#pragma once

#include "generator.h"
#include "common/random_uint64.h"

namespace utils {
    /**
     * Creates a generator that will return longs uniformly randomly from the
     * interval [lb,ub] inclusive (that is, lb and ub are possible values)
     * (lb and ub are possible values).
     *
     * @param lb the lower bound (inclusive) of generated values
     * @param ub the upper bound (inclusive) of generated values
     */
    class UniformLongGenerator : public NumberGenerator {
    public:
        static auto NewUniformLongGenerator(uint64_t lb, uint64_t ub) {
            return std::make_unique<UniformLongGenerator>(lb, ub);
        }

        explicit UniformLongGenerator(uint64_t lb, uint64_t ub)
                : generator(lb, ub) {
            m = double(lb + ub) / 2.0;
        }
        uint64_t nextValue() override {
            return generator.nextValue();
        }
        double mean() override {
            return m;
        }
    private:
        double m;
        utils::RandomUINT64 generator;
    };
}

