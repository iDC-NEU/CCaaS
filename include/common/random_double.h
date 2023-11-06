//
// Created by zwx on 23-8-30.
//
//
// Created by peng on 10/17/22.
//

#pragma once

#include "generator/generator.h"
#include "glog/logging.h"
#include <memory>

namespace utils {
    class RandomDouble : public utils::DoubleGenerator {
    public:
        static auto NewRandomDouble() {
            return std::make_unique<RandomDouble>();
        }

        explicit RandomDouble(double min = 0.0, double max = 1.0)
                : uniform(min, max) { }

        double nextValue() override {
            return uniform(*::utils::GetThreadLocalRandomGenerator());
        }

        double mean() override {
            CHECK(false) << "@todo implement ZipfianGenerator.mean()";
            return -1;
        }

    private:
        std::uniform_real_distribution<double> uniform;
    };
}
