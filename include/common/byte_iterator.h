//
// Created by zwx on 23-9-5.
//

//
// Created by peng on 10/18/22.
//

#pragma once

#include "generator/uniform_long_generator.h"
#include "gtl/phmap.hpp"
#include <string>

namespace utils {

    template<class K, class V, class Mutex=gtl::NullMutex, size_t N=4>
    using MyFlatHashMap = gtl::parallel_flat_hash_map<K, V,
        gtl::priv::hash_default_hash<K>,
        gtl::priv::hash_default_eq<K>,
        gtl::priv::Allocator<gtl::priv::Pair<const K, V>>,
        N, Mutex>;

    using ByteIterator = std::string;
    using ByteIteratorMap = MyFlatHashMap<std::string, ByteIterator>;

    inline void RandomString(auto& container, int length) {
        if ((int)container.capacity() < length) {
            LOG(ERROR) << "RandomString container is too small!";
            length = container.capacity();
        }
        container.resize(length);
        static const auto alpha = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        static const auto len = 63;
        auto ul = UniformLongGenerator(0, len - 1);
        for (int i=0; i<length; i++) {
            container[i] = alpha[ul.nextValue()];
        }
    };

    inline std::string RandomString(int length) {
        std::string result;
        result.resize(length);
        RandomString(result, length);
        return result;
    };
}

