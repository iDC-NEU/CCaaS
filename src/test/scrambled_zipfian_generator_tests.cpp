//
// Created by zwx on 23-8-30.
//
//
// Created by peng on 11/7/22.
//

#include "generator/acknowledge_counter_generator.h"
#include "generator/scrambled_zipfian_generator.h"
#include "generator/uniform_long_generator.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include <queue>

class AcknowledgeCounterGeneratorTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

    template<class T>
    static auto TestCount(T& sz, uint64_t count) {
        std::unordered_map<uint64_t, int> countVec;
        for (uint64_t i = 0; i < count; i++) {
            auto rnd = sz.nextValue();
            countVec[rnd]++; // offset
        }
        return countVec;
    }

};

// Test that advancing past {@link Integer#MAX_VALUE} works.
TEST_F(AcknowledgeCounterGeneratorTest, AcknowledgeCounterGeneratorCorrectness) {
    /** The size of the window of pending id ack's. 2^20 = {@value} */
    static const int WINDOW_SIZE = 1 << 20;

    const auto toTry = WINDOW_SIZE * 3;

    utils::AcknowledgedCounterGenerator generator(INT64_MAX - 1000);

    std::queue<uint64_t> q;

    for (auto i = 0; i < toTry; ++i) {
        auto value = generator.nextValue();
        q.push(value);
        while (q.size() >= 1000) {
            auto first = q.front();
            q.pop();
            // Don't always advance by one.
            if (util::Timer::time_now_ns()%2 == 0) {
                generator.acknowledge((int)first);
            } else {
                auto second = q.front();
                q.pop();
                q.push(first);
                generator.acknowledge((int)second);
            }
        }
    }
}


class ScrambledZipfianGeneratorTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

    template<class T>
    static auto TestCount(T& sz, uint64_t count) {
        std::unordered_map<uint64_t, int> countVec;
        for (uint64_t i = 0; i < count; i++) {
            auto rnd = sz.nextValue();
            countVec[rnd]++; // offset
        }
        return countVec;
    }

};

TEST_F(ScrambledZipfianGeneratorTest, ScrambledZipfianCorrectness) {
    auto target = {848536, 1166912, 793859, 859141, 993748, 949876, 864642, 876079, 932109, 792692, 922406};
    int min=0, max=10, count=10000000;
    auto sz = utils::ScrambledZipfianGenerator::NewScrambledZipfianGenerator(min, max);
    auto mapping = TestCount(*sz, count);
    EXPECT_TRUE(mapping.size() == 11);
    for (uint64_t i = 0; i <= 10; i++) {
        EXPECT_TRUE(mapping[i] < target.begin()[i]*1.05) << "distribution test failed!";
        EXPECT_TRUE(mapping[i] > target.begin()[i]*0.95) << "distribution test failed!";
    }
}









class ZipfianGeneratorTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

    static void TestOutOfRange(utils::ZipfianGenerator& z, uint64_t min, uint64_t max, uint64_t count) {
        for (uint64_t i = 0; i < count; i++) {
            auto rnd = z.nextValue();
            EXPECT_TRUE(min <= rnd);
            EXPECT_TRUE(max >= rnd);
        }
    }

    static auto TestCount(utils::ZipfianGenerator& z, uint64_t min, uint64_t max, uint64_t count) {
        std::vector<int> countVec(max-min+1);
        for (uint64_t i = 0; i < count; i++) {
            auto rnd = z.nextValue();
            countVec[rnd-min]++; // offset
        }
        return countVec;
    }
    static void TestUniformDistribution() {
        int min=0, max=10, count=10000000;
        utils::ZipfianGenerator z(min, max, 0);
        auto vec = TestCount(z, min, max, count);
        for (uint64_t i = 0; i < vec.size(); i++) {
            EXPECT_TRUE(vec[i] < (double)count/vec.size()*1.05) << "distribution test failed!";
            EXPECT_TRUE(vec[i] > (double)count/vec.size()*0.95) << "distribution test failed!";
        }
    }
    // the zipf distribution tested from official ycsb
    static std::initializer_list<int> YCSB_ZIPF99_0_10_1E7;
    static std::initializer_list<int> YCSB_ZIPF99_0_1E6_1E7;
};

TEST_F(ZipfianGeneratorTest, OutOfRange) {
    int min=0, max=10, count=1000;

    auto z = utils::ZipfianGenerator::NewZipfianGenerator(min, max);
    TestOutOfRange(*z, min, max, count);
}

TEST_F(ZipfianGeneratorTest, ZipfianCorrectness) {
    int min=0, max=10, count=10000000;
    auto z = utils::ZipfianGenerator::NewZipfianGenerator(min, max);
    auto vec = TestCount(*z, min, max, count);
    EXPECT_TRUE(vec.size() == 11);
    for (uint64_t i = 0; i < 11; i++) {
        EXPECT_TRUE(vec[i] < ZipfianGeneratorTest::YCSB_ZIPF99_0_10_1E7.begin()[i]*1.05) << "distribution test failed!";
        EXPECT_TRUE(vec[i] > ZipfianGeneratorTest::YCSB_ZIPF99_0_10_1E7.begin()[i]*0.95) << "distribution test failed!";
    }
}

TEST_F(ZipfianGeneratorTest, ZipfianCorrectness2) {
    int min=0, max=1000000, count=10000000;
    auto z = utils::ZipfianGenerator::NewZipfianGenerator(min, max);
    auto vec = TestCount(*z, min, max, count);
    EXPECT_TRUE(vec.size() > 11);
    for (uint64_t i = 0; i < 11; i++) {
        EXPECT_TRUE(vec[i] < ZipfianGeneratorTest::YCSB_ZIPF99_0_1E6_1E7.begin()[i]*1.05) << "distribution test failed!";
        EXPECT_TRUE(vec[i] > ZipfianGeneratorTest::YCSB_ZIPF99_0_1E6_1E7.begin()[i]*0.95) << "distribution test failed!";
    }
}

TEST_F(ZipfianGeneratorTest, UniformDistribution) {
    TestUniformDistribution();
}

TEST_F(ZipfianGeneratorTest, ConcurrentAccess) {
    std::thread t1(&ZipfianGeneratorTest::TestUniformDistribution);
    std::thread t2(&ZipfianGeneratorTest::TestUniformDistribution);
    std::thread t3(&ZipfianGeneratorTest::TestUniformDistribution);
    t1.join();
    t2.join();
    t3.join();
}

TEST_F(ZipfianGeneratorTest, TestZetaStaticCalculation) {
    auto newZetan = utils::ZipfianGenerator::ZetaStatic(0, 100000000, utils::ZipfianGenerator::ZIPFIAN_CONSTANT, 0);
    EXPECT_TRUE(std::abs(20.80293049002014 - newZetan) < 0.0000000001);
}

std::initializer_list<int> ZipfianGeneratorTest::YCSB_ZIPF99_0_10_1E7 = {3280393, 1649922, 1198018, 852880, 664195, 542254, 460741, 397192, 352452, 315486, 285257};

std::initializer_list<int> ZipfianGeneratorTest::YCSB_ZIPF99_0_1E6_1E7 = {648903, 327593, 261379, 186162, 144515, 118535, 100134, 87280, 77253, 68863, 62118};



class UniformLongGeneratorTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

    template<class T>
    static auto TestCount(T& sz, uint64_t count) {
        std::unordered_map<uint64_t, int> countVec;
        for (uint64_t i = 0; i < count; i++) {
            auto rnd = sz.nextValue();
            countVec[rnd]++; // offset
        }
        return countVec;
    }

};

TEST_F(UniformLongGeneratorTest, UniformLongCorrectness) {
    int min=10, max=100, count=10000000;
    auto sz = utils::UniformLongGenerator::NewUniformLongGenerator(min, max);
    auto mapping = TestCount(*sz, count);
    const int size = (int)mapping.size();
    EXPECT_TRUE(size == max-min+1);
    for (int i = min; i <= max; i++) {
        EXPECT_TRUE(mapping[i] < (double)count/size*1.05) << "distribution test failed!";
        EXPECT_TRUE(mapping[i] > (double)count/size*0.95) << "distribution test failed!";
    }
}
