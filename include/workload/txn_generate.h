//
// Created by user on 23-8-24.
//

#ifndef TAAS_TXN_GENERATE_H
#define TAAS_TXN_GENERATE_H

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

#include "tools/utilities.h"
#include "workload/multi_model_workload.h"
#include <queue>
#include <fstream>
#include <iostream>

namespace workload {

    extern KVTxn GenerateKVTxn(std::ifstream *infile);
    extern MotTxn GenerateSQLTxn(std::ifstream *infile);
    extern NebulaTxn GenerateGQLTxn(std::ifstream *infile);

    extern void GenerateKVTxnFile(uint64_t tid, std::fstream *outfile);
    extern void GenerateSQLTxnFile(uint64_t tid, std::fstream *outfile);
    extern void GenerateGQLFile(uint64_t tid, std::fstream *outfile);
}


#endif //TAAS_TXN_GENERATE_H
