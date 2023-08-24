//
// Created by zwx on 23-8-24.
//

#include "tools/utilities.h"
#include "workload/multi_model_workload.h"

#include <queue>
#include <fstream>
#include <iostream>


namespace workload {

    KVTxn GenerateKVTxn(std::ifstream *infile) {
        KVTxn res;
        uint64_t num = MultiModelWorkload::ctx.kPerOpNum;
        num += 1;
        while (num--) {
            std::string str;
            *infile >> str;
            if ((*infile).eof()) return res;
            std::string key, value;
            if (str.find(":") != str.npos) {
                char *temp = (char *) str.data();
                key = strtok(temp, ":");
                value = strtok(NULL, ":");
            } else {
                char *temp = (char *) str.data();
                key = temp;
                value = "";
            }
            KeyValue kv({key, value});
            res.push_back(kv);
        }
        return res;
    }

    void GenerateKVTxnFile(uint64_t tid, std::fstream *outfile) {
        uint64_t num = MultiModelWorkload::ctx.kPerOpNum;
        uint64_t randnum = Taas::RandomNumber(0, 100);
        if (randnum < MultiModelWorkload::ctx.kWriteNum) {
            while (num--) {
                std::string key = Taas::RandomString(20);
                std::string value = Taas::RandomString(20);
                *outfile << key << ":" << value << std::endl;
            }
        } else {
            while (num--) {
                std::string key = Taas::RandomString(20);
                *outfile << key << std::endl;
            }
        }
        *outfile << "kvid" << tid << ":" << "kvid" << tid << std::endl;
    }
}