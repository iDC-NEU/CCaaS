//
// Created by zwx on 23-8-24.
//
#include "tools/utilities.h"
#include "workload/multi_model_workload.h"

#include <fstream>
#include <iostream>


namespace workload {

    NebulaTxn GenerateGQLTxn(std::ifstream *infile) {
        NebulaTxn res;
        getline(*infile, res);
        if ((*infile).eof()) return "";
        return res;
    }

    void GenerateGQLFile(uint64_t tid, std::fstream *outfile) {
        uint64_t randnum = Taas::RandomNumber(0, 100);
        if (randnum < MultiModelWorkload::ctx.kWriteNum) {
            std::string name = Taas::RandomString(20);
            int age = Taas::RandomNumber(10, 90);
            *outfile << "INSERT VERTEX IF NOT EXISTS person(name,age,tid) VALUES \"vid" << tid << "\" :(\"" << name
                     << "\"," << age << ",\"vid" << tid << "\");" << std::endl;
        } else {
            uint64_t number = Taas::RandomNumber(0, MultiModelWorkload::GetTxnId());
            *outfile << "FETCH PROP ON person \"vid" << number << "\" YIELD properties(VERTEX);" << std::endl;
        }
    }
}