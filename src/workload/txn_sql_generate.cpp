//
// Created by zwx on 23-8-24.
//

#include "tools/utilities.h"
#include "workload/multi_model_workload.h"
#include <queue>
#include <fstream>
#include <iostream>


namespace workload {

    MotTxn GenerateSQLTxn(std::ifstream *infile) {
        MotTxn res;
        uint64_t num = MultiModelWorkload::ctx.kPerOpNum;
        while (num--) {
            std::string temp;
            getline(*infile, temp);
            if ((*infile).eof()) return res;
            res.push_back(temp);
        }
        return res;
    }


    //INSERT INTO taas_odbc VALUES(25,'li','tid1');
    //SELECT * FROM taas_odbc WHERE c_tid = 'tid1';
    void GenerateSQLTxnFile(uint64_t tid, std::fstream *outfile) {
        uint64_t num = MultiModelWorkload::ctx.kPerOpNum;
        uint64_t randnum = Taas::RandomNumber(0, 100);
        if (randnum < MultiModelWorkload::ctx.kWriteNum) {
            while (num--) {
                std::string name = Taas::RandomString(20);
                int now = tid*MultiModelWorkload::ctx.kPerOpNum + num;
                *outfile << "INSERT INTO taas_odbc VALUES(" << now << ", '" << name
                         << "','tid" << tid << "');" << std::endl;
            }
        } else {
            while (num--) {
                uint64_t number = Taas::RandomNumber(1, MultiModelWorkload::GetTxnId()*MultiModelWorkload::ctx.kPerOpNum);
                *outfile << "SELECT * FROM taas_odbc WHERE c_tid = 'tid" << number << "';" << std::endl;
            }
        }
    }
}