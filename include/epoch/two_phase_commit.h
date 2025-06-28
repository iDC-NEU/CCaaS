//
// Created by user on 23-11-9.
//

#ifndef TAAS_TWO_PHASE_COMMIT_H
#define TAAS_TWO_PHASE_COMMIT_H
#pragma once

#include "tools/context.h"

namespace Taas {
    class TwoPhaseCommitManager {
    public:
        static void TwoPhaseCommitManagerThreadMain();
    };
}
#endif //TAAS_TWO_PHASE_COMMIT_H
