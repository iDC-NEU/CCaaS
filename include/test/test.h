//
// Created by 周慰星 on 2022/11/9.
//

#ifndef TAAS_TEST_H
#define TAAS_TEST_H

#pragma once

#include "tools/context.h"
#include "tools/utilities.h"
#include "epoch/epoch_manager.h"

#include <cstdlib>

namespace Taas {
    void Client(const Context& ctx, uint64_t id);
    void LevelDBClient(const Context& ctx, uint64_t id);
}
#endif //TAAS_TEST_H
