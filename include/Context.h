#pragma once
#include <stdint.h>

struct Context {
    void* rsp;      // 0
};

extern "C" void ContextSwitch(Context* from, Context* to);

