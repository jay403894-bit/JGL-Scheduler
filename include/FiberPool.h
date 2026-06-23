#pragma once
#include "Stack.h"
#include "Fiber.h"
#include "Epochs.h"
#include "FiberStackArena.h"
#include <vector>
#include "platform.h"
namespace T_Threads {
    class Scheduler; // Just tell the compiler this exists

    class FiberPool {
        FiberStackArena arena;
        Stack<Fiber*> freeStack;
        std::vector<Fiber> allFibers; // To keep them alive

    public:
        FiberPool(int count);

        Fiber* Acquire();

        void Release(Fiber* f);
        static void SwitchBackToScheduler();
        static void FiberEntryWrapper();
    };
};