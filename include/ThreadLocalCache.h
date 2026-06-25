#pragma once
#include <vector>
#include "Fiber.h"
#include "GlobalFiberPool.h"

namespace T_Threads {
    //class GlobalFiberPool;  // forward declaration

    struct ThreadLocalCache {
        std::vector<Fiber*> localFibers;
        GlobalFiberPool* globalPool = nullptr;
        static constexpr size_t MAX_CAPACITY = 64;

        // Set the global pool this cache refills from
        void SetGlobalPool(GlobalFiberPool* pool) {
            globalPool = pool;
        }

        void Push(Fiber* f) {
            if (localFibers.size() < MAX_CAPACITY) {
                localFibers.push_back(f);
            }
            else if (globalPool) {
                std::vector<Fiber*> batchToReturn;
                size_t half = localFibers.size() / 2;
                batchToReturn.insert(batchToReturn.end(), localFibers.begin(), localFibers.begin() + half);
                localFibers.erase(localFibers.begin(), localFibers.begin() + half);

                globalPool->ReturnBatch(batchToReturn);
                localFibers.push_back(f);
            }
        }

        Fiber* Pop() {
            if (localFibers.empty() && globalPool) {
                localFibers = globalPool->StealBatch(MAX_CAPACITY / 2);
            }
            if (localFibers.empty()) return nullptr;

            Fiber* f = localFibers.back();
            localFibers.pop_back();
            return f;
        }
    };
}