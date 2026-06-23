#pragma once
#include "Task.h"
#include "TaskDeque.h"
#include "MPSCQueue.h"
#include "../include/blockingconcurrentqueue.h"
#include "Arena.h"
#include "FiberPool.h"
#include <atomic>
#include <array>
namespace T_Threads {
    struct QTraits : moodycamel::ConcurrentQueueDefaultTraits {
        static constexpr size_t BLOCK_SIZE = 32768;
        static constexpr size_t IMPLICIT_INITIAL_INDEX_SIZE = 1024;
    };

    namespace SharedQueues {
        inline std::unique_ptr<FiberPool> fiberPool;
        inline ArenaPool taskArena{ 10 * 1024 * 1024 }; // 10 MB arena for tasks, adjust as needed
        inline std::atomic<int> runningTasks{ 0 };
        inline std::vector<std::unique_ptr<std::atomic<bool>>> immediateCoresInUse;
        inline std::atomic<bool> paused{ false };
        inline MPSCQueue<Task*> graveyard;
        inline std::vector<std::unique_ptr<TaskDeque>> threadQs;
        inline std::vector<std::unique_ptr<MPSCQueue<Task*>>> inboxes;
        inline std::array<moodycamel::ConcurrentQueue<Task*, QTraits>, 5> proirityQ;
    }
}