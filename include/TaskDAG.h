// TaskDAG.h  -- requires Task and TaskScheduler forward declarations
#pragma once
#include <vector>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include "Task.h"
#include "TaskScheduler.h"
#include "TaskNode.h"
#include "Epochs.h"
#include "TaskAllocator.h"

static constexpr uint8_t NONE = 255;

namespace T_Threads {

    class TaskDAG {
    public:
        TaskDAG(TaskScheduler& sched) : scheduler(sched) {};
        TaskNode* CreateNode(Task* t, uint8_t priority = NONE, uint8_t cpu_id = NONE);
         void AddDependency(TaskNode* dependent, TaskNode* dependency);

        void SubmitIfReady(TaskNode* node);

        void OnTaskFinished(TaskNode* node);
        void EndFrame();
    private:
        TaskScheduler& scheduler;

        void SubmitToScheduler(TaskNode* node);
    };
};