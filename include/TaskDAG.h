// TaskDAG.h  -- requires Task and TaskScheduler forward declarations
#pragma once
#include <vector>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include "Task.h"
#include "TaskScheduler.h"
static constexpr uint8_t NONE = 255;

namespace T_Threads {
    struct TaskNode {
        Task* task;
        std::vector<TaskNode*> dependents;
        std::atomic<int> dependencies_left;
        std::atomic<bool> submitted{ false };
        uint8_t cpu_id = 0;
        uint8_t priority = 0;
        bool is_local = true;
        bool is_fork = false;
        TaskNode(Task* t) : task(t), dependencies_left(0) {}
    };
    class TaskDAG {
    public:
        TaskDAG(TaskScheduler& sched) : scheduler_(sched) {};
        TaskNode* createNode(Task* t, uint8_t priority = NONE, uint8_t cpu_id = NONE);
        void Clear();
        void AddDependency(TaskNode* dependent, TaskNode* dependency);

        void SubmitIfReady(TaskNode* node);

        void OnTaskFinished(TaskNode* node);

    private:
        TaskScheduler& scheduler_;
        std::mutex graph_mutex_;
        std::mutex pool_mutex_; // Protects node_pool
        std::vector<std::unique_ptr<TaskNode>> node_pool;

        void SubmitToScheduler(TaskNode* node);
    };
};