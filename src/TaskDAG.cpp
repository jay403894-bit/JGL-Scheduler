#include "../include/TaskDAG.h"
using namespace T_Threads;

TaskNode* TaskDAG::createNode(Task* t, uint8_t priority, uint8_t cpu_id) {
	std::lock_guard<std::mutex> lock(pool_mutex_);
    auto node = std::make_unique<TaskNode>(t);
    if (priority != NONE)
    {
        node->is_local = false;
        node->priority = priority;
    }
    if (cpu_id != NONE)
    {
        node->cpu_id = cpu_id;
    }
    node_pool.push_back(std::move(node));
    return node_pool.back().get();
}
void TaskDAG::Clear() {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    node_pool.clear();
}
void TaskDAG::AddDependency(TaskNode* dependent, TaskNode* dependency) {
    std::lock_guard<std::mutex> lock(graph_mutex_);

    dependent->dependencies_left.fetch_add(1, std::memory_order_relaxed);
    dependency->dependents.push_back(dependent);
}

void TaskDAG::SubmitIfReady(TaskNode* node) {
    if (node->dependencies_left.load(std::memory_order_acquire) == 0) {
        SubmitToScheduler(node);
    }
}
void TaskDAG::OnTaskFinished(TaskNode* node) {

    for (TaskNode* dep : node->dependents) {
        int val = dep->dependencies_left.fetch_sub(1, std::memory_order_acq_rel) - 1; // -1 to get the result AFTER sub

        if (val == 0) {
            SubmitToScheduler(dep);
        }
    }
}

void TaskDAG::SubmitToScheduler(TaskNode* node) {

    if (node->submitted.exchange(true, std::memory_order_acq_rel)) {
        return; // Already submitted by another thread, do nothing!
    }
    node->task->onComplete = [node, this]() {
        this->OnTaskFinished(node);
        };

    if (node->is_fork) {
        scheduler_.SubmitFork(node->cpu_id, node->task);
    }
    else if (node->is_local) {
        if (node->cpu_id == 0)
            scheduler_.SubmitLocal(node->task);
        else
            scheduler_.SubmitLocal(node->cpu_id, node->task);
    }
    else {
        if (node->priority == 0)
            scheduler_.SubmitPQ(node->task);
        else
            scheduler_.SubmitPQ(node->priority, node->task);
    }
}