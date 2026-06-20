#include "../include/TaskScheduler.h"

using namespace T_Threads;

TaskScheduler::TaskScheduler() {
    StartPool();
}
TaskScheduler::~TaskScheduler() {
    if (!stop_flag_) {
        Join();
    }
}
bool TaskScheduler::EnqueueToMain(Task* task)
{
    if (!pool_active_) return false;
    if (!task)
        return false;
    main_queue_.push(task);
    return true;
}
void TaskScheduler::ProcessMainThread()
{
    if (!pool_active_) return;
    Task* t;
    while (main_queue_.pop(t)) {
        if (!t) continue;
        t->execute();
    }
}
void TaskScheduler::Join() {
    if (!pool_active_) return;
    stop_flag_ = true;
    NotifyAll();
    for (auto& worker : workers_) {
        worker->Join();
    }
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        workers_.clear();
        main_queue_.clear();

        SharedQueues::immediate_cores_in_use.clear();

    }
    pool_active_.store(false, std::memory_order_release);
}

void T_Threads::TaskScheduler::NotifyAll()
{
    for (int i = 0; i < workers_.size(); i++)
        workers_[i]->NotifyWorker();
}
void TaskScheduler::StartPool() {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    stop_flag_.store(false, std::memory_order_release);
    next_worker_ = 0;
    unsigned int hw = std::thread::hardware_concurrency();
    if (hw == 0) hw = 4;
    unsigned int num_workers = std::max(1u, hw - 1u);
    workers_.clear();
    SharedQueues::thread_queues_.clear();
    workers_.reserve(num_workers);
    SharedQueues::thread_queues_.reserve(num_workers);
    SharedQueues::immediate_cores_in_use.clear();
    SharedQueues::immediate_cores_in_use.reserve(num_workers);

    for (unsigned int i = 0; i < num_workers; ++i) {
        SharedQueues::immediate_cores_in_use.push_back(std::make_unique<std::atomic<bool>>(false));
        SharedQueues::thread_queues_.push_back(std::make_unique<TaskDeque>());
        SharedQueues::inboxes_.push_back(std::make_unique<MPSCQueue<Task*>>());

    }
    for (unsigned int i = 0; i < num_workers; ++i) {
        auto worker = std::make_shared<T_Thread>();
        worker->SetQueueIndex(i);
        workers_.push_back(worker);
        worker->StartWorker(i + 1);
    }
    for (int i = 0; i < workers_.size(); i++)
    {
        while (!workers_[i]->Ready()) {
            std::this_thread::yield();
        }
    }
    pool_active_.store(true, std::memory_order_release);
}
bool TaskScheduler::SubmitLocal(Task* task)
{
    return PushLocal(task);
}
bool TaskScheduler::SubmitLocal(uint8_t cpu_affinity, Task* task)
{
   return PushLocal(task, cpu_affinity);
}
bool TaskScheduler::SubmitPQ(Task* task)
{
   return Push(task);
}
bool TaskScheduler::SubmitPQ(uint8_t priority, Task* task)
{
   return Push(task, priority);
}
bool TaskScheduler::SubmitFork(uint8_t cpu_affinity, Task* task)
{
    if (!task)
        return false;
    return PushToCore(cpu_affinity, task);
}
void TaskScheduler::Pause() {
    SharedQueues::paused_.store(true, std::memory_order_release);
}
void TaskScheduler::Resume() {
    SharedQueues::paused_.store(false, std::memory_order_release);
    NotifyAll();
}
void TaskScheduler::Stop(Task* worker_task) {
    stop_flag_.store(true, std::memory_order_release);
    worker_task->stop();
}
void TaskScheduler::WaitAll(const std::vector<Task*>& tasks) {
    for (auto* t : tasks) {
        // A slightly better way to wait than just yield()
        while (!t->complete.load(std::memory_order_acquire)) {
            // This is "exponential backoff" - it reduces CPU usage while waiting
            std::this_thread::yield();
        }
    }
}

bool TaskScheduler::PushLocal(Task* task, uint8_t cpuaffinity) {
    if (!task)
        return false;

    size_t num_workers = workers_.size();

    // Bound check against the actual pool Worker size instead of raw hardware concurrency
    if (cpuaffinity > 0 && (size_t)(cpuaffinity - 1) < num_workers) {
        size_t idx = (size_t)(cpuaffinity - 1);

        if (!SharedQueues::immediate_cores_in_use[idx]->load(std::memory_order_acquire)) {
            SharedQueues::inboxes_[idx]->push(task);
            NotifyAll();
        }
        else
            return false;
    }
    else {
        uint8_t chosen = PickNextWorker();
        SharedQueues::inboxes_[chosen]->push(task);
        NotifyAll();
    }
    return true;
}
bool TaskScheduler::Push(Task* task, uint8_t priority)
{
    //simple guard if less than pin ot priority 0 if greater max at 5
    if (priority > 4)
        priority = 4;
 
    if (!task)
        return false;
    if (!SharedQueues::priority_queue_[priority].try_enqueue(task)) {
        //currently no overflow handling if you go over about 32 mil tasks backed up 
        //but 32 mil tasks is a lot 
    };
    NotifyAll();
    return true;
}
bool TaskScheduler::PushToCore(size_t core_id, Task* task)
{
    if (core_id < 1) return false;
    if (!pool_active_) return false;
    if (!task)
        return false;

    if (SharedQueues::immediate_cores_in_use[core_id % workers_.size()]->load(std::memory_order_acquire) || core_id < 1) {
        return false;
    }
    size_t idx = (core_id - 1) % workers_.size();
    SharedQueues::immediate_cores_in_use[idx]->store(true, std::memory_order_release);
    workers_[idx]->SetImmediateTask(task);
    workers_[idx]->NotifyWorker();
    return true;
}
int TaskScheduler::PickNextWorker() {
    size_t n = workers_.size();
    for (size_t i = 0; i < n; ++i) {
        size_t idx = (next_worker_ + i) % n;
        // Skip workers that are busy with immediate tasks
        if (!SharedQueues::immediate_cores_in_use[idx]->load(std::memory_order_acquire)) {
            next_worker_ = (idx + 1) % n; // advance round-robin pointer
            return static_cast<int>(idx);
        }
    }
    // If all cores are busy, just pick the next in round-robin
    int fallback = static_cast<int>(next_worker_);
    next_worker_ = (fallback + 1) % n;
    return fallback;
}

void TaskScheduler::CollectGarbage() {
    Task* t = nullptr;
    while (SharedQueues::graveyard.pop(t)) {
        delete t;
    }
}
