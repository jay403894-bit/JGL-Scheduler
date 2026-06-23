#pragma once
#define NOMINMAX
#include "Task.h"
#include "MPSCQueue.h"
#include "Arena.h"
#include "Epochs.h"
#include "FiberPool.h"
#include "TaskDeque.h"
#include "../include/blockingconcurrentqueue.h"
#include <atomic>
#include <array>
#include <vector>
#include <memory>
#include <mutex>
#include <functional>
#include <string>
#include <unordered_map>
#include <thread>

namespace T_Threads {
    class T_Thread;
    class Event;

    struct QTraits : moodycamel::ConcurrentQueueDefaultTraits {
        static constexpr size_t BLOCK_SIZE = 32768;
        static constexpr size_t IMPLICIT_INITIAL_INDEX_SIZE = 1024;
    };

    class TaskScheduler {
        friend class T_Thread;
    public:
        static TaskScheduler& Instance() {
            if (!instance)
                throw std::runtime_error("Call TaskScheduler::Init() before Instance()!");
            return *instance;
        }
        static void Init(size_t poolSize = 0); // 0 = auto-detect

        ~TaskScheduler();
        bool EnqueueToMain(Task* task);
        void ProcessMainThread();
        void Join();
        void NotifyAll();
        void ParallelFor(int start, int end, int chunkSize, std::function<void(int, int)> func);
        void ParallelForNB(int start, int end, int chunkSize, std::function<void(int, int)> func);
        bool Push(Task* task);
        bool Push(uint8_t cpu_affinity, Task* task);
        bool PushPQ(Task* task);
        bool PushPQ(uint8_t priority, Task* task);
        bool PushFork(uint8_t cpu_affinity, Task* task);
        Event& GetEvent(const std::string& name);
        void WaitOnEvent(const std::string& eventName);
        void Pause();
        void Resume();
        void Stop(Task* worker_task);
        void Wait(const std::vector<Task*>& tasks);
        void WaitAll();
        Arena* GetArena();
        void* AllocateFromArena(size_t size);

        Task* CreateTask(void(*fn)(void*), void* data);

        template<typename F>
        LambdaTask<F>* CreateTask(F&& f) {
            void* mem = taskArena.GetActive()->allocate(sizeof(LambdaTask<F>));
            if (!mem) return nullptr;
            return new (mem) LambdaTask<F>(std::forward<F>(f));
        }
        template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
        void Push(F&& f) {
            auto* t = CreateTask(std::forward<F>(f));
            PushLocal(t);
        }
        template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
        void Push(uint8_t cpu_affinity, F&& f) {
            auto* t = CreateTask(std::forward<F>(f));
            PushLocal(t, cpu_affinity);
        }
        template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
        void PushPQ(F&& f) {
            auto* t = CreateTask(std::forward<F>(f));
            PushToPQ(t);
        }
        template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
        void PushPQ(uint8_t priority, F&& f) {
            auto* t = CreateTask(std::forward<F>(f));
            PushToPQ(t, priority);
        }
        template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
        void PushFork(size_t coreID, F&& f) {
            auto* t = CreateTask(std::forward<F>(f));
            PushToCore(coreID, t);
        }

    private:
        explicit TaskScheduler(size_t poolSize);

        // ---------- former SharedQueues state ----------
        std::unique_ptr<FiberPool> fiberPool;
        ArenaPool taskArena{ 10 * 1024 * 1024 };
        std::atomic<int> runningTasks{ 0 };
        std::vector<std::unique_ptr<std::atomic<bool>>> immediateCoresInUse;
        std::atomic<bool> paused{ false };
        MPSCQueue<Task*> graveyard;
        std::vector<std::unique_ptr<TaskDeque>> threadQs;
        std::vector<std::unique_ptr<MPSCQueue<Task*>>> inboxes;
        std::array<moodycamel::ConcurrentQueue<Task*, QTraits>, 5> priorityQ;
        // -----------------------------------------------

        static TaskScheduler* instance;
        static size_t GetSafeTC();
        Task* GetTask();
        void StartPool(size_t poolSize);
        bool PushLocal(Task* task, uint8_t cpuaffinity = 0);
        bool PushToPQ(Task* task, uint8_t priority = 3);
        bool PushToCore(size_t core_id, Task* task);
        int PickNextWorker();

        std::unordered_map<std::string, std::unique_ptr<Event>> eventRegistry;
        std::mutex registryMtx;
        std::atomic<bool> poolActive{ false };
        std::atomic<int> nextWorker{ 0 };
        std::atomic<bool> stopFlag{ false };
        std::vector<std::shared_ptr<T_Thread>> workers;
        MPSCQueue<Task*> mainQ;
        std::mutex poolMutex;
    };
}
