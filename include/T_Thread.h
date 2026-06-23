#pragma once
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <queue>
#include "Task.h"
#include "Fiber.h"
#include "Arena.h"
#include "Epochs.h"
#include "Stack.h"

namespace T_Threads {
	class TaskScheduler;
    inline thread_local Task* current_task = nullptr;
    struct WaitHandle {
        Fiber* fiber;
        std::atomic<bool> signaled{ false }; // Did the event occur?
    };
    class T_Thread {
    public:
        static thread_local T_Thread* self;
        Context schedulerCtx; // The "Home Base"
        Fiber* currentFiber = nullptr; // Track the fiber active on this thread
        Task* currentRunningTask = nullptr;
        int qIndex = 0;

        T_Thread(TaskScheduler& scheduler);
        T_Thread(const T_Thread& other) = delete;
        T_Thread& operator=(const T_Thread& other) = delete;
        ~T_Thread();
        void StartWorker(size_t cpu_affinity);
        std::thread::id GetID();
        bool SetImmediateTask(Task* task_);

        static T_Thread* GetCurrent();
        int GetQueueLoad();
        void SetQueueIndex(size_t index);
        void Join();
        void NotifyWorker();
        bool AllQueuesEmpty();
        bool Ready();
        void OnFinishedArena(ArenaPool* arena);
    private:
        void Worker();

        TaskScheduler* scheduler;
		Stack<Fiber*> SuspendedFibers;
        std::atomic<int> qLoad{ 0 };
        std::atomic<bool> immediate{ false };
        std::atomic<bool> running{ false };
        std::atomic<bool> ready{ false };
        std::atomic<bool> joining{ false };
        std::mutex workerMutex;
        std::mutex joinMutex;
        std::condition_variable cvWorkerDone;
        std::condition_variable cv;
        std::condition_variable cvAffinity;
        Task* task = nullptr;
        Task* immediateTask = nullptr;
        std::thread thread;
        std::thread::native_handle_type nativeHandle;
        std::vector<Task*> localQ;
        std::vector<Task*> overflow;

    };
};