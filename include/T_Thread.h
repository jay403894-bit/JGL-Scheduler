#pragma once
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <queue>
#include "Task.h"
#include "SharedQueues.h"
#include "Epochs.h"

namespace T_Threads {
    inline thread_local Task* current_task = nullptr;

    class T_Thread {
    public:
        T_Thread();
        T_Thread(const T_Thread& other) = delete;
        T_Thread& operator=(const T_Thread& other) = delete;
        ~T_Thread();
        void StartWorker(size_t cpu_affinity);
        std::thread::id GetID();
        bool SetImmediateTask(Task* task_);
        int GetQueueLoad();
        void SetQueueIndex(size_t index);
        void Join();
        void NotifyWorker();
        bool AllQueuesEmpty();
        bool Ready();
        void OnFinishedArena(ArenaPool* arena);
    private:
        void Worker();

        std::atomic<int> qLoad{ 0 };
        std::atomic<bool> immediate{ false };
        std::atomic<bool> running{ false };
        std::atomic<bool> ready{ false };
        std::atomic<bool> joining{ false };
        int qIndex = 0;
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