#pragma once
#include <mutex>
#include <vector>
#include <unordered_set>
#include "TaskScheduler.h"
namespace T_Threads {
    class Event {
    private:
        std::mutex mtx;
        std::unordered_set<Task*> waiters;

    public:
        void AddWaiter(Task* t) {
            std::lock_guard<std::mutex> lock(mtx);
            t->assignedFiber->status.store(FiberStatus::SUSPENDED, std::memory_order_release);
            waiters.insert(t);
        }

        // Wake up one specific task
        void Signal(Task* t) {
            {
                std::lock_guard<std::mutex> lock(mtx);
                if (!waiters.erase(t)) return; // Task wasn't waiting

                // Mark as READY while holding the lock
                t->assignedFiber->status.store(FiberStatus::READY, std::memory_order_release);
            }
            // Now push to scheduler outside the lock to minimize contention
            TaskScheduler::Instance().Push(t);
        }

        // Wake up everyone waiting on this specific event
        void SignalAll() {
            std::vector<Task*> to_wake;
            {
                std::lock_guard<std::mutex> lock(mtx);
                for (auto* t : waiters) {
                    t->assignedFiber->status.store(FiberStatus::READY, std::memory_order_release);
                    to_wake.push_back(t);
                }
                waiters.clear();
            }

            for (auto* t : to_wake) {
                TaskScheduler::Instance().Push(t);
            }
        }
    };
};