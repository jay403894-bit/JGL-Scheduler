#pragma once
#include <functional>
#include <atomic>
namespace T_Threads {
    struct Fiber;

    struct Task {
        using Func = void(*)(void*);

        Func fn;
        void* data = nullptr;
        Fiber* assignedFiber = nullptr; 
        std::atomic<bool> stop_flag{ false };
        std::function<void()> onComplete;
        std::atomic<bool> complete{ false };
        std::atomic<bool> callbackFlag{ false };
        Task(Func f, void* d = nullptr)
            : fn(f), data(d) {
        }

        inline void Execute() noexcept {
             fn(data);
             if (onComplete && !callbackFlag.load(std::memory_order_acquire)) {
                 onComplete();
             }
            complete.store(true, std::memory_order_release);
        }

        inline void SignalComplete() {
            // Only fire if we haven't already
            if (!callbackFlag.exchange(true, std::memory_order_acq_rel)) {
                if (onComplete) {
                    onComplete();
                }
            }
        }
        inline void Stop() {
            stop_flag.store(true, std::memory_order_release);
        }
    };

    template<typename F>
    class LambdaTask : public Task {
        F func;
    public:
        // Ensure we use perfect forwarding for the lambda
        LambdaTask(F&& f)
            : Task(LambdaTask::ExecuteWrapper, nullptr),
            func(std::forward<F>(f))
        {
            this->data = this;
        }

    private:
        static void ExecuteWrapper(void* ptr) {
            // Cast the void* back to the correct LambdaTask type
            LambdaTask* self = static_cast<LambdaTask*>(ptr);

            // Invoke the stored lambda directly.
            // We use () because a lambda IS a functor.
            self->func();
        }
    };
 
};