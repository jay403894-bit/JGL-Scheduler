#pragma once
#include <functional>
#include <atomic>

namespace T_Threads {
    struct Task {
        using Func = void(*)(void*);

        Func fn;
        void* data = nullptr;
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
        F func; // The lambda capture is now INLINE in the memory block!
    public:
        LambdaTask(F&& f)
            : Task(LambdaTask::ExecuteWrapper, nullptr), // Pass a wrapper to the base
            func(std::forward<F>(f))
        {
            // Link the 'data' pointer to 'this' so the wrapper can find the lambda
            this->data = this;
        }
    private:
        // 2. Static wrapper that calls the lambda stored in the instance
        static void ExecuteWrapper(void* ptr) {
            LambdaTask* self = static_cast<LambdaTask*>(ptr);
            self->func();
        }
    };
 
};