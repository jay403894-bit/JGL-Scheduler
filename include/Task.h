#pragma once
#include <functional>
#include <atomic>
namespace T_Threads {
    struct Fiber;
    struct WaitGroup { std::atomic<int> n{ 0 }; };

    enum class FiberSize { Standard, Heavy };
    struct alignas(16) Task {
        using Func = void(*)(void*);

        Func fn;
        void* data = nullptr;
        Fiber* assignedFiber = nullptr; 
        std::atomic<uint8_t> stopFlag{ false };
        std::function<void()> onComplete;
        std::atomic<uint8_t> complete{ false };
        std::atomic<uint8_t> callbackFlag{ false };
        std::atomic<Task*> next{ nullptr };
        WaitGroup* waitGroup = nullptr;
        uint8_t ownedBySlab = false; // If true, the task is allocated from the slab and should be reclaimed there
        uint8_t hiPri = false;
        FiberSize requiredSize = FiberSize::Standard;
        Task() : next(nullptr), complete(false), fn(nullptr), data(nullptr), assignedFiber(nullptr) {}
        Task(Func f, void* d = nullptr, uint8_t hipri =false, FiberSize size = FiberSize::Standard)
            : fn(f), data(d), hiPri(hipri), requiredSize(size) {
        }
        virtual ~Task() = default;
        void* operator new(std::size_t) = delete;
        void* operator new[](std::size_t) = delete;
        void operator delete(void*) = delete;
        void operator delete[](void*) = delete;

        inline void Execute() noexcept {
             fn(data);
             if (onComplete && !callbackFlag.load(std::memory_order_acquire)) onComplete();
             if (waitGroup) waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
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
            stopFlag.store(true, std::memory_order_release);
        }
    };

    template<typename F>
    class alignas(16) LambdaTask : public Task {
        F func;
    public:
        // Ensure we use perfect forwarding for the lambda
        LambdaTask(F&& f)
            : Task(LambdaTask::ExecuteWrapper, nullptr),
            func(std::forward<F>(f))
        {
            this->data = this;
        }
        void* operator new(std::size_t) = delete;
        void* operator new[](std::size_t) = delete;
        void operator delete(void*) = delete;
        void operator delete[](void*) = delete;

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