#pragma once
#include <atomic>
#include <type_traits>
#include "Task.h"

namespace T_Threads {

    template <typename T>
    class MPSCQueue {
        static_assert(std::is_pointer<T>::value, "MPSCQueue<T> expects a pointer type");

        std::atomic<Task*> head_;   
        Task*              tail_;   
        Task*              stub_;    

        void append(Task* n) {
            n->next.store(nullptr, std::memory_order_relaxed);
            Task* prev = head_.exchange(n, std::memory_order_acq_rel);
            prev->next.store(n, std::memory_order_release);
        }

    public:
        MPSCQueue() {
            stub_ = new Task();
            stub_->next.store(nullptr, std::memory_order_relaxed);
            head_.store(stub_, std::memory_order_relaxed);
            tail_ = stub_;
        }

        ~MPSCQueue() {
            clear();
            delete stub_;
        }

        void push(T task) { append(task); }

        void push_batch(T head_batch, T tail_batch) {
            tail_batch->next.store(nullptr, std::memory_order_relaxed);
            Task* prev = head_.exchange(tail_batch, std::memory_order_acq_rel);
            prev->next.store(head_batch, std::memory_order_release);
        }

        bool pop(T& out) {
            Task* tail = tail_;
            Task* next = tail->next.load(std::memory_order_acquire);

            if (tail == stub_) {
                if (!next) return false;            
                tail_ = next;                      
                tail = next;
                next = next->next.load(std::memory_order_acquire);
            }

            if (next) {                              
                tail_ = next;
                out = static_cast<T>(tail);
                return true;
            }

        
            if (tail != head_.load(std::memory_order_acquire)) {
                return false;
            }

            append(stub_);
            next = tail->next.load(std::memory_order_acquire);
            if (next) {
                tail_ = next;
                out = static_cast<T>(tail);
                return true;
            }
            return false;
        }

        void clear() {
            T tmp;
            while (pop(tmp)) { }
        }

        bool empty() const {
            Task* tail = tail_;
            Task* next = tail->next.load(std::memory_order_acquire);
            return (tail == stub_ && next == nullptr);
        }
    };
}
