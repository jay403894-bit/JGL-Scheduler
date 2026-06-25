#pragma once
#include <atomic>
#include <type_traits>
#include "Task.h"

namespace T_Threads {
    // Intrusive multi-producer / single-consumer queue (Vyukov).
    //
    // T is Task*. The link is Task::next, so the queue "node" IS the Task.
    //
    // CRITICAL invariant: the node handed back by pop() is fully DETACHED from the
    // queue, so the single consumer may safely destroy/Free it. We keep a dedicated,
    // never-returned `stub_` as the sentinel. The old implementation recycled the
    // just-popped task AS the sentinel (head_ = next), so once the worker freed that
    // task the sentinel dangled -> pop() then read freed/recycled memory and either
    // lost queued tasks (remaining never hits 0 -> hang) or dereferenced garbage
    // (crash). Routing the stub through the chain instead is what makes detaching safe.
    template <typename T>
    class MPSCQueue {
        static_assert(std::is_pointer<T>::value, "MPSCQueue<T> expects a pointer type");

        std::atomic<Task*> head_;   // producers append here (also used by the re-pushed stub)
        Task*              tail_;    // consumer reads here (single thread only)
        Task*              stub_;    // persistent sentinel: never returned, never Freed by callers

        // Append one node. Multi-producer safe; also called by the consumer to recycle
        // the stub, which is fine -- exchange makes the consumer just one more producer.
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

        // Append a chain whose internal links the caller already set (head..tail).
        void push_batch(T head_batch, T tail_batch, size_t /*count*/) {
            tail_batch->next.store(nullptr, std::memory_order_relaxed);
            Task* prev = head_.exchange(tail_batch, std::memory_order_acq_rel);
            prev->next.store(head_batch, std::memory_order_release);
        }

        bool pop(T& out) {
            Task* tail = tail_;
            Task* next = tail->next.load(std::memory_order_acquire);

            if (tail == stub_) {
                if (!next) return false;             // truly empty
                tail_ = next;                        // step over the stub
                tail = next;
                next = next->next.load(std::memory_order_acquire);
            }

            if (next) {                              // tail has a successor -> fully detached
                tail_ = next;
                out = static_cast<T>(tail);
                return true;
            }

            // tail is the last node. If a producer is mid-append, retry later (no loss:
            // tail_ has not advanced past it).
            if (tail != head_.load(std::memory_order_acquire)) {
                return false;
            }

            // Route the stub through so the last node gains a successor and detaches.
            append(stub_);
            next = tail->next.load(std::memory_order_acquire);
            if (next) {
                tail_ = next;
                out = static_cast<T>(tail);
                return true;
            }
            return false;
        }

        // Single-consumer only. Drains pointers without freeing them (matches old behavior).
        void clear() {
            T tmp;
            while (pop(tmp)) { /* discard */ }
        }

        bool empty() const {
            Task* tail = tail_;
            Task* next = tail->next.load(std::memory_order_acquire);
            return (tail == stub_ && next == nullptr);
        }
    };
}
