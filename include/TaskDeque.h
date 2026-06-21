#pragma once
#include <atomic>
#include <cstddef>
#include <optional>
#include <iostream>

#include "Task.h"

namespace T_Threads {

    class TaskDeque {
    public:
        explicit TaskDeque(size_t capacity = 32768)
            : capacity_(capacity),
            mask_(capacity - 1),
            buffer_(new Task* [capacity])
        {
            if ((capacity & (capacity - 1)) != 0)
                throw std::runtime_error("Capacity must be a power of 2");

            for (size_t i = 0; i < capacity; i++)
                buffer_[i] = nullptr;

            top_.store(0, std::memory_order_relaxed);
            bottom_.store(0, std::memory_order_relaxed);
        }

        ~TaskDeque() {
            delete[] buffer_;
        }

        // Owner-only Push
        bool push_bottom(Task* item) {
            if (!item) {
                std::cerr << "[TaskDeque::push_bottom] ERROR: pushing null item!\n";
                return false;
            }
            size_t b = bottom_.load(std::memory_order_relaxed);
            size_t t = top_.load(std::memory_order_acquire);
            if (b - t >= capacity_) {
                return false;  // Full
            }
            buffer_[b & mask_] = item;
            std::atomic_thread_fence(std::memory_order_release);
            bottom_.store(b + 1, std::memory_order_release);
            return true;
        }
        
        // Owner-only pop
        std::optional<Task*> pop_bottom() {
            size_t b = bottom_.load(std::memory_order_relaxed);
            size_t t = top_.load(std::memory_order_acquire);

            // Check if empty FIRST, before modifying bottom_
            if (t >= b) {
                return std::nullopt;  // Empty
            }

            b -= 1;
            bottom_.store(b, std::memory_order_release);

            std::atomic_thread_fence(std::memory_order_seq_cst);

            t = top_.load(std::memory_order_acquire);

            if (t <= b) {
                // Not empty
                Task* item = buffer_[b & mask_];
                if (!item) {
                    std::cerr << "[TaskDeque::pop_bottom] WARNING: read nullptr from buffer at index " << (b & mask_) << " (b=" << b << " t=" << t << ")\n";
                }
                if (t == b) {
                    // Last item race
                    if (!top_.compare_exchange_strong(
                        t, t + 1,
                        std::memory_order_acq_rel,
                        std::memory_order_relaxed))
                    {
                        // Stealer won
                        bottom_.store(b + 1, std::memory_order_relaxed);
                        return std::nullopt;
                    }
                    // Owner wins
                    bottom_.store(b + 1, std::memory_order_relaxed);
                }
                return item;
            }
            else {
                // Empty
                bottom_.store(t, std::memory_order_relaxed);
                return std::nullopt;
            }
        }
        
        std::optional<Task*> steal() {
            size_t t = top_.load(std::memory_order_acquire);
            std::atomic_thread_fence(std::memory_order_seq_cst);
            size_t b = bottom_.load(std::memory_order_acquire);

            if (t < b) {
                Task* item = buffer_[t & mask_];
                if (top_.compare_exchange_strong(
                    t, t + 1,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed))
                {
                    return item;
                }
            }
            return std::nullopt;
        }

        size_t size() const {
            size_t t = top_.load(std::memory_order_acquire);
            size_t b = bottom_.load(std::memory_order_acquire);
            return (b > t) ? (b - t) : 0;
        }

        size_t capacity() const {
            return capacity_;
        }
        bool empty() const {
            size_t t = top_.load(std::memory_order_acquire);
            size_t b = bottom_.load(std::memory_order_acquire);
            return t >= b;
        }
    private:
        Task** buffer_;
        const size_t capacity_;
        const size_t mask_;

        alignas(64) std::atomic<size_t> top_;
        alignas(64) std::atomic<size_t> bottom_;
    };

} 