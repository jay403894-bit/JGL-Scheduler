#pragma once
#include <vector>
#include <cstddef>
#include <mutex>
namespace T_Threads {
    class TaskAllocator {
    public:
        static constexpr size_t SLOT = 256;   // bytes per slot (>= your biggest task)
        static constexpr size_t BATCH = 32;    // refill/flush granularity
    private:
        struct alignas(16) Block { std::byte b[SLOT]; };

        // a Free slot's first 8 bytes ARE the "next Free" link (the intrusive trick)
        static void*& next(void* slot) { return *reinterpret_cast<void**>(slot); }

        // ---- shared backing (touched rarely, in batches, under the lock) ----
        std::vector<Block> mem;
        void* sharedHead = nullptr;
        std::mutex mtx;

        // ---- per-thread cache (the lock-Free hot path) ----
        struct Cache { void* head = nullptr; size_t count = 0; };
        static Cache& local() {
            static thread_local Cache c;       // ONE per thread (see caveat below)
            return c;
        }

    public:
        explicit TaskAllocator(size_t slots) : mem(slots) {
            for (auto& blk : mem) { next(&blk) = sharedHead; sharedHead = &blk; }
        }

        void* Alloc() {                        // lock-Free unless the cache is empty
            Cache& c = local();
            if (!c.head) refill(c);
            if (!c.head) return nullptr;       // backing fully exhausted
            void* slot = c.head;
            c.head = next(slot);
            c.count--;
            return slot;
        }

        void Free(void* slot) {                // lock-Free unless the cache overflows
            Cache& c = local();
            next(slot) = c.head;
            c.head = slot;
            c.count++;
            if (c.count > 2 * BATCH) flush(c);
        }

    private:
        void refill(Cache& c) {                // move up to BATCH from shared -> local
            std::lock_guard<std::mutex> lk(mtx);
            for (size_t i = 0; i < BATCH && sharedHead; ++i) {
                void* s = sharedHead; sharedHead = next(s);
                next(s) = c.head;   c.head = s;   c.count++;
            }
        }
        void flush(Cache& c) {                 // move excess from local -> shared
            std::lock_guard<std::mutex> lk(mtx);
            while (c.count > BATCH) {
                void* s = c.head; c.head = next(s); c.count--;
                next(s) = sharedHead; sharedHead = s;
            }
        }
    };
};