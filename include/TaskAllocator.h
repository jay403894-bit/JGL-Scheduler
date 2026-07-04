#pragma once
#include <vector>
#include <cstddef>
#include <mutex>
#include <atomic>
#ifdef _DEBUG
#include <Windows.h> // OutputDebugStringA/__debugbreak for the free-list canary check below
#endif
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
        std::atomic<long long> liveCount{ 0 }; // see LiveCount() below

        // ---- per-thread cache (the lock-Free hot path) ----
        struct Cache { void* head = nullptr; size_t count = 0; };
        static Cache& local() {
            static thread_local Cache c;       // ONE per thread (see caveat below)
            return c;
        }

    public:
        explicit TaskAllocator(size_t slots) : mem(slots) {
            for (auto& blk : mem) {
                next(&blk) = sharedHead; sharedHead = &blk;
#ifdef _DEBUG
                // Every slot starts life "free" -- stamp the canary here too (not just in
                // Free()), or the FIRST-ever Alloc() of a never-before-freed slot reads
                // whatever value(std::byte)-initialization left at bytes[8,16) (zero), which
                // doesn't match the canary and looks exactly like corruption on startup.
                *reinterpret_cast<uint64_t*>(reinterpret_cast<std::byte*>(&blk) + 8) = 0xFEEDFACECAFEBEEFULL;
#endif
            }
        }

#ifdef _DEBUG
        // Bytes [8,16) of a slot hold the intrusive "next free" link's tail + are otherwise
        // unused while free (the link itself only needs the first 8 bytes). Free() stamps a
        // canary there; Alloc() verifies it's UNCHANGED before handing the slot back out. If
        // something wrote through this slot AFTER it was freed (use-after-free) or if the same
        // slot got Free()'d twice (corrupting the free-list into aliasing two live owners), the
        // canary will have been clobbered by whatever real data got written into the
        // still-technically-free slot -- this catches it at the NEXT Alloc() of that exact slot,
        // right at the point of detection, instead of silently corrupting the free-list chain
        // until the whole pool eventually appears "exhausted" (Alloc() returning nullptr) far
        // downstream of the actual bug.
        static constexpr uint64_t kFreeCanary = 0xFEEDFACECAFEBEEFULL;
        static void StampCanary(void* slot) {
            *reinterpret_cast<uint64_t*>(reinterpret_cast<std::byte*>(slot) + 8) = kFreeCanary;
        }
        static void CheckCanary(void* slot) {
            uint64_t v = *reinterpret_cast<uint64_t*>(reinterpret_cast<std::byte*>(slot) + 8);
            if (v != kFreeCanary) {
                OutputDebugStringA("TaskAllocator: corrupted freed slot detected "
                    "(use-after-free or double-free) -- breaking at the Alloc() that noticed.\n");
                __debugbreak();
            }
        }
#endif

        void* Alloc() {                        // lock-Free unless the cache is empty
            Cache& c = local();
            if (!c.head) refill(c);
            if (!c.head) return nullptr;       // backing fully exhausted
            void* slot = c.head;
#ifdef _DEBUG
            CheckCanary(slot);
#endif
            c.head = next(slot);
            c.count--;
            liveCount.fetch_add(1, std::memory_order_relaxed);
            return slot;
        }

        void Free(void* slot) {                // lock-Free unless the cache overflows
            Cache& c = local();
            next(slot) = c.head;
#ifdef _DEBUG
            StampCanary(slot);
#endif
            c.head = slot;
            c.count++;
            liveCount.fetch_sub(1, std::memory_order_relaxed);
            if (c.count > 2 * BATCH) flush(c);
        }

        // Diagnostic only: how many slots are currently checked out (Alloc'd but not yet
        // Free'd), across every thread's cache + the shared pool. Not synchronized with
        // Alloc/Free beyond the atomic itself -- a momentary snapshot, good enough to watch
        // whether usage climbs monotonically (a real leak) or oscillates near a steady state
        // (normal churn) while chasing an exhaustion bug.
        long long LiveCount() const { return liveCount.load(std::memory_order_relaxed); }
        size_t Capacity() const { return mem.size(); }

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