#pragma once
#include "Task.h"
namespace T_Threads {
 
        struct Arena {
        char* buffer;
        size_t size;
        std::atomic<size_t> offset{ 0 }; 

        Arena(size_t totalSize) : size(totalSize) {
            buffer = new char[totalSize];
        }
        ~Arena() { delete[] buffer; }

        void* allocate(size_t bytes) {
            size_t alignment = 8;
            size_t currentOffset;
            size_t nextOffset;

            do {
                currentOffset = offset.load(std::memory_order_relaxed);
                size_t padding = (alignment - (currentOffset % alignment)) % alignment;
                nextOffset = currentOffset + padding + bytes;

                if (nextOffset > size) return nullptr; // Out of memory

            } while (!offset.compare_exchange_weak(currentOffset, nextOffset,
                std::memory_order_relaxed));

            size_t padding = (alignment - (currentOffset % alignment)) % alignment;
            return buffer + currentOffset + padding;
        }

        void clear() { offset.store(0, std::memory_order_relaxed); }
    };
    struct ArenaPool {
        static constexpr size_t NUM_ARENAS = 3;
        Arena arenas[NUM_ARENAS];
        std::atomic<size_t> activeIndex{ 0 };
        ArenaPool(size_t sizePerArena) : arenas{
        Arena(sizePerArena),
        Arena(sizePerArena),
        Arena(sizePerArena)
        } {}
        Arena* GetActive() { return &arenas[activeIndex.load()]; }

        void Rotate() {
            activeIndex.store((activeIndex.load() + 1) % NUM_ARENAS);
        }
    };

};