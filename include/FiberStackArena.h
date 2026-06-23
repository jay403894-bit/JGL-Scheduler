#pragma once
#include <memoryapi.h>
#include <atomic>
#include "platform.h"
class FiberStackArena {
    void* base;
    size_t totalSize;
    std::atomic<size_t> offset;

public:
    FiberStackArena(size_t capacity) {
        // Allocate one massive chunk of memory
        base = VirtualAlloc(nullptr, capacity, MEM_RESERVE, PAGE_NOACCESS);
        totalSize = capacity;
        offset = 0;
    }
    ~FiberStackArena() {
        if (base) {
            VirtualFree(base, 0, MEM_RELEASE);
        }
    }
    void* AllocateStack(size_t size) {
        size_t current = offset.fetch_add(size);
        // Commit only the memory we actually need right now
        return VirtualAlloc((char*)base + current, size, MEM_COMMIT, PAGE_READWRITE);
    }
};