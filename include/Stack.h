#pragma once
#include <atomic>
#include "Epochs.h"
namespace T_Threads {
    template <typename T>
    class Stack {
    private:
        struct Node {
            T value;
            Node* next;
        };
        std::atomic<Node*> head;
    public:
        void Push(T v) {
            Node* n = new Node{ v,nullptr };
            Node* oldHead = head.load();
            do {
                n->next = oldHead;
            } while (!head.compare_exchange_weak(oldHead, n, std::memory_order_release, std::memory_order_relaxed));
        }

        bool Pop(T& v, size_t currentEpoch) {
            Node* oldHead = head.load(std::memory_order_acquire);
            while (oldHead) {
                Node* next = oldHead->next;
                if (head.compare_exchange_weak(oldHead, next,
                    std::memory_order_acquire, std::memory_order_relaxed)) {

                    v = oldHead->value;

                    // Instead of delete, we RETIRE the node to the EpochManager
                    // This ensures other threads aren't looking at this node
                    EpochManager::Instance().RetirePtr<Node>(oldHead, currentEpoch);
                    return true;
                }
            }
            return false;
        }
    };
};