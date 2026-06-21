#pragma once
#define NOMINMAX
#include <vector>
#include <atomic>
#include <iostream>
#include <intrin.h>
#include <limits>
#include <cstdint>
#include "TaskScheduler.h"

namespace T_Threads {
	struct LNodeBase; 
	struct LMarkableReference {
		LNodeBase* val;
		bool marked;

		LMarkableReference(LNodeBase* val = nullptr, bool mark = false)
			: val(val), marked(mark) {}
	};

	struct LMarkablePointer {
		std::atomic<uintptr_t> ref{ 0 };

		static uintptr_t pack(LNodeBase* ptr, bool mark) {
			return reinterpret_cast<uintptr_t>(ptr) | (mark ? 1ULL : 0ULL);
		}
		static LNodeBase* unpackPtr(uintptr_t val) {
			return reinterpret_cast<LNodeBase*>(val & ~1ULL);
		}
		static bool unpackMark(uintptr_t val) {
			return (val & 1ULL) != 0;
		}

		LMarkablePointer(LNodeBase* val = nullptr, bool mark = false) {
			ref.store(pack(val, mark), std::memory_order_release);
		}

		LNodeBase* getReference() const {
			return unpackPtr(ref.load(std::memory_order_acquire));
		}

		bool getMark() const {
			return unpackMark(ref.load(std::memory_order_acquire));
		}

		LNodeBase* get(bool& mark) const {
			uintptr_t val = ref.load(std::memory_order_acquire);
			mark = unpackMark(val);
			return unpackPtr(val);
		}

		bool attemptMark(LNodeBase* expectedPtr, bool newMark) {
			uintptr_t curr = ref.load(std::memory_order_acquire);
			while (true) {
				LNodeBase* ptr = unpackPtr(curr);
				bool mark = unpackMark(curr);

				if (ptr != expectedPtr) return false;
				if (mark == newMark) return true;

				uintptr_t desired = pack(ptr, newMark);
				if (ref.compare_exchange_weak(curr, desired, std::memory_order_acq_rel))
					return true;
			}
		}

		void set(LNodeBase* val, bool mark) {
			ref.store(pack(val, mark), std::memory_order_release);
		}

		bool compareAndSet(LNodeBase* expectedPtr, LNodeBase* newPtr, bool expectedMark, bool newMark) {
			uintptr_t curr = pack(expectedPtr, expectedMark);
			uintptr_t desired = pack(newPtr, newMark);
			return ref.compare_exchange_strong(curr, desired, std::memory_order_acq_rel);
		}
	};
	struct LNodeBase {
		LMarkablePointer next;   
		uint64_t key;          
	};
	template<typename T>
	struct LNode : LNodeBase {
		T data;  
		LNode(uint64_t k, T d) { 
			key = k;
			data = d;
		}
	};

	template <typename T>
	class LockFreeList {
		Arena* arena;
		struct Window {
			LNodeBase* pred;
			LNodeBase* curr;
			Window(LNodeBase* myPred, LNodeBase* myCurr) {
				pred = myPred, curr = myCurr;
			}
			static Window find(LNodeBase* head, uint64_t key) {
				LNodeBase* pred = nullptr;
				LNodeBase* curr = nullptr;
				LNodeBase* succ = nullptr;
				bool marked = false;
				bool snip = false;
			RETRY:
				while (true) {
					pred = head;
					curr = pred->next.getReference();
					while (true) {
						succ = curr->next.get(marked);
						while (marked) {
							snip = pred->next.compareAndSet(curr, succ, false, false);
							if (!snip) goto RETRY;
							curr = succ;
							succ = curr->next.get(marked);
						}
						if (curr->key >= key)
							return Window(pred, curr);
						pred = curr;
						curr = succ;
					}
				}
			}
		};

		LNodeBase* head;
		LNodeBase* tail;
	public:
		LockFreeList(Arena* arena) : arena(arena) {
			void* memHead = arena->allocate(sizeof(LNode<T>));
			void* memTail = arena->allocate(sizeof(LNode<T>));
			head = new (memHead) LNode<T>(0, T());
			tail = new (memTail) LNode<T>(UINT64_MAX, T());
			head->next.set(tail, false);
		}
		~LockFreeList() {}
		bool add(uint64_t key, T item) {
			while (true) {
				Window window = Window::find(head, key);
				LNode<T>* pred = static_cast<LNode<T>*>(window.pred);
				LNode<T>* curr = static_cast<LNode<T>*>(window.curr);

				if (curr->key == key)
					return false;

				void* mem = arena->allocate(sizeof(LNode<T>));
				LNode<T>* node = new (mem) LNode<T>(key, item);
				node->next.set(curr, false);

				if (pred->next.compareAndSet(curr, node, false, false)) {
					return true;
				}
			}
		}
		bool remove(uint64_t key) {
			bool snip = false;
			while (true) {
				Window window = Window::find(head, key);
				LNode<T>* pred = static_cast<LNode<T>*>(window.pred);
				LNode<T>* curr = static_cast<LNode<T>*>(window.curr);
				if (curr->key != key) {
					return false;
				}
				else {
					LNode<T>* succ = curr->next.getReference();
					snip = curr->next.attemptMark(succ, true);
					if (!snip)
						continue;
					pred->next.compareAndSet(curr, succ, false, false);
					return true;
				}
			}
		}
		bool contains(uint64_t key) {
			LNodeBase* curr = head;

			while (curr != nullptr) {
				LNodeBase* succ = curr->next.getReference();
				bool marked = curr->next.getMark();

				if (curr->key >= key) {
					return (curr->key == key && !marked);
				}

				curr = succ;
			}
			return false;
		}
		T* get(uint64_t key) {
			bool marked = false;
			LNodeBase* curr = head;

			while (curr->key < key) {
				curr = curr->next.get(marked);
			}

			if (curr->key == key && !marked) {
				LNode<T>* typedNode = static_cast<LNode<T>*>(curr);
				return &typedNode->data;  // return pointer to T
			}

			return nullptr;  // not found
		}
		template <typename F>
		void for_each(F func) {
			LNodeBase* curr = head->next.getReference();
			while (curr != tail) {
				bool marked = false;
				LNodeBase* succ = curr->next.get(marked);
				if (!marked) {
					LNode<T>* typedNode = static_cast<LNode<T>*>(curr);
					func(typedNode->data);
				}
				curr = succ;
			}
		}
	};
};