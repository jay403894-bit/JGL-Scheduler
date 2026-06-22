#pragma once
#include <atomic>
#include <memory>
#include <iostream>

namespace T_Threads {
	template <typename T>
	class MPSCQueue {
	private:
		struct Node {
			T data_;
			std::atomic<Node*> next_;
			Node(T d = T()) : data_(d), next_(nullptr) {}
		};

		std::atomic<Node*> tail_;
		Node* head_;
		std::atomic<size_t> size_;
		std::atomic<int> nodesPushed{ 0 };
	public:
		MPSCQueue() {
			head_ = new Node();
			tail_.store(head_, std::memory_order_relaxed);
			size_ = 0;
		}

		~MPSCQueue() {
			clear();
			delete head_;
		}

		void push(const T& item) {
			Node* node = new Node(item);
			Node* prev = tail_.exchange(node, std::memory_order_acq_rel);
			prev->next_.store(node, std::memory_order_release);
			size_.fetch_add(1, std::memory_order_release);
		}


		bool pop(T& out_result) {
			Node* head = head_;
			Node* next = head->next_.load(std::memory_order_acquire);

			if (!next) {
				// Check if a push is in flight: tail moved but link not yet written
				if (tail_.load(std::memory_order_acquire) == head) {
					return false; // truly empty
				}
				// PushToCore is in progress — spin until the link is visible
				while (!(next = head->next_.load(std::memory_order_acquire))) {
					std::this_thread::yield();
				}
			}

			out_result = next->data_;
			head_ = next;
			delete head;
			size_.fetch_sub(1, std::memory_order_relaxed);
			return true;
		}

		bool empty() const {
			return head_ == tail_.load(std::memory_order_acquire);
		}

		void clear() {
			T dummy;
			while (pop(dummy)) {}
		}
	};
};