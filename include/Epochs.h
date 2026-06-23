#pragma once
#include <atomic>
#include <thread>
#include <vector>
#include "Arena.h"
#include <mutex>
namespace T_Threads {
	struct RetiredAlloc {
		void* ptr;
		size_t epoch;
		void (*deleter)(void*);
	};
	struct LNodeBase;
	struct LMarkableReference;
	struct SNMarkableReference;
	struct SNodeBase;
	struct DelayedTask;
	struct PeriodicTask;
	extern thread_local size_t thread_id;
	inline std::atomic<size_t>  thread_counter;
	extern thread_local std::vector<RetiredAlloc> retired;
	extern thread_local Task* currentRunningTask;
	class EpochManager {
	private:
		std::mutex retireMutex;
		struct GlobalRetired {
			void* ptr;        // Could be the node pointer OR the arena pointer
			size_t epoch;
			bool isArena;     // Flag to differentiate
			void (*deleter)(void*);
		};
		std::vector<GlobalRetired> globalRetiredList;

		std::atomic<size_t> globalEpoch{ 0 };

		struct ThreadEpoch {
			std::atomic<size_t> localEpoch{ 0 };
		};
		std::vector<ThreadEpoch*> threadEpochs;
		EpochManager() = default;
	public:
		EpochManager(const EpochManager&) = delete;
		EpochManager& operator=(const EpochManager&) = delete;
		~EpochManager() {
			for (auto* te : threadEpochs) {
				delete te;
			}
			threadEpochs.clear();
		}
		static EpochManager& Instance() {
			static EpochManager mgr;
			return mgr;
		}
		void Tick()
		{
			AdvanceEpoch();
			TryReclaim();
		}
	
		void Init(size_t maxThreads)
		{
			threadEpochs.resize(maxThreads);
			for (size_t i = 0; i < maxThreads; i++) {
				threadEpochs[i] = new ThreadEpoch();
			}
		}
		void Reclaim(size_t safeEpoch) {
			auto it = retired.begin();
			while (it != retired.end()) {
				if (it->epoch < safeEpoch) {
					it->deleter(it->ptr);
					it = retired.erase(it);
				}
				else {
					++it;
				}
			}
		}
		void TryReclaim() {
			size_t safeEpoch = MinActiveEpoch();

			std::lock_guard<std::mutex> lock(retireMutex);
			auto it = globalRetiredList.begin();
			while (it != globalRetiredList.end()) {
				if (it->epoch < safeEpoch) {
					if (it->isArena) {
						// Cast back to Arena and clear
						static_cast<Arena*>(it->ptr)->clear();
					}
					else {
						// Call the standard node deleter
						it->deleter(it->ptr);
					}
					it = globalRetiredList.erase(it);
				}
				else {
					++it;
				}
			}
		}
		void EnterEpoch(size_t threadId) {
			threadEpochs[threadId]->localEpoch.store(globalEpoch.load(std::memory_order_acquire),
				std::memory_order_release);
		}
		void LeaveEpoch(size_t threadId) {
			threadEpochs[threadId]->localEpoch.store(SIZE_MAX, std::memory_order_release);
		}
		size_t CurrentEpoch() { return globalEpoch.load(std::memory_order_acquire); }
		size_t MinActiveEpoch() {
			size_t minEpoch = globalEpoch.load(std::memory_order_acquire);
			for (auto& te : threadEpochs) {
				size_t e = te->localEpoch.load(std::memory_order_acquire);
				if (e != SIZE_MAX && e < minEpoch) minEpoch = e;
			}
			return minEpoch;
		}
		/*template<typename T>
		void RetirePtr(void* p, size_t epoch, Arena* arena) {
			retired.push_back({ p, epoch, [](void* ptr) {
			} });
		}*/
		// For individual nodes
		template<typename T>
		void RetirePtr(T* p, size_t epoch) {
			std::lock_guard<std::mutex> lock(retireMutex);
			globalRetiredList.push_back({ (void*)p, epoch, false, [](void* ptr) {
				delete static_cast<T*>(ptr);
			} });
		}

		// For Arena blocks
		void RetireArena(Arena* arena, size_t epoch) {
			std::lock_guard<std::mutex> lock(retireMutex);
			globalRetiredList.push_back({ (void*)arena, epoch, true, nullptr });
		}
	private:
		void AdvanceEpoch() { globalEpoch.fetch_add(1, std::memory_order_acq_rel); }


	};
};

struct EpochGuard {
	size_t tid;
	EpochGuard(size_t id) : tid(id) {
		T_Threads::EpochManager::Instance().EnterEpoch(tid);
	}
	~EpochGuard() {
		T_Threads::EpochManager::Instance().LeaveEpoch(tid);
	}
};