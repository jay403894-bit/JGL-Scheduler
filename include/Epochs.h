#pragma once
#include <atomic>
#include <thread>
#include <vector>
#include <mutex>
#include "Task.h"

namespace T_Threads {
	struct EpochParticipant {
		std::atomic<size_t> localEpoch{ SIZE_MAX };
	};
	struct RetiredAlloc {
		void* ptr;
		size_t epoch;
		void (*deleter)(void*);
	};
	using DeleterFunc = void(*)(void*);
	struct LNodeBase;
	struct LMarkableReference;
	struct SNMarkableReference;
	struct SNodeBase;
	struct DelayedTask;
	struct PeriodicTask;
	extern thread_local size_t thread_id;
	inline std::atomic<size_t>  thread_counter;
	extern thread_local std::vector<RetiredAlloc> retired;
	class EpochManager {
	private:
		std::mutex retireMutex;
		std::vector<std::atomic<size_t>*> participants;
		std::mutex participantMutex;
		struct GlobalRetired {
			void* ptr;        // Could be the node pointer OR the arena pointer
			size_t epoch;
			void (*deleter)(void*);
		};
		std::vector<GlobalRetired> globalRetiredList;

		std::atomic<size_t> globalEpoch{ 0 };

		struct ThreadEpoch {
			std::atomic<size_t> localEpoch{ SIZE_MAX };   // SIZE_MAX == not in an epoch
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
		// Simply pass the address of the member that already exists in your Task
		void RegisterParticipant(std::atomic<size_t>* slot) {
			std::lock_guard<std::mutex> lock(participantMutex);
			participants.push_back(slot);
		}

		
		static EpochManager& Instance() {
			// Intentionally leaked (never destructed). Worker threads can outlive main
			// in this design (the scheduler instance is heap-allocated and not deleted),
			// so a Meyers-singleton destructor would run at static teardown while a
			// worker still calls Enter/LeaveEpoch -> threadEpochs[tid] freed -> read AV.
			// Leaking the manager lets the OS reclaim it at process exit instead.
			static EpochManager* mgr = new EpochManager();
			return *mgr;
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
				// Register the bare-thread fallback slots (used by non-fiber callers,
				// e.g. the main thread building a DAG). Fiber slots are registered in
				// GlobalFiberPool. MinActiveEpoch scans the union of both.
				RegisterParticipant(&threadEpochs[i]->localEpoch);
			}
		}
		// Fallback epoch slot for a bare thread (one not running on a fiber), by thread_id.
		std::atomic<size_t>* ThreadSlot(size_t tid) {
			return &threadEpochs[tid]->localEpoch;
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
					it->deleter(it->ptr);
					it = globalRetiredList.erase(it);
				}
				else {
					++it;
				}
			}
		}
		size_t CurrentEpoch() { return globalEpoch.load(std::memory_order_acquire); }
		size_t MinActiveEpoch() {
			size_t minEpoch = globalEpoch.load(std::memory_order_acquire);
			// Scan the participants union (all fiber slots + all thread fallback slots).
			// Registration only happens at setup, so this never races a freed slot.
			std::lock_guard<std::mutex> lock(participantMutex);
			for (auto* slot : participants) {
				size_t e = slot->load(std::memory_order_acquire);
				if (e != SIZE_MAX && e < minEpoch) minEpoch = e;
			}
			return minEpoch;
		}
	
		template<typename T>
		void RetirePtr(T* p, size_t epoch, DeleterFunc d) {
			std::lock_guard<std::mutex> lock(retireMutex);
			globalRetiredList.push_back({ (void*)p, epoch, d });
		}

	
	private:
		void AdvanceEpoch() { globalEpoch.fetch_add(1, std::memory_order_acq_rel); }


	};
};

struct EpochGuard {
	std::atomic<size_t>* slot;

	EpochGuard(std::atomic<size_t>* s) : slot(s) {
		// Enter: store global epoch
		slot->store(T_Threads::EpochManager::Instance().CurrentEpoch(),
			std::memory_order_release);
	}

	~EpochGuard() {
		// Leave: mark as SIZE_MAX
		slot->store(SIZE_MAX, std::memory_order_release);
	}
};