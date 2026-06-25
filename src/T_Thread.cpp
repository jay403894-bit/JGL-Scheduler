#include "../include/T_Thread.h"
#include "../include/platform.h"
#include "../include/TaskScheduler.h"
#include <chrono>
#include <iostream>
using namespace T_Threads;
thread_local T_Thread* T_Thread::instance = nullptr;

T_Thread::T_Thread(TaskScheduler& scheduler) : scheduler(&scheduler) {
	std::memset(&schedulerCtx, 0, sizeof(Context));
}
T_Thread::~T_Thread() {
}
void T_Thread::StartWorker(size_t cpu_affinity)
{
	auto ready = std::make_shared<std::atomic<bool>>(false);
	thread = std::thread([this, ready]() {
		while (!ready->load(std::memory_order_acquire)) std::this_thread::yield();
		instance = this;
		thread_id = thread_counter.fetch_add(1);
		// Wire up the global fiber pool to this thread's cache
		localCache.SetGlobalPool(&scheduler->GetGlobalPool());
		this->Worker();
		});
	nativeHandle = thread.native_handle();
#ifdef _WIN32
	SetThreadAffinityMask(nativeHandle, 1ULL << cpu_affinity);
#endif
	ready->store(true, std::memory_order_release);
};
std::thread::id T_Thread::GetID() {
	return thread.get_id();
}
bool T_Thread::SetImmediateTask(Task* new_task) {
	if (!new_task) return false;
	{
		immediateTask = new_task;
		immediate.store(true, std::memory_order_release);
	}
	cv.notify_one();
	return true;
}
void T_Thread::SetQueueIndex(size_t index)
{
	qIndex = index;
};
void T_Thread::Join() {
	bool expected = false;
	if (!joining.compare_exchange_strong(expected, true)) return;

	running.store(false, std::memory_order_release);
	NotifyWorker();

	std::unique_lock<std::mutex> lock(joinMutex);
	cvWorkerDone.wait(lock, [this] {
		return !running.load(std::memory_order_acquire);
		});

	if (thread.joinable())
		thread.join();

	joining.store(false, std::memory_order_release);
}
T_Thread* T_Thread::GetCurrent() { 
	return instance; 
}

void T_Thread::CoYield(Fiber* targetFiber){
	if (targetFiber) {
		targetFiber->CoYield();
	}
}
void T_Thread::Suspend(Fiber* targetFiber){
	if (targetFiber) {
		targetFiber->Suspend();
	}
}
 void T_Thread::Resume(Fiber* targetFiber) {
	 if (targetFiber) {
		 targetFiber->Resume(); // This pushes it into the TaskScheduler queue
	 }
}

 void T_Threads::T_Thread::CoYield()
 {
	 GetCurrent()->currentFiber->CoYield();
 }

 void T_Threads::T_Thread::Suspend()
 {
	 GetCurrent()->currentFiber->Suspend();
 }

 uint64_t T_Thread::GenerateID() {
	 return scheduler->nextId.fetch_add(1, std::memory_order_relaxed);
 }
void T_Thread::NotifyWorker(){
	cv.notify_one();
}

bool T_Thread::Ready(){
	return ready.load(std::memory_order_acquire);
}
       
Fiber* T_Thread::AcquireFiber(Task* task) {
	// 1. Try the pantry (Lock-free)
	Fiber* f = localCache.Pop();
	if (f) return f;

	// 2. If pantry empty, we must go to the "Global Warehouse"
	// Because your ThreadLocalCache::Pop() already calls StealBatch, 
	// it will re-populate itself automatically.
	f = localCache.Pop();

	if (!f) {
		std::cerr << "CRITICAL: Global pool exhausted!" << std::endl;
	}
	return f;
}

void T_Thread::ReleaseFiber(Fiber* f) {
	// Simply push to the pantry. 
	// The pantry will automatically flush to the GlobalPool if it gets too full.
	localCache.Push(f);
}

uint32_t T_Thread::FastRand() {
	static thread_local uint32_t x = []() {
		auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
		uint32_t seed = static_cast<uint32_t>(now);
		seed ^= (std::hash<std::thread::id>{}(std::this_thread::get_id()) << 1);
		return seed == 0 ? 1 : seed;
		}();	
	x ^= x << 13;
	x ^= x >> 17;
	x ^= x << 5;
	return x;
}
void T_Thread::Worker() {
	running.store(true, std::memory_order_release);
	const size_t BATCH_SIZE = 64;
	Task* batch[BATCH_SIZE];

	while (running.load(std::memory_order_acquire)) {
		Task* task_to_run = nullptr;
		
		ready.store(true, std::memory_order_release);
		
		
		auto& inbox = scheduler->inboxes[qIndex];
		auto& local_deque = scheduler->threadQs[qIndex];

		size_t count = 0;
		// Collect available tasks into the local buffer
		while (count < BATCH_SIZE && scheduler->inboxes[qIndex]->pop(batch[count])) {
			count++;
		}

		// Use your existing push_bottom_batch for an atomic update
		if (count > 0) {
			if (!scheduler->threadQs[qIndex]->push_bottom_batch(batch, count)) {
				// If deque is full, handle overflow (e.g., push back to inbox or wait)
				std::cerr << "Local Deque full, overflow handling needed!" << std::endl;
			}
		}

		// --- 1. Immediate task execution ---
		bool is_handling_fork = false;
		{
			if (immediateTask != nullptr) {
				if (!localQ.empty()) {
					while (!localQ.empty()) {
						Task* t = localQ.back();
						localQ.pop_back();
						scheduler->threadQs[qIndex]->push_bottom(t);
					}
				}
				task_to_run = immediateTask;
				current_task = immediateTask;
				immediateTask = nullptr;
				immediate.store(false, std::memory_order_release);
				is_handling_fork = true;
			}
		}
		{
			// --- 2. Local Worker queue of set affinity ---
			if (!localQ.empty()) {
				task_to_run = localQ.back();
				localQ.pop_back();
			}

			// --- 3. Local work-stealing queue ---
			if (!task_to_run) {
				auto opt = scheduler->threadQs[qIndex]->pop_bottom();
				if (opt) {
					Task* task = *opt;
					if (!task) {
						std::cerr << "[worker " << qIndex << "] Null task from pop_bottom!" << std::endl;
					}
					else {
						task_to_run = task;
						current_task = task;
					}
				}
			}
			// --- 4. Work stealing ---
			if (!task_to_run) {
				std::uniform_int_distribution<> dist(1, scheduler->workers.size()-1);
				int res = FastRand() % (scheduler->workers.size() - 1);
				while (res == qIndex) {
					res = FastRand() % (scheduler->workers.size() - 1);
				}
				auto stolen = scheduler->threadQs[res]->steal();
				if (stolen.has_value()) {
					task_to_run = *stolen;
					current_task = task_to_run;
				}
			}
		}
	

		// --- 6. Execute task if found ---
		if (task_to_run) {
			Fiber* existingFiber = task_to_run->assignedFiber;

			Fiber* f;
			if (existingFiber) {
				f = existingFiber;      // resume existing context
			}
			else {
				f = AcquireFiber(task_to_run);
				if (!f) {
					std::cerr << "CRITICAL: Fiber pool exhausted!" << std::endl;
					scheduler->threadQs[qIndex]->push_bottom(task_to_run);
					std::this_thread::yield();
					continue;
				}
				task_to_run->assignedFiber = f;
				f->owningTask = task_to_run;
				f->Init(GlobalFiberPool::FiberEntryWrapper);
			}

			f->status.store(FiberStatus::RUNNING, std::memory_order_release);
			f->homeCtx = &this->schedulerCtx;   // where the fiber returns to: THIS worker
			currentRunningTask = task_to_run;
			currentFiber = f;
			{
				EpochGuard guard(thread_id);
				ContextSwitch(&this->schedulerCtx, &f->ctx);
			}

			FiberStatus fs = f->status.load(std::memory_order_acquire);
			if (fs == FiberStatus::DEAD) {
				// Completed for good -- reclaim the task and the fiber. Free it the same
				// way it was allocated: slab tasks (CreateTask) go back to the slab; raw
				// `new Task(...)` tasks are delete'd. Freeing a heap pointer into the slab
				// poisons the free list and overflows on reuse (see Task::ownedBySlab).
				task_to_run->assignedFiber = nullptr;
				ReleaseFiber(f);
				bool slab = task_to_run->ownedBySlab;
				task_to_run->~Task();
				if (slab)
					scheduler->GetAllocator()->Free(task_to_run);
				else
					::operator delete(task_to_run);
				scheduler->pendingTasks.fetch_sub(1, std::memory_order_acq_rel);
				currentFiber = nullptr;
				currentRunningTask = nullptr;
			}
			else if (fs == FiberStatus::WANTS_YIELD) {
				// Yielded. The switch above already saved the fiber's context
				f->status.store(FiberStatus::READY, std::memory_order_release);
				scheduler->threadQs[qIndex]->push_bottom(task_to_run);
				currentFiber = nullptr;
				currentRunningTask = nullptr;
			}
			else {
				// WANTS_SUSPEND: publish SUSPENDED now that the context is saved. 
				f->status.store(FiberStatus::SUSPENDED, std::memory_order_release);
				currentFiber = nullptr;
				currentRunningTask = nullptr;
			}

			if (is_handling_fork) {
				if (qIndex < (int)scheduler->immediateCoresInUse.size()) {
					scheduler->immediateCoresInUse[qIndex]->store(false, std::memory_order_release);
				}
				is_handling_fork = false;
			}

			if (retired.size() > 512) {
				EpochManager::Instance().Tick();
			}
			{
				std::unique_lock<std::mutex> lock(workerMutex);
				cv.wait_for(lock, std::chrono::milliseconds(1), [this]() {
					return !running.load(std::memory_order_acquire)
						|| immediate.load(std::memory_order_acquire)
						|| (!scheduler->paused.load(std::memory_order_acquire) );
					});

				if (!running.load(std::memory_order_acquire)) break;
			}
		}
	}
	running.store(false, std::memory_order_release);
	cvWorkerDone.notify_all();
}
