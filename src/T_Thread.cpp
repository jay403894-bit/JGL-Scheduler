#include "../include/T_Thread.h"
#include "../include/platform.h"
#include <iostream>
using namespace T_Threads;
thread_local T_Thread* T_Thread::self = nullptr;

T_Thread::T_Thread() {
	std::memset(&schedulerCtx, 0, sizeof(Context));
}
T_Thread::~T_Thread() {
}
void T_Thread::StartWorker(size_t cpu_affinity)
{
	auto ready = std::make_shared<std::atomic<bool>>(false);
	thread = std::thread([this, ready]() {
		while (!ready->load(std::memory_order_acquire)) std::this_thread::yield();
		self = this;
		thread_id = thread_counter.fetch_add(1);
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
T_Thread* T_Thread::GetCurrent() { return self; }
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
void T_Thread::NotifyWorker()
{
	cv.notify_one();
}
bool T_Thread::AllQueuesEmpty() {
	if (!localQ.empty())
		return false;
	if (!overflow.empty())
		return false;
	if (!SharedQueues::inboxes[qIndex]->empty())
		return false;
	for (const auto& q : SharedQueues::threadQs) {
		if (!q->empty()) {
			return false;
		}
	}
	if (SharedQueues::proirityQ[0].size_approx() > 0)
		return false;
	if (SharedQueues::proirityQ[1].size_approx() > 0)
		return false;
	if (SharedQueues::proirityQ[2].size_approx() > 0)
		return false;
	if (SharedQueues::proirityQ[3].size_approx() > 0)
		return false;
	if (SharedQueues::proirityQ[4].size_approx() > 0)
		return false;

	return true;
}
bool T_Thread::Ready()
{
	return ready.load(std::memory_order_acquire);
}
void T_Thread::OnFinishedArena(ArenaPool* arena) {

	size_t epoch = EpochManager::Instance().CurrentEpoch();
	Arena* arenaToRetire = SharedQueues::taskArena.GetActive();

	EpochManager::Instance().RetireArena(arenaToRetire, epoch);

	// 3. Rotate the pool to the next arena
	SharedQueues::taskArena.Rotate();

	// 4. Tick to trigger the background reclamation
	EpochManager::Instance().Tick();

}
void T_Thread::Worker() {
	running.store(true, std::memory_order_release);
	while (running.load(std::memory_order_acquire)) {
		Task* task_to_run = nullptr;
		{
			ready.store(true, std::memory_order_release);
			std::unique_lock<std::mutex> lock(workerMutex);
			cv.wait_for(lock, std::chrono::microseconds(5), [this]() {
				return !running.load(std::memory_order_acquire)
					|| immediate.load(std::memory_order_acquire)
					|| (!SharedQueues::paused.load(std::memory_order_acquire) && !AllQueuesEmpty());
				});

			if (!running.load(std::memory_order_acquire)) break;
		}
		auto& inbox = SharedQueues::inboxes[qIndex];
		auto& local_deque = SharedQueues::threadQs[qIndex];
		Task* t = nullptr;
		if (!overflow.empty()) {
			while (local_deque->size() < local_deque->capacity() && !overflow.empty()) {
				if (!local_deque->push_bottom(overflow.back())) {
					break;  
				}
				overflow.pop_back();
			}
		}

		int retries = 0;
		const int MAX_RETRIES = 100;

		while (true) {
			if (inbox->pop(t)) {
				retries = 0;

				if (!t) {
					std::cerr << "[worker " << qIndex << "] ERROR: popped null task\n";
					continue;
				}

				qLoad.fetch_add(1, std::memory_order_relaxed);

				while (!local_deque->push_bottom(t)) {
					std::this_thread::yield();
				}
			}
			else {
				if (inbox->empty() && ++retries > MAX_RETRIES) {
					break; // confirmed empty after sustained polling
				}
				std::this_thread::yield();
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
						SharedQueues::threadQs[qIndex]->push_bottom(t);
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
			// --- 2. Local Worker queue  of set affinity---
			if (!localQ.empty()) {
				task_to_run = localQ.back();
				localQ.pop_back();
			}

			//--- 3 local work stealing queue of no affinity
			if (!task_to_run) {
				auto opt = SharedQueues::threadQs[qIndex]->pop_bottom();
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
				size_t numThreads = SharedQueues::threadQs.size();
				size_t start = rand() % numThreads;

				for (size_t i = 0; i < numThreads; ++i) {
					size_t target = (start + i) % numThreads;
					if (target == qIndex) continue;

					// Peek or pop the task
					auto opt = SharedQueues::threadQs[target]->steal();
					if (opt) {
						Task* task = *opt;

						// --- THE SAFETY CHECK ---
						// We assume you have a way to get the fiber assigned to this task
						// or that your Fiber class is the unit being queued.
						task_to_run = task;
						current_task = task;
						break;
					}
				}
			}
		}
		if(!task_to_run){
			// --- 5. Priority Queue ---
			bool success = false;
			Task* task;

			for (int i = 0; i < 5; i++) {
				success = SharedQueues::proirityQ[i].try_dequeue(task);
				if (success) {
					task_to_run = task;
					break;
				}
			}
		}


		// --- 6. Execute task if found ---
		if (task_to_run) {
			Fiber* existingFiber = task_to_run->assignedFiber;
			bool resuming = existingFiber &&
				existingFiber->status.load(std::memory_order_acquire) == FiberStatus::SUSPENDED;

			Fiber* f;
			if (resuming) {
				f = existingFiber; // switch back into the suspended fiber's saved context
			} else {
				f = SharedQueues::fiberPool->Acquire();
				if (!f) {
					std::cerr << "CRITICAL: Fiber pool exhausted!" << std::endl;
					// Put task back so it isn't lost, then yield
					SharedQueues::threadQs[qIndex]->push_bottom(task_to_run);
					std::this_thread::yield();
					continue;
				}
				task_to_run->assignedFiber = f;
				f->Init(FiberPool::FiberEntryWrapper);
			}

			f->status.store(FiberStatus::RUNNING, std::memory_order_release);
			currentRunningTask = task_to_run;
			currentFiber = f;
			{
				EpochGuard guard(thread_id);
				ContextSwitch(&this->schedulerCtx, &f->ctx);
			}

			// Check why we returned: task finished or fiber suspended itself
			if (f->status.load(std::memory_order_acquire) == FiberStatus::SUSPENDED) {
				// Fiber is waiting on an event — keep it alive, Signal() will re-queue
				// Decrement runningTasks so the arena/epoch accounting stays correct;
				// Signal's Push() will re-increment it when the task resumes.
				currentFiber = nullptr;
				currentRunningTask = nullptr;
			} else {
				// Fiber ran to completion (FiberEntryWrapper marked it DEAD)
				SharedQueues::fiberPool->Release(f);
				task_to_run->assignedFiber = nullptr;
				currentFiber = nullptr;
				currentRunningTask = nullptr;
			}

			if (is_handling_fork)
			{
				if (qIndex < SharedQueues::immediateCoresInUse.size()) {
					SharedQueues::immediateCoresInUse[qIndex]->store(false, std::memory_order_release);
				}
				is_handling_fork = false;
			}
		
			if (SharedQueues::runningTasks.fetch_sub(1, std::memory_order_acq_rel) == 1) {
				// We are the LAST thread to finish! 
				// It is safe for us to retire the arena.
				OnFinishedArena(&SharedQueues::taskArena);
			}	
			if (retired.size() > 512) { // Only pay the mutex cost once every 512 objects
				T_Threads::EpochManager::Instance().Tick();
			}
		}
	}
	running.store(false, std::memory_order_release);
	cvWorkerDone.notify_all();
}
