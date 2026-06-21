#include "../include/T_Thread.h"
#include "../include/platform.h"

#include <iostream>
using namespace T_Threads;
T_Thread::T_Thread() {
}
T_Thread::~T_Thread() {
}
void T_Thread::StartWorker(size_t cpu_affinity)
{
	auto ready = std::make_shared<std::atomic<bool>>(false);
	thread = std::thread([this, ready]() {
		while (!ready->load(std::memory_order_acquire)) std::this_thread::yield();
		this->Worker();
		});
	thread_id = thread_counter.fetch_add(1) + 1;
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
		EpochManager::Instance().EnterEpoch(T_Threads::thread_id);
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

		int inbox_drain_count = 0;
		while (inbox->pop(t)) {
			if (++inbox_drain_count > 100000) {
				std::cerr << "[worker " << qIndex << "] ERROR: inbox drain looping >100k times! Aborting.\n";
				break;  
			}

			if (!t) {
				std::cerr << "[worker " << qIndex << "] ERROR: popped null from inboxes_\n";
				continue;
			}
			qLoad.fetch_add(1, std::memory_order_relaxed);

			if (inbox_drain_count % 100 == 0) {
				std::cerr << "[worker " << qIndex << "] inbox drain #" << inbox_drain_count << " task " << (void*)t << "\n";
			}

			if (local_deque->push_bottom(t)) {
				continue; 
			}

			std::cerr << "[worker " << qIndex << "] FAILED push_bottom for task " << (void*)t << "\n";

			if (!SharedQueues::proirityQ[0].try_enqueue(t)) {
				std::cerr << "[worker " << qIndex << "] WARNING: task " << (void*)t << " overflow, stashing locally\n";
				overflow.push_back(t);
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
				for (size_t i = 0; i < SharedQueues::threadQs.size(); ++i) {
					if (i == qIndex) continue;
					auto opt = SharedQueues::threadQs[i]->steal();
					if (opt) {
						task_to_run = *opt;
						current_task = task_to_run;
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
			task_to_run->execute();
			SharedQueues::graveyard.push(task_to_run);

			if (is_handling_fork)
			{
				if (qIndex < SharedQueues::immediateCoresInUse.size()) {
					SharedQueues::immediateCoresInUse[qIndex]->store(false, std::memory_order_release);
				}
				is_handling_fork = false;
			}
			task = nullptr;
			current_task = nullptr;
			task_to_run = nullptr;
			if (SharedQueues::runningTasks.fetch_sub(1, std::memory_order_acq_rel) == 1) {
				// We are the LAST thread to finish! 
				// It is safe for us to retire the arena.
				OnFinishedArena(&SharedQueues::taskArena);
			}	
		}
		EpochManager::Instance().LeaveEpoch(T_Threads::thread_id);
	}
	running.store(false, std::memory_order_release);
	cvWorkerDone.notify_all();
}
