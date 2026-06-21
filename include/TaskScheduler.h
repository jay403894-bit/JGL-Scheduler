#pragma once
#define NOMINMAX
#include "T_Thread.h"
#include "SharedQueues.h"
#include "Task.h"
#include "MPSCQueue.h"
#include "Arena.h"
#include "Epochs.h"

namespace T_Threads {
	class TaskScheduler {
	public:
		static TaskScheduler& Instance() {
			static TaskScheduler instance;
			return instance;
		}
		~TaskScheduler();
		bool EnqueueToMain(Task* task);
		void ProcessMainThread();
		void Join();
		void NotifyAll();
		bool SubmitLocal(Task* task);
		bool SubmitLocal(uint8_t cpu_affinity, Task* task);
		bool SubmitPQ(Task* task);
		bool SubmitPQ(uint8_t priority, Task* task);
		bool SubmitFork(uint8_t cpu_affinity, Task* task);
		void Pause();
		void Resume();
		void Stop(Task* worker_task); 
		void Wait(const std::vector<Task*>& tasks);
		Arena* GetArena();
		void WaitAll();

		Task* CreateTask(void(*fn)(void*), void* data);

		template<typename F>
		LambdaTask<F>* CreateTask(F&& f) {
			// 2. Allocate space for the specific LambdaTask<F> instance
			void* mem = SharedQueues::taskArena.GetActive()->allocate(sizeof(LambdaTask<F>));
			if (!mem) return nullptr;

			// 3. Construct the LambdaTask<F> directly into that memory
			LambdaTask<F>* task = new (mem) LambdaTask<F>(std::forward<F>(f));
			return task;
		}
		template <class F, std::enable_if_t<!std::is_same_v<std::decay_t<F>, Task*>, int> = 0>
		void SubmitLocal(F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushLocal(t);
			SharedQueues::runningTasks.fetch_add(1, std::memory_order_relaxed);
		}
		template <class F, std::enable_if_t<!std::is_same_v<std::decay_t<F>, Task*>, int> = 0>
		void SubmitLocal(uint8_t cpu_affinity, F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushLocal(t, cpu_affinity);
			SharedQueues::runningTasks.fetch_add(1, std::memory_order_relaxed);
		}
		template <class F, std::enable_if_t<!std::is_same_v<std::decay_t<F>, Task*>, int> = 0>
		void SubmitPQ(F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			Push(t);
			SharedQueues::runningTasks.fetch_add(1, std::memory_order_relaxed);
		}
		template <class F, std::enable_if_t<!std::is_same_v<std::decay_t<F>, Task*>, int> = 0>
		void SubmitPQ(uint8_t priority, F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			Push(t, priority);
			SharedQueues::runningTasks.fetch_add(1, std::memory_order_relaxed);
		}
		template <class F, std::enable_if_t<!std::is_same_v<std::decay_t<F>, Task*>, int> = 0>
		void SubmitFork(size_t coreID, F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushToCore(coreID, t);
			SharedQueues::runningTasks.fetch_add(1, std::memory_order_relaxed);
		}
		void* AllocateFromArena(size_t size);

	private:

		void StartPool(size_t poolSize);
		bool PushLocal(Task* task, uint8_t cpuaffinity = 0);
		bool Push(Task* task, uint8_t priority = 3);
		bool PushToCore(size_t core_id, Task* task);
		TaskScheduler(size_t poolSize = std::thread::hardware_concurrency() - 1);
		int PickNextWorker();

		std::atomic<bool> poolActive{ false };
		std::atomic<int> nextWorker{ 0 };
		std::atomic<bool> stopFlag{ false };
		std::atomic<int> nextIndex{ -1 };
		std::vector<std::shared_ptr<T_Thread>> workers;
		MPSCQueue<Task*> mainQ;

		std::condition_variable cv;
		std::mutex poolMutex;
		std::mutex workerMutex;
	};
};
