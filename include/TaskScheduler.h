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
		void ParallelFor(int start, int end, int chunkSize, std::function<void(int, int)> func);
		void ParallelForNB(int start, int end, int chunkSize, std::function<void(int, int)> func);
		bool Push(Task* task);
		bool Push(uint8_t cpu_affinity, Task* task);
		bool PushPQ(Task* task);
		bool PushPQ(uint8_t priority, Task* task);
		bool PushFork(uint8_t cpu_affinity, Task* task);
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
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void Push(F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushLocal(t);
		}
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void Push(uint8_t cpu_affinity, F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushLocal(t, cpu_affinity);
		}
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void PushPQ(F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushToPQ(t);
		}
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void PushPQ(uint8_t priority, F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushToPQ(t, priority);
		}
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void PushFork(size_t coreID, F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushToCore(coreID, t);
		}
		void* AllocateFromArena(size_t size);

	private:

		void StartPool(size_t poolSize);
		bool PushLocal(Task* task, uint8_t cpuaffinity = 0);
		bool PushToPQ(Task* task, uint8_t priority = 3);
		bool PushToCore(size_t core_id, Task* task);
		TaskScheduler(size_t poolSize = std::thread::hardware_concurrency() - 1);
		int PickNextWorker();

		std::atomic<bool> poolActive{ false };
		std::atomic<int> nextWorker{ 0 };
		std::atomic<bool> stopFlag{ false };
		std::atomic<int> nextIndex{ -1 };
		std::vector<std::shared_ptr<T_Thread>> workers;
		MPSCQueue<Task*> mainQ;
		std::mutex testMutex;

		std::condition_variable cv;
		std::mutex poolMutex;
		std::mutex workerMutex;
	};
};
