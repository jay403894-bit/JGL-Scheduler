#pragma once
#define NOMINMAX
#include "T_Thread.h"
#include "SharedQueues.h"
#include "Task.h"
#include "MPSCQueue.h"

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
		void StartPool();
		bool SubmitLocal(Task* task);
		bool SubmitLocal(uint8_t cpu_affinity, Task* task);
		bool SubmitPQ(Task* task);
		bool SubmitPQ(uint8_t priority, Task* task);
		bool SubmitFork(uint8_t cpu_affinity, Task* task);
		void Pause();
		void Resume();
		void Stop(Task* worker_task); 
		void CollectGarbage();
		void WaitAll(const std::vector<Task*>& tasks);

		template <class F, std::enable_if_t<!std::is_same_v<std::decay_t<F>, Task*>, int> = 0>
		void SubmitLocal(F&& f) {
			Task* t = new LambdaTask(std::forward<F>(f));
			PushLocal(t);
		}
		template <class F, std::enable_if_t<!std::is_same_v<std::decay_t<F>, Task*>, int> = 0>
		void SubmitLocal(uint8_t cpu_affinity, F&& f) {
			Task* t = new LambdaTask(std::forward<F>(f));
			PushLocal(t, cpu_affinity);
		}
		template <class F, std::enable_if_t<!std::is_same_v<std::decay_t<F>, Task*>, int> = 0>
		void SubmitPQ(F&& f) {
			Task* t = new LambdaTask(std::forward<F>(f));
			Push(t);
		}
		template <class F, std::enable_if_t<!std::is_same_v<std::decay_t<F>, Task*>, int> = 0>
		void SubmitPQ(uint8_t priority, F&& f) {
			Task* t = new LambdaTask(std::forward<F>(f));
			Push(t, priority);
		}
		template <class F, std::enable_if_t<!std::is_same_v<std::decay_t<F>, Task*>, int> = 0>
		void SubmitFork(size_t coreID, F&& f) {
			Task* t = new LambdaTask(std::forward<F>(f));
			PushToCore(coreID, t);
		}

	private:
		bool PushLocal(Task* task, uint8_t cpuaffinity = 0);
		bool Push(Task* task, uint8_t priority = 3);
		bool PushToCore(size_t core_id, Task* task);
		TaskScheduler();
		int PickNextWorker();

	
		std::atomic<bool> pool_active_{ false };
		std::atomic<int> next_worker_{ 0 };
		std::atomic<bool> stop_flag_{ false };
		std::atomic<int> next_index_{ -1 };
		std::vector<std::shared_ptr<T_Thread>> workers_;
		MPSCQueue<Task*> main_queue_;

		std::condition_variable cv_;
		std::thread worker_thread_;
		std::mutex pool_mutex_;
		std::mutex worker_mutex_;
		std::atomic<int> cycles_since_switched{ 0 };

	};
};
