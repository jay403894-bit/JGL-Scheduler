#include "../include/T_Thread.h"
#include "../include/TaskScheduler.h"
#include "../include/Event.h"
using namespace T_Threads;
TaskScheduler* TaskScheduler::instance = nullptr;

TaskScheduler::TaskScheduler(size_t poolSize) {
	StartPool(poolSize);
}
size_t TaskScheduler::GetSafeTC() {
	unsigned int cores = std::thread::hardware_concurrency();
	if (cores == 0) return 1;
	if (cores == 1) return 1;
	return static_cast<size_t>(cores - 1);
}
void TaskScheduler::Init(size_t poolSize) {
	if (instance != nullptr)
		throw std::runtime_error("TaskScheduler already initialized!");
	instance = new TaskScheduler(poolSize);
}
TaskScheduler::~TaskScheduler() {
	if (!stopFlag)
		Join();
}
bool TaskScheduler::EnqueueToMain(Task* task) {
	if (!poolActive) return false;
	if (!task) return false;
	mainQ.push(task);
	return true;
}
void TaskScheduler::ProcessMainThread() {
	if (!poolActive) return;
	Task* t;
	while (mainQ.pop(t)) {
		if (!t) continue;
		t->Execute();
	}
}
void TaskScheduler::Join() {
	if (!poolActive) return;

	stopFlag.store(true, std::memory_order_release);

	{
		std::lock_guard<std::mutex> lock(registryMtx);
		for (auto& pair : eventRegistry)
			pair.second->SignalAll();
	}
	NotifyAll();

	for (auto& worker : workers)
		worker->Join();

	{
		std::lock_guard<std::mutex> lock(poolMutex);
		workers.clear();
		mainQ.clear();
		immediateCoresInUse.clear();
	}

	poolActive.store(false, std::memory_order_release);
}
void TaskScheduler::NotifyAll() {
	for (auto& w : workers)
		w->NotifyWorker();
}
void TaskScheduler::ParallelFor(int start, int end, int chunkSize, std::function<void(int, int)> func) {
	chunkSize = std::max(1, chunkSize);
	int totalItems = end - start;
	if (totalItems <= 0) return;

	int numTasks = (totalItems + chunkSize - 1) / chunkSize;

	std::vector<Task*> activeTasks;
	for (int i = 0; i < numTasks; ++i) {
		int chunkStart = start + i * chunkSize;
		int chunkEnd = std::min(chunkStart + chunkSize, end);
		auto t = CreateTask([=]() { func(chunkStart, chunkEnd); });
		Push(t);
		activeTasks.push_back(t);
	}
	NotifyAll();

	for (auto* t : activeTasks) {
		if (!t) continue;
		while (!t->complete.load(std::memory_order_acquire))
			std::this_thread::yield();
	}
}
void TaskScheduler::ParallelForNB(int start, int end, int chunkSize, std::function<void(int, int)> func) {
	chunkSize = std::max(1, chunkSize);
	int totalItems = end - start;
	if (totalItems <= 0) return;

	int numTasks = (totalItems + chunkSize - 1) / chunkSize;
	for (int i = 0; i < numTasks; ++i) {
		int chunkStart = start + i * chunkSize;
		int chunkEnd = std::min(chunkStart + chunkSize, end);
		Push([=]() { func(chunkStart, chunkEnd); });
	}
}
void TaskScheduler::StartPool(size_t poolSize) {
	std::lock_guard<std::mutex> lock(poolMutex);

	if (poolSize == 0)
		poolSize = GetSafeTC();
	if (poolSize > GetSafeTC())
		poolSize = GetSafeTC();

	unsigned int num_workers = static_cast<unsigned int>(poolSize);
	EpochManager::Instance().Init(num_workers);
	stopFlag.store(false, std::memory_order_release);
	nextWorker = 0;

	const size_t fibersPerWorker = 8;
	fiberPool = std::make_unique<FiberPool>(num_workers * fibersPerWorker);

	workers.clear();
	threadQs.clear();
	immediateCoresInUse.clear();
	inboxes.clear();
	workers.reserve(num_workers);
	threadQs.reserve(num_workers);
	immediateCoresInUse.reserve(num_workers);
	inboxes.reserve(num_workers);

	for (unsigned int i = 0; i < num_workers; ++i) {
		immediateCoresInUse.push_back(std::make_unique<std::atomic<bool>>(false));
		threadQs.push_back(std::make_unique<TaskDeque>());
		inboxes.push_back(std::make_unique<MPSCQueue<Task*>>());
	}
	for (unsigned int i = 0; i < num_workers; ++i) {
		auto worker = std::make_shared<T_Thread>(*this);
		worker->SetQueueIndex(i);
		workers.push_back(worker);
		worker->StartWorker(i + 1);
	}
	for (auto& w : workers) {
		while (!w->Ready())
			std::this_thread::yield();
	}
	poolActive.store(true, std::memory_order_release);
}
void TaskScheduler::WaitOnEvent(const std::string& eventName) {
	auto* thread = T_Thread::GetCurrent();
	Task* myTask = thread->currentRunningTask;
	Fiber* myFiber = myTask->assignedFiber;

	auto& event = GetEvent(eventName);
	event.AddWaiter(myTask);

	ContextSwitch(&myFiber->ctx, &thread->schedulerCtx);
}
bool TaskScheduler::Push(Task* task) {
	return PushLocal(task);
}
bool TaskScheduler::Push(uint8_t cpu_affinity, Task* task) {
	return PushLocal(task, cpu_affinity);
}
bool TaskScheduler::PushPQ(Task* task) {
	return PushToPQ(task);
}
bool TaskScheduler::PushPQ(uint8_t priority, Task* task) {
	return PushToPQ(task, priority);
}
bool TaskScheduler::PushFork(uint8_t cpu_affinity, Task* task) {
	if (!task) return false;
	return Push(cpu_affinity, task);
}
Event& TaskScheduler::GetEvent(const std::string& name) {
	std::lock_guard<std::mutex> lock(registryMtx);
	if (eventRegistry.find(name) == eventRegistry.end())
		eventRegistry[name] = std::make_unique<Event>();
	return *eventRegistry[name];
}
void TaskScheduler::Pause() {
	paused.store(true, std::memory_order_release);
}
void TaskScheduler::Resume() {
	paused.store(false, std::memory_order_release);
	NotifyAll();
}
void TaskScheduler::Stop(Task* worker_task) {
	stopFlag.store(true, std::memory_order_release);
	worker_task->Stop();
}
Task* TaskScheduler::GetTask() {
	Task* task_to_run = nullptr;
	Task* task;

	for (int i = 0; i < 5; i++) {
		if (priorityQ[i].try_dequeue(task)) {
			task_to_run = task;
			break;
		}
	}
	if (!task_to_run) {
		size_t numThreads = threadQs.size();
		size_t start = rand() % numThreads;
		for (size_t i = 0; i < numThreads; ++i) {
			size_t target = (start + i) % numThreads;
			auto opt = threadQs[target]->steal();
			if (opt) {
				task_to_run = *opt;
				current_task = task_to_run;
				break;
			}
		}
	}
	return task_to_run;
}
void TaskScheduler::Wait(const std::vector<Task*>& tasks) {
	for (auto* t : tasks) {
		while (!t->complete.load(std::memory_order_acquire)) {
			Task* task = GetTask();
			if (task) {
				task->Execute();
				task->complete.store(true, std::memory_order_release);
			}
			else
				std::this_thread::yield();
		}
	}
}
void TaskScheduler::WaitAll() {
	while (runningTasks.load(std::memory_order_acquire) > 0)
		std::this_thread::yield();
}
Arena* TaskScheduler::GetArena() {
	return taskArena.GetActive();
}
Task* TaskScheduler::CreateTask(void(*fn)(void*), void* data) {
	void* mem = taskArena.GetActive()->allocate(sizeof(Task));
	if (!mem) return nullptr;
	return new (mem) Task(fn, data);
}
void* TaskScheduler::AllocateFromArena(size_t size) {
	return taskArena.GetActive()->allocate(size);
}
bool TaskScheduler::PushLocal(Task* task, uint8_t cpuaffinity) {
	if (!task) return false;

	size_t num_workers = workers.size();
	if (cpuaffinity > 0 && (size_t)(cpuaffinity - 1) < num_workers) {
		size_t idx = (size_t)(cpuaffinity - 1);
		if (!immediateCoresInUse[idx]->load(std::memory_order_acquire)) {
			runningTasks.fetch_add(1, std::memory_order_relaxed);
			inboxes[idx]->push(task);
			NotifyAll();
		}
		else
			return false;
	}
	else {
		runningTasks.fetch_add(1, std::memory_order_relaxed);
		uint8_t chosen = PickNextWorker();
		inboxes[chosen]->push(task);
		NotifyAll();
	}
	return true;
}
bool TaskScheduler::PushToPQ(Task* task, uint8_t priority) {
	if (priority > 4) priority = 4;
	if (!task) return false;

	runningTasks.fetch_add(1, std::memory_order_relaxed);
	if (!priorityQ[priority].try_enqueue(task))
		runningTasks.fetch_sub(1, std::memory_order_relaxed);
	NotifyAll();
	return true;
}
bool TaskScheduler::PushToCore(size_t core_id, Task* task) {
	if (core_id < 1) return false;
	if (!poolActive) return false;
	if (!task) return false;

	size_t idx = (core_id - 1) % workers.size();
	if (immediateCoresInUse[idx]->load(std::memory_order_acquire)) return false;

	runningTasks.fetch_add(1, std::memory_order_relaxed);
	immediateCoresInUse[idx]->store(true, std::memory_order_release);
	workers[idx]->SetImmediateTask(task);
	workers[idx]->NotifyWorker();
	return true;
}
int TaskScheduler::PickNextWorker() {
	size_t n = workers.size();
	for (size_t i = 0; i < n; ++i) {
		size_t idx = (nextWorker + i) % n;
		if (!immediateCoresInUse[idx]->load(std::memory_order_acquire)) {
			nextWorker = (idx + 1) % n;
			return static_cast<int>(idx);
		}
	}
	int fallback = static_cast<int>(nextWorker);
	nextWorker = (fallback + 1) % n;
	return fallback;
}
