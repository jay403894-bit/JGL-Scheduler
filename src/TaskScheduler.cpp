#include "../include/TaskScheduler.h"

using namespace T_Threads;
TaskScheduler::TaskScheduler(size_t poolSize) {
	StartPool(poolSize);
};
TaskScheduler::~TaskScheduler() {
	if (!stopFlag) {
		Join();
	}
}
bool TaskScheduler::EnqueueToMain(Task* task)
{
	if (!poolActive) return false;
	if (!task)
		return false;
	mainQ.push(task);
	return true;
}
void TaskScheduler::ProcessMainThread()
{
	if (!poolActive) return;
	Task* t;
	while (mainQ.pop(t)) {
		if (!t) continue;
		t->Execute();
	}
}
void TaskScheduler::Join() {
	if (!poolActive) return;
	stopFlag = true;
	NotifyAll();
	for (auto& worker : workers) {
		worker->Join();
	}
	{
		std::lock_guard<std::mutex> lock(poolMutex);
		workers.clear();
		mainQ.clear();

		SharedQueues::immediateCoresInUse.clear();

	}
	poolActive.store(false, std::memory_order_release);
}

void T_Threads::TaskScheduler::NotifyAll()
{
	for (int i = 0; i < workers.size(); i++)
		workers[i]->NotifyWorker();
}
void T_Threads::TaskScheduler::ParallelFor(int start, int end, int chunkSize, std::function<void(int, int)> func)
{
	
	chunkSize = std::max(1, chunkSize);
	int totalItems = end - start;
	if (totalItems <= 0) return;

	int numTasks = (totalItems + chunkSize - 1) / chunkSize;

	std::vector<T_Threads::Task*> activeTasks;
	for (int i = 0; i < numTasks; ++i) {
		int chunkStart = start + i * chunkSize;
		int chunkEnd = std::min(chunkStart + chunkSize, end);

		auto t = CreateTask([=]() {
			func(chunkStart, chunkEnd);
			});
		Push(t);
		activeTasks.push_back(t);

	}
	NotifyAll();
	
	for (auto* t : activeTasks) {
		if (t == nullptr) continue;
		while (!t->complete.load(std::memory_order_acquire)) {
			std::this_thread::yield();
		}
	}
}
	

void T_Threads::TaskScheduler::ParallelForNB(int start, int end, int chunkSize, std::function<void(int, int)> func)
{
	chunkSize = std::max(1, chunkSize);
	int totalItems = end - start;
	if (totalItems <= 0) return;

	int numTasks = (totalItems + chunkSize - 1) / chunkSize;

	for (int i = 0; i < numTasks; ++i) {
		int chunkStart = start + i * chunkSize;
		int chunkEnd = std::min(chunkStart + chunkSize, end);

		this->Push([=]() {
			func(chunkStart, chunkEnd);
			});
	}
}
void TaskScheduler::StartPool(size_t poolSize) {
	std::lock_guard<std::mutex> lock(poolMutex);
	if (poolSize > std::thread::hardware_concurrency() - 1)
		poolSize = std::thread::hardware_concurrency() - 1;
	unsigned int hw = std::thread::hardware_concurrency();
	if (hw == 0)
		hw = 4; // or whatever default you prefer

	unsigned int maxWorkers = std::max(1u, hw - 1u);
	EpochManager::Instance().Init(maxWorkers);

	if (poolSize > maxWorkers)
		poolSize = maxWorkers;

	stopFlag.store(false, std::memory_order_release);
	nextWorker = 0;
	unsigned int num_workers = poolSize;

	workers.clear();
	SharedQueues::threadQs.clear();
	workers.reserve(num_workers);
	SharedQueues::threadQs.reserve(num_workers);
	SharedQueues::immediateCoresInUse.clear();
	SharedQueues::immediateCoresInUse.reserve(num_workers);

	for (unsigned int i = 0; i < num_workers; ++i) {
		SharedQueues::immediateCoresInUse.push_back(std::make_unique<std::atomic<bool>>(false));
		SharedQueues::threadQs.push_back(std::make_unique<TaskDeque>());
		SharedQueues::inboxes.push_back(std::make_unique<MPSCQueue<Task*>>());

	}
	for (unsigned int i = 0; i < num_workers; ++i) {
		auto worker = std::make_shared<T_Thread>();
		worker->SetQueueIndex(i);
		workers.push_back(worker);
		worker->StartWorker(i + 1);
	}
	for (int i = 0; i < workers.size(); i++)
	{
		while (!workers[i]->Ready()) {
			std::this_thread::yield();
		}
	}
	poolActive.store(true, std::memory_order_release);
}
bool TaskScheduler::Push(Task* task)
{
	return PushLocal(task);
}
bool TaskScheduler::Push(uint8_t cpu_affinity, Task* task)
{
	return PushLocal(task, cpu_affinity);
}
bool TaskScheduler::PushPQ(Task* task)
{
	return PushToPQ(task);
}
bool TaskScheduler::PushPQ(uint8_t priority, Task* task)
{
	return PushToPQ(task, priority);
}
bool TaskScheduler::PushFork(uint8_t cpu_affinity, Task* task)
{
	if (!task)
		return false;
	return Push(cpu_affinity, task);
}
void TaskScheduler::Pause() {
	SharedQueues::paused.store(true, std::memory_order_release);
}
void TaskScheduler::Resume() {
	SharedQueues::paused.store(false, std::memory_order_release);
	NotifyAll();
}
void TaskScheduler::Stop(Task* worker_task) {
	stopFlag.store(true, std::memory_order_release);
	worker_task->Stop();
}
void TaskScheduler::Wait(const std::vector<Task*>& tasks) {
	for (auto* t : tasks) {
		// A slightly better way to wait than just yield()
		while (!t->complete.load(std::memory_order_acquire)) {
			// This is "exponential backoff" - it reduces CPU usage while waiting
			std::this_thread::yield();
		}
	}
}
Arena* T_Threads::TaskScheduler::GetArena()
{
	return SharedQueues::taskArena.GetActive();
}
void TaskScheduler::WaitAll() {
	while (SharedQueues::runningTasks.load(std::memory_order_acquire) > 0) {
		std::this_thread::yield();
	}
}

Task* T_Threads::TaskScheduler::CreateTask(void(*fn)(void*), void* data)
{
	void* mem = SharedQueues::taskArena.GetActive()->allocate(sizeof(Task));
	if (!mem) return nullptr;
	return new (mem) Task(fn, data);

}
void* TaskScheduler::AllocateFromArena(size_t size) {
	return SharedQueues::taskArena.GetActive()->allocate(size);
}
bool TaskScheduler::PushLocal(Task* task, uint8_t cpuaffinity) {
	if (!task)
		return false;

	size_t num_workers = workers.size();

	if (cpuaffinity > 0 && (size_t)(cpuaffinity - 1) < num_workers) {
		size_t idx = (size_t)(cpuaffinity - 1);

		if (!SharedQueues::immediateCoresInUse[idx]->load(std::memory_order_acquire)) {
			SharedQueues::runningTasks.fetch_add(1, std::memory_order_relaxed);
			SharedQueues::inboxes[idx]->push(task);
			NotifyAll();
		}
		else
			return false;
	}
	else {
		SharedQueues::runningTasks.fetch_add(1, std::memory_order_relaxed);
		uint8_t chosen = PickNextWorker();
		SharedQueues::inboxes[chosen]->push(task);
		NotifyAll();
	}
	return true;
}
bool TaskScheduler::PushToPQ(Task* task, uint8_t priority)
{
	if (priority > 4)
		priority = 4;

	if (!task)
		return false;

	SharedQueues::runningTasks.fetch_add(1, std::memory_order_relaxed);
	if (!SharedQueues::proirityQ[priority].try_enqueue(task)) {
		SharedQueues::runningTasks.fetch_sub(1, std::memory_order_relaxed);
	}
	NotifyAll();
	return true;
}
bool TaskScheduler::PushToCore(size_t core_id, Task* task)
{
	if (core_id < 1) return false;
	if (!poolActive) return false;
	if (!task) return false;
	if (SharedQueues::immediateCoresInUse[core_id % workers.size()]->load(std::memory_order_acquire) || core_id < 1) return false;
	

	SharedQueues::runningTasks.fetch_add(1, std::memory_order_relaxed);
	size_t idx = (core_id - 1) % workers.size();
	SharedQueues::immediateCoresInUse[idx]->store(true, std::memory_order_release);
	workers[idx]->SetImmediateTask(task);
	workers[idx]->NotifyWorker();
	return true;
}
int TaskScheduler::PickNextWorker() {
	size_t n = workers.size();
	for (size_t i = 0; i < n; ++i) {
		size_t idx = (nextWorker + i) % n;
		// Skip workers that are busy with immediate tasks
		if (!SharedQueues::immediateCoresInUse[idx]->load(std::memory_order_acquire)) {
			nextWorker = (idx + 1) % n; // advance round-robin pointer
			return static_cast<int>(idx);
		}
	}
	// If all cores are busy, just pick the next in round-robin
	int fallback = static_cast<int>(nextWorker);
	nextWorker = (fallback + 1) % n;
	return fallback;
}

