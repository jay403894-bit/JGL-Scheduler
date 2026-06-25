#include "../include/GlobalFiberPool.h"
#include "../include/T_Thread.h"
using namespace T_Threads;

T_Threads::GlobalFiberPool::GlobalFiberPool(size_t standardCount, size_t heavyCount)
	: standardArena(standardCount * 64 * 1024),  // 64 KB per standard fiber
	heavyArena(heavyCount * 512 * 1024)        // 512 KB per heavy fiber
{
	// Initialize standard fibers
	standardFibers.reserve(standardCount);
	for (size_t i = 0; i < standardCount; ++i) {
		void* stackMem = standardArena.AllocateStack(64 * 1024);
		if (!stackMem) {
			throw std::runtime_error("Failed to allocate standard fiber stack");
		}
		standardFibers.emplace_back();
		Fiber& f = standardFibers.back();
		f.stackBase = stackMem;
		f.stackSize = 64 * 1024;
		availableFibers.push_back(&f);
	}

	// Initialize heavy fibers
	heavyFibers.reserve(heavyCount);
	for (size_t i = 0; i < heavyCount; ++i) {
		void* stackMem = heavyArena.AllocateStack(512 * 1024);
		if (!stackMem) {
			throw std::runtime_error("Failed to allocate heavy fiber stack");
		}
		heavyFibers.emplace_back();
		Fiber& f = heavyFibers.back();
		f.stackBase = stackMem;
		f.stackSize = 512 * 1024;
		availableFibers.push_back(&f);
	}
}

GlobalFiberPool* T_Threads::GlobalFiberPool::Create(size_t standardCount, size_t heavyCount)
{
	return new GlobalFiberPool(standardCount, heavyCount);
}

std::vector<Fiber*> T_Threads::GlobalFiberPool::StealBatch(size_t count)
{
	std::lock_guard<std::mutex> lock(poolMutex);
	std::vector<Fiber*> batch;

	while (count > 0 && !availableFibers.empty()) {
		batch.push_back(availableFibers.back());
		availableFibers.pop_back();
		count--;
	}
	return batch;
}

void T_Threads::GlobalFiberPool::ReturnBatch(std::vector<Fiber*>& batch)
{

	if (batch.empty()) return;

	std::lock_guard<std::mutex> lock(poolMutex);

	if (availableFibers.size() + batch.size() > availableFibers.capacity()) {
		availableFibers.reserve(availableFibers.size() + batch.size());
	}

	availableFibers.insert(
		availableFibers.end(),
		std::make_move_iterator(batch.begin()),
		std::make_move_iterator(batch.end())
	);

	batch.clear();

}

void T_Threads::GlobalFiberPool::FiberEntryWrapper()
{
	Fiber* self = T_Thread::GetCurrent()->currentFiber;
	Task*  task = T_Thread::GetCurrent()->currentRunningTask;

	if (self && task) {
		task->Execute();
	}

	self->status.store(FiberStatus::DEAD, std::memory_order_release);


	ContextSwitch(&self->ctx, self->homeCtx);
}

size_t T_Threads::GlobalFiberPool::AvailableCount() const
{

	std::lock_guard<std::mutex> lock(poolMutex);
	return availableFibers.size();

}
