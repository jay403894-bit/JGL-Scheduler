#include "../include/FiberPool.h"
#include "../include/TaskScheduler.h"
using namespace T_Threads;
FiberPool::FiberPool(int count) : arena(count * 1024 * 1024) {

	allFibers.resize(count);
	for (int i = 0; i < count; ++i) {
		// Allocate the stack from the arena
		void* stackMem = arena.AllocateStack(1024 * 1024);

		// Initialize the Fiber
		allFibers[i].stackBase = stackMem;
		allFibers[i].stackSize = 1024 * 1024;

		// Add to the free stack
		freeStack.Push(&allFibers[i]);
	}
}
Fiber* FiberPool::Acquire()
{

	Fiber* f = nullptr;
	if (freeStack.Pop(f, EpochManager::Instance().CurrentEpoch())) {
		return f;
	}
	return nullptr;
}

void T_Threads::FiberPool::Release(Fiber* f)

{
	freeStack.Push(f);
}

void T_Threads::FiberPool::SwitchBackToScheduler()
{         // Find the current thread's scheduler context
			// (You might need a thread_local pointer to the current T_Thread)
	auto* self = T_Thread::GetCurrent();

	// Switch from the Fiber's context back to the worker loop
	ContextSwitch(&self->currentFiber->ctx, &self->schedulerCtx);
}

void T_Threads::FiberPool::FiberEntryWrapper()
{   
	// 1. Get the worker thread that spawned this fiber
	auto* thread = T_Thread::GetCurrent();

	if (thread->currentFiber && thread->currentRunningTask) {
		thread->currentRunningTask->Execute();
	}
	// Mark DEAD before returning so Worker knows the task completed (not suspended)
	if (thread->currentFiber)
		thread->currentFiber->status.store(FiberStatus::DEAD, std::memory_order_release);
	ContextSwitch(&thread->currentFiber->ctx, &thread->schedulerCtx);
}
