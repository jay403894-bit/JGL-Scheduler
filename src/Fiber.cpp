#include "../include/Fiber.h"
#include "../include/T_Thread.h"
#include "../include/TaskScheduler.h"
using namespace T_Threads;
void Fiber::Init(void(*entryPoint)())
{
	uintptr_t* stackPtr = (uintptr_t*)((uintptr_t)((char*)stackBase + stackSize) & ~0xF);

	// Windows x64: RSP must be 8 mod 16 at function entry. 'ret' pops 8 bytes,
	// so the entryPoint slot must sit at an address that is 0 mod 16. stackTop is
	// 0 mod 16, so push a dummy first to put entryPoint at stackTop-16 (also 0 mod 16).
	// After ret: RSP = stackTop-8 (8 mod 16). Correct.
	*(--stackPtr) = 0;                      // alignment dummy
	*(--stackPtr) = (uintptr_t)entryPoint;  // ret address
	*(--stackPtr) = 0; // rbx
	*(--stackPtr) = 0; // rbp
	*(--stackPtr) = 0; // rdi
	*(--stackPtr) = 0; // rsi
	*(--stackPtr) = 0; // r12
	*(--stackPtr) = 0; // r13
	*(--stackPtr) = 0; // r14
	*(--stackPtr) = 0; // r15  <-- RSP points here after ContextSwitch loads it

	ctx.rsp = (void*)stackPtr;
}

void T_Threads::Fiber::CoYield() {
	this->status.store(FiberStatus::READY, std::memory_order_relaxed);

	// 1. Access the current thread
	auto* thread = T_Thread::self;

	// 2. Put the task back in the queue
	TaskScheduler::Instance().Push(currentRunningTask);

	// 3. Jump back to the thread's "Home Base" (schedulerCtx)
	ContextSwitch(&this->ctx, &thread->schedulerCtx);
}

void Fiber::Suspend() {
	// The fiber knows its own task
	Task* myTask = currentRunningTask;
	auto thread = T_Thread::GetCurrent();

	// Now perform the context switch out...
	ContextSwitch(&this->ctx, &thread->schedulerCtx);
}
void Fiber::Resume() {
	// 1. Atomically ensure we only resume if we are truly suspended
	FiberStatus expected = FiberStatus::SUSPENDED;
	if (this->status.compare_exchange_strong(expected, FiberStatus::READY, std::memory_order_release)) {

		// 2. Re-inject into the scheduler
		TaskScheduler::Instance().Push(currentRunningTask);
	}
}