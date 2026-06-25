#include "../include/Fiber.h"
#include "../include/T_Thread.h"
#include "../include/TaskScheduler.h"
using namespace T_Threads;
std::atomic<uint64_t> T_Threads::Fiber::idGenerator{ 0 };
void Fiber::Init(void(*entryPoint)())
{
	// 16-byte-align the very top of this fiber's stack.
	uintptr_t top = ((uintptr_t)((char*)stackBase + stackSize)) & ~(uintptr_t)0xF;
	uintptr_t* sp = (uintptr_t*)top;

	// Windows x64 ABI: the CALLER must leave 32 bytes of shadow space ABOVE the
	// return address for the callee to spill its register params. When
	// ContextSwitch 'ret's into entryPoint, that shadow space is whatever sits
	// above the entry RSP. Reserve it HERE, inside this fiber's own stack --
	// otherwise the entry function writes past stackTop, which is either the
	// next fiber's base (silent corruption) or, for the last fiber, unmapped
	// memory (0xC0000005 write AV at the stack-region boundary).
	sp -= 4;                              // 32 bytes shadow space (owned by this fiber)
	*(--sp) = 0;                          // landing slot: entry RSP points here (unused)
	*(--sp) = (uintptr_t)entryPoint;      // return address consumed by ContextSwitch 'ret'

	// 8 callee-saved registers consumed by ContextSwitch's pops (r15 is lowest).
	*(--sp) = 0; // rbx
	*(--sp) = 0; // rbp
	*(--sp) = 0; // rdi
	*(--sp) = 0; // rsi
	*(--sp) = 0; // r12
	*(--sp) = 0; // r13
	*(--sp) = 0; // r14
	*(--sp) = 0; // r15

	// 160 bytes for non-volatile XMM6-15 (10 * 16). ContextSwitch restores these
	// (movdqu) and then does `add rsp,160` BEFORE the pops, so this block must sit
	// below the GPR slots and ctx.rsp must point at its base. Zero-initialized;
	// a fresh fiber has no meaningful incoming XMM state.
	for (int k = 0; k < 20; ++k) *(--sp) = 0; // 20 * 8 = 160 bytes

	ctx.rsp = (void*)sp; // ContextSwitch loads RSP here (base of the XMM block)
}

void Fiber::CoYield() {
	// Record intent and switch out. Do NOT re-queue here: the task must not become
	// grabbable until our context is actually saved (during the switch below).
	// Pushing first lets another worker pick up the task and resume this fiber while
	// it's still running -- two workers, one stack. The worker re-queues us once it
	// regains control, by which point our context is saved.
	this->status.store(FiberStatus::WANTS_YIELD, std::memory_order_release);
	ContextSwitch(&this->ctx, this->homeCtx);
}

void Fiber::Suspend() {
	// Record intent and switch out. We must NOT publish SUSPENDED here: a racing
	// Resume() could flip it to READY and re-queue us before the ContextSwitch below
	// has saved our context. The worker promotes us to SUSPENDED only after we've
	// switched out -- the first moment a resume is actually safe.
	this->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
	ContextSwitch(&this->ctx, this->homeCtx);
}
void Fiber::Resume() {
	FiberStatus expected = FiberStatus::SUSPENDED;
	if (this->status.compare_exchange_strong(expected, FiberStatus::READY, std::memory_order_release)) {
		TaskScheduler::Instance().Requeue(this->owningTask);  // re-queue WITHOUT re-counting
	}
}