#pragma once
#include "Context.h"
#include "platform.h"
#include <atomic>
namespace T_Threads {
	enum class FiberStatus {
		READY,      // In the work queue, waiting to be stolen/run
		RUNNING,    // Currently assigned to a thread
		SUSPENDED,  // Waiting for an event/IO, not in any work queue
		DEAD        // Finished, pending cleanup/reclamation
	};
	struct alignas(16) Fiber {
		Context ctx;

		void* stackBase;
		size_t stackSize;
		std::atomic<FiberStatus>  status;
		void (*taskFunction)();
		Fiber() : stackBase(nullptr), stackSize(0), taskFunction(nullptr), status(FiberStatus::READY) {}
		Fiber(Fiber&& other) noexcept
			: ctx(other.ctx), stackBase(other.stackBase), stackSize(other.stackSize),
			  taskFunction(other.taskFunction), status(other.status.load(std::memory_order_relaxed)) {}
		Fiber& operator=(Fiber&&) = delete;
		Fiber(const Fiber&) = delete;
		Fiber& operator=(const Fiber&) = delete;
		void Init(void (*entryPoint)());
		void CoYield();    // Swaps back to scheduler                            
		void Suspend();  // Moves from RUNNING -> SUSPENDED
		void Resume();   // Moves from SUSPENDED -> READY

		// Safety check for the work-stealer
		bool IsReady() const { return status == FiberStatus::READY; }
	};
} // namespace T_Threads