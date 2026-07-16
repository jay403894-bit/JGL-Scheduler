# 🧵 JLib::TaskScheduler

An advanced, low-overhead C++17 cooperative runtime engine. By leveraging custom assembly/C++ fibers, work-stealing deques, and a slab-allocated task DAG, it abstracts physical CPU topology into a high-performance, starvation-free execution matrix designed for real-time engines.

---

## 📋 Table of Contents

1. [Execution Paradigm & Component Map](#1-execution-paradigm--component-map)
2. [Starvation Prevention](#2-starvation-prevention)
3. [Task Execution Modalities](#3-task-execution-modalities)
4. [Critical Integration Contracts](#4-critical-integration-contracts)
5. [Core API & Workflow Architectures](#5-core-api--workflow-architectures)
6. [Synchronization & Memory Safety](#6-synchronization--memory-safety)
7. [JLib::TaskDAG](#7-jlibtaskdag)

---

## 🏗️ 1. Execution Paradigm & Component Map

The scheduler maps physical hardware cores to logical workers, utilizing specialized allocation and queuing mechanisms to eliminate system call overhead.

### Topology Awareness
On `Initialize()`, the system maps the host CPU's Last Level Cache (LLC) clusters and SMT (Hyper-threaded) siblings. Work-stealing operations prioritize columns within the same cache domain before reaching across hardware boundaries.

### Slab Allocator & Local Caches
Standard `new` and `delete` operations are explicitly deleted from Task allocations. Tasks are provisioned out of a custom thread-local slab allocator, ensuring perfect cache alignment and zero runtime heap fragmentation.

### Dual-Queue Priorities
Each worker maintains split high/low priority deques. Work is stolen with hierarchical preference: same-core siblings → LLC-local peers → global random steal. Low-priority tasks receive fair CPU access through starvation prevention (see section 2).

---

## 🛡️ 2. Starvation Prevention

The scheduler implements three complementary mechanisms to ensure no priority level starves indefinitely:

### Age-Based Promotion
**What it does:** Low-priority tasks waiting in queue > 50ms are automatically promoted to high-priority.

**Why it matters:** Without aging, a continuous flood of high-priority work could starve low-priority background tasks forever.

**Implementation:**
- `taskQueuedTime` map tracks when each loPri task was pushed
- `GetTaskBatch()` checks age when stealing; old tasks get promoted
- Timestamps cleaned up on task completion

```cpp
// Automatic: when a loPri task waits > 50ms, it gets promoted
size_t age = now - taskQueuedTime[task];
if (age > kAgePromotionThresholdMs) {
    task->hiPri = 1;  // promoted
}
```

### Steal Fairness
**What it does:** After 8 consecutive hiPri steals, the scheduler forces a loPri scan.

**Why it matters:** Without fairness, stealing logic could scan hiPri queues indefinitely while loPri work sits untouched.

**Implementation:**
- `consecutiveHiPriSteals` counter tracks the streak
- After threshold, forces one loPri scan before resuming hiPri preference
- Resets counter whenever loPri work is actually stolen

```cpp
// Force fairness every N steals
if (consecutiveHiPriSteals >= kStealFairnessWindow) {
    // Scan loPri queues this round
    consecutiveHiPriSteals = 0;
}
```

### Priority Inheritance (SchedulerMutex)
**What it does:** When a high-priority task tries to acquire a lock held by a low-priority task, the lock holder's priority is temporarily boosted.

**Why it matters:** Without inheritance, priority inversion deadlock can occur: hiPri task waits on loPri holder; loPri starves while waiting; hiPri never proceeds.

**Implementation:**
- `SchedulerMutex` wrapper around `std::mutex`
- On `lock()` contention, boosts the current holder
- On `unlock()`, restores original priority
- Uses `Thread::GetCurrent()->currentRunningTask` for fiber-aware tracking

```cpp
// Safe lock that prevents priority inversion
SchedulerMutex lock;

lock.lock();    // Auto-boosts holder if contended
// critical section
lock.unlock();  // Restores priority
```

---

## 🎮 3. Task Execution Modalities

The scheduler operates two distinct execution pathways. **Selecting the wrong pathway will result in immediate deadlocks or queue corruption.**

| Execution Mode | fastJob | Allocation | Thread Model | Use Case |
|---|---|---|---|---|
| **Standard Task** | `true` (Default) | Raw Thread Loop | Non-Cooperative | Bulk math, raycasts, data sweeps |
| **Fiber Task** | `false` | Custom ASM/C++ Fiber Stack | Cooperative | Fork-join patterns, wait-able work |

---

## 🚦 4. Critical Integration Contracts

### Contract 1: The Fork-Join / PushFork Rule

When employing a fork-join parallelism architecture, tasks must cooperatively yield their execution contexts during wait cycles rather than blocking the physical thread.

**The Rule:** You **MUST** pass `fastJob = false` inside `CreateTask` when pushing to `PushFork`.

**The Trap:** If a task enters `PushFork` with `fastJob = true`, the scheduler runs it as a standard thread-bound job. When that job calls `WaitFor()`, it will attempt to execute fiber suspension mechanics on a naked thread, resulting in an immediate hard deadlock.

### Contract 2: Long-Running Services vs. Immediate Mode

`PushImmediate(cpu_affinity, task)` strips a worker thread from the general pool, forcing it to offload its current queue to neighboring cores and lock onto a dedicated loop.

**The Rule:** Service tasks (e.g., Audio processing loops, Networking listeners) launched via `PushImmediate` must be configured with `fastJob = true`.

**The Trap:** Immediate-mode tasks are structurally isolated from the fiber scheduling pool. If an immediate task triggers a fiber suspend/resume sequence, the synchronization state tracking the active worker queue boundaries will break down, causing metadata corruption.

---

## 🚀 5. Core API & Workflow Architectures

### The Fork-Join Pattern (Fibers)

Used when a parent task must spin up sub-tasks and wait for them to finish before proceeding.

```cpp
// 1. Instantiate the coordination tracker
JLib::WaitGroup wg;

// 2. Provision tasks via the Slab Allocator (fastJob MUST be false for Fibers)
Task* parent = scheduler.CreateTask([]() { /* ... */ }, hiPri, FiberSize, false);

// Increment counter BEFORE dispatching
wg.Increment(1); 
scheduler.PushFork(parent);

// 3. Suspend current context until worker execution concludes
scheduler.WaitFor(wg);
```

### The Immediate Mode Pattern (Threads)

Used to pin critical engine subsystems to specific CPU cores.

```cpp
// Service tasks run raw on the thread (fastJob MUST be true)
Task* audioService = scheduler.CreateTask([]() {
    while(engineRunning) {
        UpdateAudioBuffers();
        // Uses OS sleeps or custom atomics, NEVER fiber yields!
    }
}, hiPri, FiberSize::Standard, true);

// Evicts core work and locks the thread to this execution loop
scheduler.PushImmediate(CoreAffinity::Mask_Core2, audioService);
```

---

## 🛡️ 6. Synchronization & Memory Safety

### Priority Inheritance (SchedulerMutex)

Use `SchedulerMutex` instead of `std::mutex` when a lock might be held by a low-priority task while a high-priority task waits on it.

```cpp
// Instead of: std::mutex lock;
SchedulerMutex lock;

// In your fiber-executed task:
lock.lock();
{
    // Critical section
    // If another task contends on this lock, the lock holder's priority
    // is temporarily boosted to prevent starvation of this task
}
lock.unlock();
```

**When to use:**
- ✅ Locks shared between hiPri and loPri tasks
- ✅ Resource pools where hiPri work might wait on loPri holders
- ❌ Scheduler-internal locks (already fast, no fiber wait)
- ❌ Locks only used within hiPri tasks (no inversion risk)

### Memory Lifecycle Ownership

You **never** call `delete` on a task. Once a task completion gate resolves, the scheduler automatically returns its memory layout block to the thread-local slab allocation pool. This is enforced: `operator delete` is explicitly deleted in the `Task` class.

---

## 📊 7. JLib::TaskDAG

The TaskDAG manages complex, multi-threaded task dependencies. It provides structural synchronization across your worker threads, allowing you to establish explicit execution orders (e.g., ensuring Physics updates finish before Rendering commands are submitted) without using blocking mutexes or thread-stalling primitives.

### 🧠 Memory Architecture & Cache Line Optimization

To maximize execution throughput, TaskDAG acts as a zero-allocation database manager during runtime:

**The 64-Byte Cache Target:** Standard task graphs often bloat tasks by attaching completion callback variables. TaskDAG eliminates this bloat entirely. Completion hooks live embedded in the `TaskNode` itself (the node doubles as the trampoline's context and always outlives it), so the core `Task` struct is optimized down to exactly 64 bytes and firing a node performs **zero heap allocations** — nodes come from the slab, and nothing else is allocated. This guarantees that a task payload matches a CPU cache line perfectly, eliminating cache line splitting.

**Epoch-Based Reclamation (EBR):** Nodes are not deleted using standard `free()` hooks. When a task completes, the static `NodeDeleter` registers it with an EBR pipeline, cleanly returning its slot back to a dedicated thread-safe slab allocator.

**Transient Tracking Vector (nodes):** The internal tracking vector is optimized strictly for a single-threaded build phase. It is used to discover roots and execute cycle checking, then it is cleared completely inside `Submit()`. Post-submission, the graph is entirely decentralized; nodes are autonomous and self-free upon execution.

### 🔀 Node Typologies & Logical Gates

The DAG framework provides three specialized execution nodes created via the factory interface:

**1. Standard Worker Nodes (`CreateNode`)**
- **Behavior:** Distributed broadly across the general work-stealing thread pool.
- **Affinity Control:** Supports optional priority levels and explicit `cpu_id` hardware pinning tags.

**2. Main-Thread Affinitized Nodes (`CreateMainNode`)**
- **Behavior:** Routes tasks exclusively to `TaskScheduler::PushMain`. These tasks will only execute when the primary thread explicitly runs `ProcessMainThread`.
- **Critical Rule:** Whoever awaits a graph containing a main-thread node **MUST** use `TaskScheduler::WaitForMain`. Calling the standard `WaitFor` will trap worker threads while the main-thread node sits un-pumped in the queue, resulting in a permanent engine hang.

**3. Logic Gates (`CreateGate`)**
- **Purpose:** A structural control node that carries no computational task payload. Instead, it serves as a conditional gate to evaluate complex boolean readiness states.
- **AND Gates:** The node remains locked until every single dependency broadcasts a completion signal.
- **OR Gates:** The node triggers the exact moment the first dependency finishes, short-circuiting the remaining timeline and releasing downstream dependents immediately.
- **Composition:** Gates can be nested to construct complex conditional pipelines (e.g., evaluating `(System_A && System_B) || System_C`).

### 🔄 Lifecycle & Execution Pipeline

The lifecycle of an engine frame's task graph progresses through four strict phases:

```
[ Build Phase ] ──► [ HasCycle Check ] ──► [ Submit() ] ──► [ Trampoline Core Loop ]
(Add Dependencies)   (Kahn's Validation)    (Fire Roots)     (OnTaskFinishedWrapper)
```

**Phase 1: The Build Phase**
You instantiate your execution units and establish directional boundaries using `AddDependency(dependent, dependency)`.

**Phase 2: Structural Validation (HasCycle)**
Before execution begins, the system executes an offline Kahn's Algorithm Cycle Check. It walks the graph topology to confirm that no circular references exist (e.g., Node A waiting for Node B, which is waiting for Node A). If a cycle is detected, the graph fails validation safely, preventing an in-flight thread freeze.

**Phase 3: The Launch (Submit)**
Calling `Submit()` triggers validation. If successful, the engine discovers all "root" nodes (nodes with 0 incoming dependencies) and invokes `Fire()`.

**Phase 4: The Trampoline Loop (OnTaskFinishedWrapper)**
When a node runs, the DAG intercepts its execution using a static trampoline hook:
1. `Fire()` replaces the task's function pointer with `OnTaskFinishedWrapper`
2. The worker thread executes the node's actual stored payload (`origFn(origData)`)
3. The moment the math finishes, it calls `OnTaskFinished()`, which atomically decrements child dependency counters
4. If a child's logic rules are satisfied, it immediately calls `Fire()` on that child, cascading work through the pool

### 💻 Integration Example

```cpp
// 1. Instantiate the graph container bound to your engine scheduler
JLib::TaskDAG graph(engineScheduler);

// 2. Provision raw tasks out of the slab allocator
Task* inputTask   = engineScheduler.CreateTask(UpdateInput, hiPri, FiberSize::Standard, true);
Task* physicsTask = engineScheduler.CreateTask(IntegratePhysics, hiPri, FiberSize::Standard, true);
Task* renderTask  = engineScheduler.CreateTask(SubmitRenderCommands, hiPri, FiberSize::Standard, true);

// 3. Wrap tasks into DAG Nodes
TaskNode* inputNode   = graph.CreateNode(inputTask);
TaskNode* physicsNode = graph.CreateNode(physicsTask);

// The render submission must run on the main thread due to graphics context locking
TaskNode* renderNode  = graph.CreateMainNode(renderTask);

// 4. Construct the Dependency Matrix
// Render waits for BOTH Input and Physics to resolve (Implicit AND behavior)
graph.AddDependency(renderNode, inputNode);
graph.AddDependency(renderNode, physicsNode);

// 5. Kick off the pipeline
// This validates the graph structure, fires the roots, and safely recycles nodes.
bool success = graph.Submit();
if (!success) {
    // Handle cycle detection or validation failure
}
```

---

## 🚀 Getting Started

```cpp
#include "TaskScheduler.h"

int main() {
    // Initialize with auto-detected core count
    JLib::TaskScheduler::Init();
    auto& sched = JLib::TaskScheduler::Instance();
    
    // Create and push a high-priority fiber task
    auto* task = sched.CreateTask([]() {
        std::cout << "Running on fiber!\n";
    }, 1, JLib::FiberSize::Standard, false);  // hiPri, not fastJob
    
    sched.Push(task);
    sched.WaitAll();
    
    return 0;
}
```

---

**Built for real-time engines. Proven under concurrent load.**
