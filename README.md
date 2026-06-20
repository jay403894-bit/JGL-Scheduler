# T_Threads

A high-performance, lightweight lockless-ish C++ task scheduler with thread affinity and work-stealing queues.

## Contribution

Feel free to explore and experiment with the scheduler. Pull requests or ideas welcome for improvements.

## Features

- **Thread Pool:** Efficient management of worker threads.
- **Local Queues:** Tasks can be scheduled to specific threads for cache locality.
- **Heap/periodic and delayed tasks:** Tasks can be scheduled to run periodically or delayed.
- **Epoch based garbage collection:** garbage collected by epochmanager - currently set to 500 ms, can be tuned in startPool().
- **Work-Stealing:** Threads can steal tasks from other queues when idle.
- **Priority Queues:** Supports multiple levels of priority.
- **Lambda Support:** Tasks can be submitted as function pointers or lambda expressions.
- **Lightweight:** Minimal locking, mostly atomic operations for performance.

## Motivation

T_Threads was built as a hobby project to explore advanced parallelism in C++. It provides a flexible task scheduler with:

- Local queues for thread affinity.
- Work-stealing across threads.
- Priority-based execution.
- Minimal overhead for fast task dispatch.

It’s designed for hobby projects, experiments, or as a foundation for building custom concurrent systems.


Usage
Starting the Scheduler
TaskScheduler& scheduler = TaskScheduler::instance();
scheduler.startPool(uint8_t clock_worker);  // Launches worker threads

you must choose a core to run the clock and heap to notify workers and run periodic and delayed tasks

The pool runs automatically and can optionally be joined:

scheduler.join();  // Stops all workers and joins threads
 
the pool will automatically rejoin on program exit -- HOWEVER any forked workers must be manually stopped by holding a reference to the task they run with the stop() function in the task->stop() otherwise it will hang on exit, once stopped they will rejoin the pool

Submitting Tasks
Local Tasks

Assign tasks to a thread-local queue:

scheduler.submitLocal(task);         // Round-robin load balanced
scheduler.submitLocal(1, task);      // Assign to CPU core 1 specifically

Priority Tasks

Submit tasks to a global priority queue (5 levels):

scheduler.submitPQ(task);             // Default priority (3)
scheduler.submitPQ(0, task);          // Specific priority (0–4)

Forked Tasks

Fork a task outside the pool:

scheduler.submitFork(coreID, task);


Forked tasks temporarily remove the thread from the pool. Any local work is redistributed before forking. Stop the worker to rejoin the pool.

Lambda Tasks

All submit methods accept lambda expressions:

scheduler.submitLocal([](){ std::cout << "Lambda task running!" << std::endl; });
scheduler.submitLocal(1, [](){ std::cout << "Pinned lambda!" << std::endl; });

Periodic and Delayed tasks
		
bool submitPeriodic(std::string id, double ms, Task*& task);
bool submitDelayed(double ms, Task*& task);

to cancel a periodic task

simply save a reference to the Task* it starts as and run task->stop() this will stop the periodic task
you can also stop a forked task and get it to rejoin the pool the same way 


Main Thread Tasks

You can enqueue tasks from any thread to run on the main thread:

scheduler.enqueueToMain(task);
scheduler.processMainThread();  // Run all main-thread tasks

Memory Management

Tasks are usually heap-allocated.

The scheduler deletes tasks after execution if auto_delete is true.

To track completion safely, keep tasks in a vector and delete them after all tasks finish:

std::vector<Task*> tasks;
// Submit tasks...
for (auto t : tasks) {
    while (!t->complete.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
}
for (auto t : tasks) {
    delete t;  // Safe deletion after completion
}



Limitations / Known Issues

Lambdas cannot be saved for tracking and are one shot.

Windows Only: No pthreads/Linux support yet.

Task Limits: ~32 million tasks in global queues, ~32k per work-stealing queue.

Exceptions: Not really entirely handled, just returns bool values if inputs are invalid

No Dependency Management: Tasks with dependencies must be manually managed.

Blocking / Completion: No built-in way to block until all tasks finish. Must track manually using references to the tasks if you want to block or check completion of a task.

Memory Safety: Be cautious with raw pointers and task lifetimes.

Notes

The scheduler is a singleton (TaskScheduler::instance()), introducing global state.

Local queues execute pinned tasks before work-stealing tasks.

Priority queues run after local queues finish.

Blocking mechanisms, DAGs, or dependency callbacks are not implemented due to complexity and race conditions.

This is a hobby project, not production-grade, but very flexible for experimentation.
