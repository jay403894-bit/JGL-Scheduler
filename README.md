# T_Threads



A high-performance, lightweight lockfree-ish where applicable C++ fibers task scheduler/jobs system with thread affinity and work-stealing queues.



## Contribution



Feel free to explore and experiment with the scheduler. Pull requests or ideas welcome for improvements.



## Features

- **Thread Pool:** Efficient management of worker threads.

- **Local Queues:** Tasks can be scheduled to specific threads for cache locality.

- **Manual task Garbage collection:** deferred deletion, scheduler.CollectGarbage() at the end of a frame or whenever tasks are completed and no longer being used/saved.

- **Work-Stealing:** Threads can steal tasks from other queues when idle.

- **Priority Queues:** Supports multiple levels of priority.

- **Lambda Support:** Tasks can be submitted as function pointers or lambda expressions.

- **Lightweight:** lockfree where applicable, very few locks used only where necessary, atomic operations for performance.

- **DAG:** Supports task dependency graphing with optional TaskDAG

- **Fibers:** runs using usermode fibers for efficiency

- **memory management:* Arena allocation and epochs used for garbage collection, simply         T_Threads::EpochManager::Instance().Tick(); at the end of your frame/main loop and include "Epochs.h" in your main loop file.


## Motivation



T_Threads was built as a hobby project to explore advanced parallelism in C++. It provides a flexible task scheduler with:



- Local queues for thread affinity.

- Work-stealing across threads.

- Priority-based execution.

- Minimal overhead for fast task dispatch.



It’s designed for hobby projects, experiments, or as a foundation for building custom concurrent systems. I am currently using it in my game/simulation engine project 

-----
Usage
-----

----------------------
Starting the Scheduler
----------------------
	TaskScheduler& scheduler = TaskScheduler::Instance();

	scheduler.StartPool(uint8_t clock_worker);  // Launches worker threads

you must choose a core to run the clock and heap to notify workers and run periodic and delayed tasks

The pool runs automatically and can optionally be joined:

	scheduler.Join();  // Stops all workers and joins threads

the pool will automatically rejoin on program exit -- HOWEVER any forked workers must be manually stopped by holding a reference to the task they run with the stop() function in the task->stop() otherwise it will hang on exit, once stopped they will rejoin the pool

--------------
Creating Tasks
--------------
tasks can be created with lambdas or C-style static void(void*) functions using CreateTask

	// Example function for tasks
	void simpleTaskFn(void* data) {
   		 int* id = static_cast<int*>(data);
    		std::this_thread::yield();
   		 std::cout << "Task " << *id
      		  << " executed on thread " << std::this_thread::get_id()
     		   << std::endl << std::flush;
   		 std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}

	int id=1
	
	Task* task = scheduler.CreateTask(simpleTaskFn, id);

or
	auto* myTask = CreateTask([]() {
    		// Your code here
    		printf("Task executing from Arena!\n");
	});


----------------
Submitting Tasks
----------------

-------------------------
Work stealing deque tasks
           &
Local affinity tasks
-------------------------
Assign tasks to a thread-local queue:

	scheduler.Push(task);         // Round-robin load balanced
	scheduler.Push(1, task);      // Assign to CPU core 1 affinity hint, work stealing may take it 

you can also directly type in a lambda instead of a task, but it will not be tracked unless you specifically

save it as a task or lambdatask object. auto works since you will just CreateTask() and its compatible with both

-------------
Priority Tasks
-------------

Submit tasks to a global priority queue (5 levels):

	scheduler.PushPQ(task);             // Default priority (3)
	scheduler.PushPQ(0, task);          // Specific priority (0–4)

------------
Forked Tasks
------------

Fork a task outside the pool:

	scheduler.PushFork(coreID, task);

Forked tasks temporarily remove the thread from the pool. Any local work is redistributed before forking. Stop the worker to rejoin the pool.


to stop a forked task 

	scheduler.Stop(Task*); 
	
granted you have to setup stop conditions in the task this sets the flag though so main or another thread can request stop


-----------------
Main Thread Tasks
-----------------
You can enqueue tasks from any thread to run on the main thread:

	scheduler.EnqueueToMain(task);
	scheduler.ProcessMainThread();  // Run all main-thread tasks

Tasks are usually heap-allocated.

To track completion safely, keep tasks in a vector and delete them after all tasks finish using waitall:

    std::vector<Task*> tasks;
    for (int it = 0; it < iterations; ++it) {
        for (int t = 0; t < tasksPerIteration; ++t) {
            // allocate id on heap to keep it alive for the function
            int* id = new int(t);

            Task* task = new Task(simpleTaskFn, id);

            scheduler.Push(task);
            tasks.push_back(task);
            std::this_thread::yield();
        }
		scheduler.WaitAll(tasks);


------------
Parallel For
------------

Define a function or lambda with two ints, (start and end) and call 
ParallelFor(start,end,chunksize,func(int,int)) and it will distribute it
amongst the threads.

	// The function signature usually looks like this:
	// void(int start, int end)

	scheduler.ParallelFor(0, 10000, 128, [&](int start, int end) {
    		// This code runs in parallel on different threads!
   		 for (int i = start; i < end; ++i) {
      		  UpdateEntity(i);
    		}
		});

	auto myPhysicsWork = [&](int start, int end) {
    		for (int i = start; i < end; ++i) {
       		 // This is your standard entity update logic
       		 UpdateParticle(i); 
    		}
	};

// 2. The scheduler handles the "How" (Partitioning the range 0-1000)
scheduler.ParallelFor(0, 1000, 128, myPhysicsWork);

there is also a nonblocking variant  ParallelForNB -- this one will not block main if you want to fire and forget

-----------------
Memory Management
-----------------
memory is automatically handled with Arenas and Epoch Garbage collection
simply include "Epochs.h" in your main file and run this at the end of your main loop 
this should ONLY BE RUN by the main thread
		
        T_Threads::EpochManager::Instance().Tick();

____________________
:::TO USE THE DAG:::
--------------------

The TaskDAG module provides a high-performance, dependency-aware task scheduling system. It allows you to build complex task graphs where tasks execute only after their dependencies have completed, ensuring optimal utilization of the TaskScheduler.



Getting Started

1. Creating the Graph

Use the TaskDAG as a factory for your nodes. The graph owns the nodes, simplifying memory management.

	TaskScheduler& sched = TaskScheduler::instance();
	TaskDAG dag(sched);



// Create nodes with routing options

	TaskNode* nodeA = dag.createNode(new LambdaTask([](){ /* Work */ }), 		Queue::Default, ANY_CORE);

	TaskNode* nodeB = dag.createNode(new LambdaTask([](){ /* Work */ }), Queue::Priority, 0);



// Define dependencies (B runs after A)

	dag.addDependency(nodeB, nodeA);

2. Executing the DAG

Submit your entry-point node(s). The scheduler handles the propagation through the graph automatically.

	dag.submitIfReady(nodeA);

3. Cleanup

simply
        T_Threads::EpochManager::Instance().Tick();
at the end of your main loop 

4. If you want to run a service rather than a fork job with PushFork!!!
 -- you still can run a continuous service (audio, networking, etc) if you want!
in the tasks void* data, send a pointer to the task itself as a parameter.

then run task->SignalComplete() when the subsystem is active and ready instead of waiting for it to finish!

this will signal to the dag to run any dependent task on that service!


----------------
suspending tasks
----------------

fibers allow us to suspend tasks, (documentation to be improved in the future) 
to do so, use the included Event.h event system to declare events and subscribe tasks as waiters 
this can suspend and then signal them to awaken later and switch back context

---------------------------
Limitations / Known Issues
---------------------------

Windows Only: No pthreads/Linux support yet.

Task Limits: ~32 million tasks in global queues, ~32k per work-stealing queue. overflow not really handled arena size is about 10mb which is 4x that so can be adjusted.

if you need bigger sizes you have to modify the source or if you had to save memory and needed less and knew in advance

Memory Safety: Be cautious with raw pointers and task lifetimes.


-----
Notes
-----

The scheduler is a singleton (TaskScheduler::instance()), introducing global state.

Local queues execute pinned tasks before work-stealing tasks.

Priority queues run after local queues finish work.

This is a hobby project, not production-grade, but very flexible for experimentation. 
