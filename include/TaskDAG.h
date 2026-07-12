// TaskDAG.h  -- requires Task and TaskScheduler forward declarations
#pragma once
#include <vector>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include "Task.h"
#include "TaskScheduler.h"
#include "TaskNode.h"
#include "Epochs.h"
#include "TaskAllocator.h"

static constexpr uint8_t NONE = 255;

namespace JLib {

    class TaskDAG {
        struct TaskFinishedContext {
            TaskDAG* parent;
            TaskNode* node;
            // The node's REAL work, saved before Fire() repoints the task's fn/data at the
            // trampoline below. This replaced Task::onComplete/onCompleteData outright -- the
            // DAG was their only user anywhere, and wrapping here shrank every Task to exactly
            // one 64-byte cache line (see Task.h's static_assert).
            Task::Func origFn;
            void* origData;
        };
    public:
        TaskDAG(TaskScheduler& sched) : scheduler(sched) {};
        TaskNode* CreateNode(Task* t, uint8_t priority = NONE, uint8_t cpu_id = NONE);
        // Like CreateNode, but the task runs via TaskScheduler::PushMain (only progresses when
        // the main thread calls ProcessMainThread) instead of the worker pool. Use for anything
        // that must run on the main thread -- e.g. Submit() calls in this renderer, which push
        // into Renderer::m_WorkerLocalStorage/m_Buckets and are only safe single-threaded today.
        // Whoever waits on this DAG's completion MUST use TaskScheduler::WaitForMain, not
        // WaitFor, or a main-affinity node (and everything downstream of it) hangs forever.
        TaskNode* CreateMainNode(Task* t, uint8_t priority = NONE);
        // A gate has no task; it fires its dependents instantly when its trigger is met.
        // Compose gates to express arbitrary boolean readiness, e.g. (A && B) || C.
        TaskNode* CreateGate(TaskNode::LogicType type);
         void AddDependency(TaskNode* dependent, TaskNode* dependency);

         // Trampoline installed as the task's fn by Fire(): runs the node's real work, THEN
         // propagates completion to dependents. Same ordering the old Task::onComplete hook
         // gave (fn -> completion -> waitGroup decrement, since Execute() decrements after fn
         // returns), without Task itself carrying callback fields.
         static void OnTaskFinishedWrapper(void* data) {
             auto* context = static_cast<TaskFinishedContext*>(data);
             context->origFn(context->origData);              // the node's actual task
             context->parent->OnTaskFinished(context->node);  // fire dependents
             delete context; // Cleanup
         }
        // Offline cycle check (Kahn's). MUST be called before any node is submitted --
        // it walks every tracked node, which self-free once running. Returns true if the
        // graph has a cycle (some node's dependencies_left can never reach 0).
        bool HasCycle();

        void Validate();

        // Validate then kick off the whole graph. Returns false (and reclaims the nodes)
        // if there's a cycle; otherwise submits all roots and returns true. This is the
        // intended entry point -- prefer it over calling SubmitIfReady per root, because
        // it also clears node tracking at the right moment (nodes self-free after this).
        bool Submit();

        void OnTaskFinished(TaskNode* node);
        void EndFrame();
    private:
        TaskScheduler& scheduler;
        // Tracks every node created this build, for cycle detection / root discovery.
        // Single-threaded build only; entries dangle after Submit() (nodes self-free),
        // so it is cleared there and never iterated post-submit.
        std::vector<TaskNode*> nodes;

        void Fire(TaskNode* node);   // run the node (or, for a gate, propagate instantly)
        static void NodeDeleter(void* p);   // EBR deleter: ~TaskNode + return its slot to the slab
    };
};