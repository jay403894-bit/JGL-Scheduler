// Reproduce the engine pattern: `new Task(fnptr, data)` + Push + Wait, then ParallelFor.
// Hypothesis: heap `new Task` (~140B) gets Freed into the 256B slab; ParallelFor's
// template CreateTask then placement-news a larger LambdaTask into that under-sized
// heap slot -> heap overflow -> read AV.
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <dbghelp.h>
#include "include/TaskScheduler.h"
#include "include/Event.h"
#include "include/TaskDAG.h"
#include <atomic>
#include <vector>
#include <chrono>
#include <thread>
#include <cmath>
#include <algorithm>
#include <cstdio>
#pragma comment(lib, "dbghelp.lib")

using namespace T_Threads;

static LONG WINAPI OnCrash(EXCEPTION_POINTERS* ep) {
    auto code = ep->ExceptionRecord->ExceptionCode;
    if (code != EXCEPTION_ACCESS_VIOLATION && code != 0xC0000374 /*heap corruption*/)
        return EXCEPTION_CONTINUE_SEARCH;
    fprintf(stderr, "\n*** CRASH code=0x%08lX addr=%p tid=%lu ***\n",
        (unsigned long)code, ep->ExceptionRecord->ExceptionAddress, GetCurrentThreadId());
    if (code == EXCEPTION_ACCESS_VIOLATION)
        fprintf(stderr, "    %s at 0x%p\n", ep->ExceptionRecord->ExceptionInformation[0] ? "WRITE" : "READ",
            (void*)ep->ExceptionRecord->ExceptionInformation[1]);
    HANDLE proc = GetCurrentProcess();
    SymSetOptions(SYMOPT_LOAD_LINES | SYMOPT_DEFERRED_LOADS);
    SymInitialize(proc, nullptr, TRUE);
    void* frames[40];
    USHORT n = CaptureStackBackTrace(0, 40, frames, nullptr);
    char b[sizeof(SYMBOL_INFO) + 256]; SYMBOL_INFO* sym = (SYMBOL_INFO*)b;
    sym->SizeOfStruct = sizeof(SYMBOL_INFO); sym->MaxNameLen = 255;
    for (USHORT i = 0; i < n; ++i) {
        DWORD64 disp = 0; const char* name = "?";
        if (SymFromAddr(proc, (DWORD64)frames[i], &disp, sym)) name = sym->Name;
        IMAGEHLP_LINE64 ln{}; ln.SizeOfStruct = sizeof(ln); DWORD ld = 0;
        if (SymGetLineFromAddr64(proc, (DWORD64)frames[i], &ld, &ln))
            fprintf(stderr, "  [%02u] %s  (%s:%lu)\n", i, name, ln.FileName, ln.LineNumber);
        else
            fprintf(stderr, "  [%02u] %s +0x%llx\n", i, name, (unsigned long long)disp);
    }
    fflush(stderr);
    TerminateProcess(GetCurrentProcess(), 99);
    return EXCEPTION_EXECUTE_HANDLER;
}

struct Params { int a, b; std::atomic<long long>* sum; };
static void Wrapper(void* p) {
    Params* pp = static_cast<Params*>(p);
    long long local = 0;
    for (int i = pp->a; i < pp->b; ++i) local += i;
    pp->sum->fetch_add(local, std::memory_order_relaxed);
}

static void EventWaiter(void* d) {
    auto* woken = static_cast<std::atomic<int>*>(d);
    TaskScheduler::Instance().WaitOnEvent("e");   // suspend until SignalAll
    woken->fetch_add(1, std::memory_order_relaxed);
}

int main(int argc, char** argv) {
    AddVectoredExceptionHandler(1, OnCrash);
    // mode: "A" = Wait-on-new-Task, "B" = ParallelFor, "E" = event suspend/resume, else both
    char mode = (argc > 1) ? argv[1][0] : '*';
    TaskScheduler::Init();
    auto& s = TaskScheduler::Instance();

    if (mode == 'E') {
        // Stress the park/signal race: many fibers WaitOnEvent, main hammers SignalAll.
        // A lost wakeup => some task stuck SUSPENDED => 'woken' plateaus => watchdog trips.
        const int N = 128, ROUNDS = 300;
        for (int round = 0; round < ROUNDS; ++round) {
            std::atomic<int> woken{ 0 };
            for (int i = 0; i < N; ++i) s.Push(s.CreateTask(EventWaiter, &woken));
            auto t0 = std::chrono::steady_clock::now();
            while (woken.load(std::memory_order_acquire) < N) {
                s.GetEvent("e").SignalAll();
                std::this_thread::yield();
                if (std::chrono::steady_clock::now() - t0 > std::chrono::seconds(10)) {
                    fprintf(stderr, "EVENT HANG round %d woken=%d/%d (lost wakeup)\n",
                        round, woken.load(), N);
                    return 3;
                }
            }
            if ((round % 50) == 0) { printf("[e] round %d ok\n", round); fflush(stdout); }
        }
        printf("EVENT_TEST_OK rounds=%d\n", ROUNDS);
        return 0;
    }

    if (mode == 'D') {
        // DAG / cycle detection. Objects are leaked on purpose: a node's onComplete
        // captures `this` (the dag), so destroying the dag while a worker is still in
        // OnTaskFinished would be a UAF -- leaking sidesteps that in this short test.

        // 1) Valid diamond: A->B, A->C, B->D, C->D. HasCycle() false; all 4 run once.
        {
            auto* dag = new TaskDAG(s);
            auto* ran = new std::atomic<int>(0);
            auto mk = [&] { return dag->CreateNode(s.CreateTask([ran] { ran->fetch_add(1, std::memory_order_relaxed); })); };
            TaskNode* A = mk(), * B = mk(), * C = mk(), * D = mk();
            dag->AddDependency(B, A);
            dag->AddDependency(C, A);
            dag->AddDependency(D, B);
            dag->AddDependency(D, C);
            if (dag->HasCycle()) { fprintf(stderr, "DAG: false-positive cycle on valid diamond\n"); return 4; }
            if (!dag->Submit()) { fprintf(stderr, "DAG: Submit rejected a valid diamond\n"); return 4; }
            auto t0 = std::chrono::steady_clock::now();
            while (ran->load(std::memory_order_acquire) < 4) {
                std::this_thread::yield();
                if (std::chrono::steady_clock::now() - t0 > std::chrono::seconds(10)) {
                    fprintf(stderr, "DAG: diamond hung, ran=%d/4\n", ran->load()); return 4;
                }
            }
            printf("[d] diamond ran all 4 ok\n");
        }

        // 2) Cycle: A->B->C->A. HasCycle() true; Submit() must reject and not run/hang.
        {
            auto* dag = new TaskDAG(s);
            auto* ran = new std::atomic<int>(0);
            auto mk = [&] { return dag->CreateNode(s.CreateTask([ran] { ran->fetch_add(1, std::memory_order_relaxed); })); };
            TaskNode* A = mk(), * B = mk(), * C = mk();
            dag->AddDependency(B, A);
            dag->AddDependency(C, B);
            dag->AddDependency(A, C);   // closes the cycle
            if (!dag->HasCycle()) { fprintf(stderr, "DAG: cycle NOT detected\n"); return 4; }
            if (dag->Submit()) { fprintf(stderr, "DAG: Submit accepted a cyclic graph\n"); return 4; }
            if (ran->load() != 0) { fprintf(stderr, "DAG: cyclic task ran (%d)\n", ran->load()); return 4; }
            printf("[d] cycle detected + rejected ok\n");
        }

        printf("DAG_TEST_OK\n");
        return 0;
    }

    if (mode == 'P') {
        // Benchmark: same sqrt workload, serial loop vs ParallelFor, across sizes.
        // Reports the MEDIAN of many reps (robust to scheduling jitter), in microseconds.
        unsigned hw = std::thread::hardware_concurrency();
        int workers = (hw > 1) ? (int)hw - 1 : 1;
        printf("workers=%d   (median of 101 reps, microseconds)\n", workers);
        printf("%-12s %-14s %-14s %-10s\n", "N", "serial(us)", "parallel(us)", "speedup");

        const int sizes[] = { 1000, 10000, 100000, 1000000, 4000000 };
        const int REPS = 101;
        volatile long long ssink = 0;

        for (int N : sizes) {
            int chunk = N / workers; if (chunk < 1) chunk = 1;

            // Warm up the fiber pool / caches / branch predictors before timing.
            std::atomic<long long> warm{ 0 };
            for (int w = 0; w < 5; ++w)
                s.ParallelFor(0, N, chunk, [&](int a, int b) {
                    double l = 0; for (int i = a; i < b; ++i) l += std::sqrt((double)i);
                    warm.fetch_add((long long)l, std::memory_order_relaxed); });

            std::vector<double> ts, tp; ts.reserve(REPS); tp.reserve(REPS);
            for (int r = 0; r < REPS; ++r) {
                // --- serial ---
                auto a0 = std::chrono::high_resolution_clock::now();
                double acc = 0; for (int i = 0; i < N; ++i) acc += std::sqrt((double)i);
                auto a1 = std::chrono::high_resolution_clock::now();
                ssink += (long long)acc;
                ts.push_back(std::chrono::duration<double, std::micro>(a1 - a0).count());

                // --- parallel ---
                std::atomic<long long> psink{ 0 };
                auto b0 = std::chrono::high_resolution_clock::now();
                s.ParallelFor(0, N, chunk, [&](int a, int b) {
                    double l = 0; for (int i = a; i < b; ++i) l += std::sqrt((double)i);
                    psink.fetch_add((long long)l, std::memory_order_relaxed); });
                auto b1 = std::chrono::high_resolution_clock::now();
                tp.push_back(std::chrono::duration<double, std::micro>(b1 - b0).count());
            }
            std::sort(ts.begin(), ts.end());
            std::sort(tp.begin(), tp.end());
            double sMed = ts[REPS / 2], pMed = tp[REPS / 2];
            printf("%-12d %-14.2f %-14.2f %-9.2fx\n", N, sMed, pMed, sMed / pMed);
            fflush(stdout);
        }
        return 0;
    }

    const int RANGE = 100000, CHUNK = 64;
    long long expected = 0; for (long long i = 0; i < RANGE; ++i) expected += i;

    for (int iter = 0; iter < 300; ++iter) {
        std::atomic<long long> sumB{ 0 };
        s.ParallelFor(0, RANGE, CHUNK, [&sumB](int a, int b) {
            long long local = 0; for (int i = a; i < b; ++i) local += i;
            sumB.fetch_add(local, std::memory_order_relaxed);
            });
        if (sumB.load() != expected) {
            fprintf(stderr, "MISMATCH iter %d sumB=%lld exp=%lld\n", iter, sumB.load(), expected);
            return 2;
        }
        if ((iter % 25) == 0) { printf("[t] iter %d ok\n", iter); fflush(stdout); }
    }
    printf("PF_TEST_OK\n");
    return 0;
}
