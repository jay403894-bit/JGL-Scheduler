add_library(JLib-Scheduler STATIC
    ContextSwitch.asm
    Epochs.cpp
    Fiber.cpp
    GlobalFiberPool.cpp
    Task.cpp
    TaskDAG.cpp
    TaskScheduler.cpp
    Thread.cpp
)
target_include_directories(JLib-Scheduler PUBLIC
    ../include
)

set_property(TARGET JLib-Scheduler PROPERTY CXX_STANDARD 20)
