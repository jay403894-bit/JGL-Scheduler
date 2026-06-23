cmake_minimum_required(VERSION 3.10)
project(T_Threads VERSION 1.0)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Define your library
add_library(T_Threads 
    src/ContextSwitch.asm
    src/Epochs.cpp
    stc/Fiber.cpp
    stc/FiberPool.cpp
    src/T_Thread.cpp
    src/TaskDAG.cpp
    src/TaskScheduler.cpp 
)

# Tell users where the header files are
target_include_directories(T_Threads PUBLIC include)

# Optional: Build your tests only if requested
option(BUILD_TESTS "Build test application" OFF)
if(BUILD_TESTS)
    add_executable(DAGTest tests/main.cpp)
    target_link_libraries(DAGTest PRIVATE T_Threads)
endif()