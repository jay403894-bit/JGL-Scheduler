cmake_minimum_required(VERSION 3.10)
project(T_Threads VERSION 1.0 LANGUAGES CXX)

# Set C++ standard (matches the Release|x64 vcxproj config)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ContextSwitch.asm is hand-written MASM (Microsoft x64 calling convention) -- Windows/MSVC
# only. Guarded so a non-MSVC configure doesn't fail outright; there's no portable
# implementation to fall back to yet.
if(MSVC)
    enable_language(ASM_MASM)
    set(T_THREADS_ASM_SOURCES src/ContextSwitch.asm)
else()
    message(WARNING "T_Threads: ContextSwitch.asm is MASM/Windows-only; skipping it on this compiler.")
    set(T_THREADS_ASM_SOURCES "")
endif()

# Define your library
add_library(T_Threads 
    ${T_THREADS_ASM_SOURCES}
    src/Epochs.cpp
    src/Fiber.cpp
    src/GlobalFiberPool.cpp
    src/T_Thread.cpp
    src/TaskDAG.cpp
    src/TaskScheduler.cpp 
)

# Tell users where the header files are
target_include_directories(T_Threads PUBLIC include)

# Optional: Build the demo/test application only if requested
option(BUILD_TESTS "Build test application" OFF)
if(BUILD_TESTS)
    add_executable(DAGTest main.cpp)
    target_link_libraries(DAGTest PRIVATE T_Threads)
endif()