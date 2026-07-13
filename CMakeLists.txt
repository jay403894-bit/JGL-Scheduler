cmake_minimum_required(VERSION 3.16)
project(3DEngine)
project(3DEngine CXX ASM)

set (CMAKE_CXX_STANDARD 20)
set(CMAKE_ENABLE_EXPORTS ON)

add_subdirectory(src)

add_executable(
  pf_test
  pf_test.cpp
)

set_property(TARGET pf_test PROPERTY CXX_STANDARD 20)

target_link_libraries(
  pf_test
  PRIVATE
  JLib-Scheduler
)