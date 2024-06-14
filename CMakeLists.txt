cmake_minimum_required(VERSION 3.16)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")

project(CArrowBenchmark)
set(CMAKE_BUILD_TYPE Release)


find_package(Arrow REQUIRED)

# Enable verbose output
#set(CMAKE_VERBOSE_MAKEFILE ON)


message(STATUS "The value of MY_VARIABLE is: ${ARROW_VERSION}")


find_package(Parquet REQUIRED)
add_executable(benchmark benchmark.cc)
target_link_libraries(benchmark Arrow::arrow_shared Parquet::parquet_shared)
