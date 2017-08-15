cmake_minimum_required(VERSION 3.7)
project(mpi_mem_usage)

set(CMAKE_CXX_STANDARD 11)

find_package(MPI REQUIRED)

set(SOURCE_FILES
        mpi_io_mem_usage.cpp
        utils.cpp
        utils.h)

add_executable(mpi_mem_usage ${SOURCE_FILES})
target_link_libraries(mpi_mem_usage ${MPI_C_LIBRARIES} ${MPI_CXX_LIBRARIES})
