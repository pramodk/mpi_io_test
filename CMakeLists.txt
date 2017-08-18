cmake_minimum_required(VERSION 3.7)
project(mpi_mem_usage)

set(CMAKE_CXX_STANDARD 11)

find_package(MPI REQUIRED)

set(SOURCE_FILES
        main.cpp
        utils.cpp
        utils.h FileWriter.cpp FileWriter.h)

add_executable(mpiio ${SOURCE_FILES})
target_link_libraries(mpiio ${MPI_C_LIBRARIES} ${MPI_CXX_LIBRARIES})
