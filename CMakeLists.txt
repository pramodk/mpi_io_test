cmake_minimum_required(VERSION 2.8)
project(mpi_mem_usage)

set(CMAKE_CXX_STANDARD 11)

find_package(MPI REQUIRED)

set(SOURCE_FILES
        main.cpp
        utils.cpp
        reportinglib/driver.cpp
        utils.h
        ReportWriter.cpp
        ReportWriter.h)

include_directories(${PROJECT_SOURCE_DIR})

add_executable(mpiio ${SOURCE_FILES})
target_link_libraries(mpiio ${MPI_C_LIBRARIES} ${MPI_CXX_LIBRARIES})
