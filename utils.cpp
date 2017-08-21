//-----------------------------------------------------------------------------------------------
// Utility functions for memory reporting
//-----------------------------------------------------------------------------------------------

#if __bg__
#define BLUEGENEQ
#include <spi/include/kernel/memory.h>
#endif
#if defined(__APPLE__) && defined(__MACH__)
#include "mach/mach.h"

#else
#include <malloc.h>
#endif

#include <stdint.h>
#include <cstdio>
#include <mpi.h>
#include <stdlib.h>
#include <iostream>
#include "utils.h"

uint64_t usageInBytes(int item) {
#if defined(BLUEGENEQ)
    /* BLUEGENE-Q ------------------------------------------------ */
    uint64_t heap = 0;
    Kernel_GetMemorySize(KERNEL_MEMSIZE_HEAP, &heap);
    return heap;

#elif defined(__APPLE__) && defined(__MACH__)
    /* OSX ------------------------------------------------------
     * Returns the current resident set size (physical memory use) measured
     * in bytes, or zero if the value cannot be determined on this OS.
     */
    struct mach_task_basic_info info;
    mach_msg_type_number_t infoCount = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &infoCount) !=
        KERN_SUCCESS)
        return (uint64_t)0L; /* Can't access? */
    return (uint64_t)info.resident_size_max;

#elif HAVE_MALLINFO
    /* *NIX PLATFORMS WITH MALLINFO ------------------------------ */
    uint64_t r;
    struct mallinfo m;
    m = mallinfo();
    if (item == 1) {
        r = m.uordblks;
    } else if (item == 2) {
        r = m.hblkhd;
    } else if (item == 3) {
        r = m.arena;
    } else if (item == 4) {
        r = m.fordblks;
    } else if (item == 5) {
        r = m.hblks;
    } else if (item == 6) {
        r = m.hblkhd + m.arena;
    } else {
        r = m.hblkhd + m.uordblks;
    }
    return r;

#else
    /* UNSUPPORTED PLATFORM ------------------------------------ */
    return 0;
#endif
}

//-----------------------------------------------------------------------------------------------

double memUsageInfo(const char* msg) {
    double usageMB = (double)usageInBytes(0) / (1024.0 * 1024.0);
    double minUsageMB = 0, maxUsageMB = 0, avgUsageMB = 0;
    int rank, commSize;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &commSize);

    MPI_Reduce(&usageMB, &minUsageMB, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Allreduce(&usageMB, &maxUsageMB, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
    MPI_Reduce(&usageMB, &avgUsageMB, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    avgUsageMB /= commSize;

    MPI_Bcast(&avgUsageMB, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        printf("\t => MEMUSAGE :: ");
        if (msg) {
            printf(" %32s ", msg);
        }
        printf(" Max = %.3lfMB Min = %.3lfMB Mean = %.3lfMB\n", maxUsageMB, minUsageMB, avgUsageMB);
    }

    return usageMB;
}

//-----------------------------------------------------------------------------------------------
// Check MPI Error
//-----------------------------------------------------------------------------------------------

void check_mpi_error(int& error) {
    if (error != MPI_SUCCESS) {
        std::cerr << "Error in MPI Operation" << std::endl;
        char message[1024];
        int mlength;
        MPI_Error_string(error, message, &mlength);
        std::cerr << "MPI: " << message << std::endl;
        //abort();
    }
}

ReportingData::~ReportingData() {
    delete[] gids;
    delete[] sizes;
    delete[] mappingDisp;
    delete[] disp;
    delete[] mappingCount;
    delete[] fakemapping;
    delete[] fakedata;
}
