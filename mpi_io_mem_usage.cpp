#include "utils.h"
#include <iostream>
#include <math.h>
#include <mpi.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <vector>

//#define MAX_BUFFER_IN_BYTES (8*1024*1024)
#define MAX_BUFFER_IN_BYTES (8 * 5)

void mpi_file_write_by_staging(void* data, int nbytes, int rank, int comm_size) {
    std::vector<int> sizes;
    std::vector<long> aggregator_sizes;
    std::vector<int> colors;
    int color;

    if (rank == 0) {
        sizes.reserve(comm_size);
        colors.reserve(comm_size);
        aggregator_sizes.resize(comm_size);
    }

    MPI_Gather(&nbytes, 1, MPI_INT, &sizes[0], 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        int aggregator_rank = 0;
        long aggretaed_bytes = 0;

        for (size_t i = 0; i < comm_size; i++) {
            if ((aggretaed_bytes + sizes[i]) > MAX_BUFFER_IN_BYTES) {
                aggregator_rank = i;
                aggretaed_bytes = 0;
            }

            colors[i] = aggregator_rank;
            aggretaed_bytes += sizes[i];
            aggregator_sizes[aggregator_rank] = aggretaed_bytes;
        }
    }

    MPI_Scatter(&colors[0], 1, MPI_INT, &color, 1, MPI_INT, 0, MPI_COMM_WORLD);

    MPI_Comm subcomms;
    MPI_Comm_split(MPI_COMM_WORLD, color, rank, &subcomms);

    int newrank = 0;
    int newcomm_size = 0;
    MPI_Comm_rank(subcomms, &newrank);
    MPI_Comm_size(subcomms, &newcomm_size);

    unsigned long bytes_to_write;
    MPI_Scatter(&aggregator_sizes[0], 1, MPI_LONG, &bytes_to_write, 1, MPI_LONG, 0, MPI_COMM_WORLD);

    printf(
        "I am rank %d (new %d) and I am going to send %d bytes to %d RANK and "
        "aggregating %ld\n",
        rank, newrank, nbytes, color, bytes_to_write);

    std::vector<char> aggregator_data;
    aggregator_data.reserve(bytes_to_write);

    std::vector<int> recvcounts, displs;

    if (newrank == 0) {
        recvcounts.reserve(newcomm_size);
        displs.reserve(newcomm_size);
    }

    MPI_Gather(&nbytes, 1, MPI_INT, &recvcounts[0], 1, MPI_INT, 0, subcomms);

    if (newrank == 0) {
        size_t offset = 0;
        for (int i = 0; i < newcomm_size; i++) {
            displs[i] = offset;
            offset += recvcounts[i];
        }
    }

    MPI_Gatherv(data, nbytes, MPI_CHAR, &aggregator_data[0], &recvcounts[0], &displs[0], MPI_CHAR,
                0, subcomms);

    // comm for aggregators only
    MPI_Comm aggregator_comm;

    color = newrank == 0 ? 0 : 1;
    MPI_Comm_split(MPI_COMM_WORLD, color, rank, &aggregator_comm);

    unsigned long int offset = 0;
    MPI_Exscan(&bytes_to_write, &offset, 1, MPI_UNSIGNED_LONG, MPI_SUM, aggregator_comm);

    MPI_File fh;
    MPI_Status status;

    int result = MPI_File_open(aggregator_comm, "sreport.bbp", MPI_MODE_CREATE | MPI_MODE_WRONLY,
                               MPI_INFO_NULL, &fh);

    MPI_File_write_at_all(fh, offset, &aggregator_data[0], bytes_to_write, MPI_CHAR, &status);

    MPI_File_close(&fh);

    MPI_Comm_free(&subcomms);
    MPI_Comm_free(&aggregator_comm);
}

void write_with_write_at(MPI_File& fh, void* data, int nbytes, int rank, int comm_size) {
    unsigned long int num_bytes = nbytes;
    unsigned long int offset = 0;
    MPI_Status status;

    // calculate offset to write at
    MPI_Exscan(&num_bytes, &offset, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);

    memUsageInfo("Before writing data with write_at");

    MPI_File_write_at(fh, offset, data, num_bytes, MPI_BYTE, &status);

    memUsageInfo("After writing data with write_at");
}

void write_with_write_all(MPI_File& fh, void* data, int nbytes, int rank, int comm_size) {
    unsigned long int num_bytes = nbytes;
    unsigned long int offset = 0;
    MPI_Status status;

    MPI_Datatype etype, arraytype, contig;
    MPI_Offset disp;

    // calculate offset to write at
    MPI_Exscan(&num_bytes, &offset, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);

    etype = MPI_BYTE;
    disp = offset;

    // write nbytes contigiously
    MPI_Type_contiguous(nbytes, MPI_BYTE, &arraytype);
    MPI_Type_commit(&arraytype);

    MPI_File_set_view(fh, disp, etype, arraytype, "native", MPI_INFO_NULL);

    memUsageInfo("Before writing data with write_all");

    MPI_File_write_all(fh, data, nbytes, etype, MPI_STATUS_IGNORE);

    memUsageInfo("After writing data with write_all");

    MPI_Type_free(&arraytype);
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int rank, comm_size;

    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    void* mappingData = NULL;
    int numCellElements = 0;

    srand(time(NULL) + rank);

    // each rank has few neurons
    numCellElements = randomNumber(1, 5);

    memUsageInfo("At the begining");

    if (rank == 0) {
        printf("Total number of ranks = %d\n", comm_size);
    }

    // number of bytes written is an integer per neuron
    int num_bytes = sizeof(int) * numCellElements;

    MPI_Barrier(MPI_COMM_WORLD);

    MPI_File fh;
    MPI_Status status;

    memUsageInfo("Before File Open");

    int result = MPI_File_open(MPI_COMM_WORLD, "report.bbp", MPI_MODE_CREATE | MPI_MODE_WRONLY,
                               MPI_INFO_NULL, &fh);

    memUsageInfo("After File Open");

    if (result != MPI_SUCCESS)
        std::cerr << "Error opening the file " << std::endl;

    MPI_Barrier(MPI_COMM_WORLD);

    memUsageInfo("Before Buffer Allocation");

    // just some buffer to write
    mappingData = malloc(num_bytes);

    memUsageInfo("After Buffer Allocation");

    // write using MPI_File_write_at
    write_with_write_at(fh, mappingData, num_bytes, rank, comm_size);

    // write using MPI_File_write_all
    write_with_write_all(fh, mappingData, num_bytes, rank, comm_size);

    // write using MPI_File_write_at_all and aggregation
    mpi_file_write_by_staging(mappingData, num_bytes, rank, comm_size);
    free(mappingData);

    MPI_File_close(&fh);
    MPI_Finalize();
}
