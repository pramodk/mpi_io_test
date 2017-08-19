//
// Created by Kumbhar Pramod Shivaji on 15.08.17.
//

#ifndef MPI_MEM_USAGE_FILE_WRITER_H
#define MPI_MEM_USAGE_FILE_WRITER_H

#include <mpi.h>
#include <string>
#include <vector>

class ReportWriter {

    // name of the report
    std::string report_name;

    // name of the file
    std::string filename;

    // size of report in bytes on this rank
    long report_size;

    // max size of buffer set by reportinglib
    long max_report_buffer_size;

    // whether current rank is i/o aggregator
    bool is_aggregator;

    // size of buffer for all aggregated bytes when is_aggregator is true
    long aggregator_report_size;

    // rank in the report_comm
    int report_comm_rank;

    // size of the report_comm
    int report_comm_size;

    // comm of all ranks participating to i/o (from reportinglib)
    MPI_Comm report_comm;

    // comm of ranks collecting buffer to same aggregator rank
    MPI_Comm sub_report_comm;

    // comm of all aggregator ranks
    MPI_Comm aggregator_comm;

    // each cell element could be of different size
    std::vector<int> aggregated_report_sizes;

    // each cell element has offsets
    std::vector<long long> aggregated_report_displacements;

    // report data buffer
    void* data;

    // file handle
    MPI_File fh;

    int comm_rank(MPI_Comm c) {
        int rank;
        MPI_Comm_rank(c, &rank);
        return rank;
    }

    int comm_size(MPI_Comm c) {
        int size;
        MPI_Comm_size(c, &size);
        return size;
    }

    int sub_report_comm_rank() {
        return comm_rank(sub_report_comm);
    }

    int aggregator_comm_rank() {
        return comm_rank(aggregator_comm);
    }

    int sub_report_comm_size() {
        return comm_size(sub_report_comm);
    }

    int aggregator_comm_size() {
        return comm_size(aggregator_comm);
    }

  public:
    ReportWriter(std::string name, std::string filename, MPI_Comm comm);
    long number_steps_can_buffer(long report_size, long max_buffer_size);
    void setup_view(int *sizes, long long *displacements, int nelements, long max_buffer);
    void open_file();
    void setup_subcomms();
    void finalize();
    void setup_file_view(int *sizes, long long *displacements, int n);
};

#endif  // MPI_MEM_USAGE_FILE_WRITER_H
