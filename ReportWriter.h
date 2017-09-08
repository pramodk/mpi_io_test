#ifndef REPORT_WRITER_H
#define REPORT_WRITER_H

#include <mpi.h>
#include <string>
#include <vector>

class ReportWriter {
    // name of the file
    std::string filename;

    // size of report in bytes on this rank
    long report_size;

    // max size of buffer set by reportinglib
    long max_report_buffer_size;

    // whether current rank is i/o aggregator
    bool is_aggregator;

    // size of buffer for all aggregated bytes when is_aggregator is true
    long ag_report_size;

    // sum of report size in subcomm; this is same as ag_report_size on rank 0 of subcomm
    long subcomm_report_size;

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

    // when aggregator gathers sizes, displacements into aggregator's
    // shared window are stored here
    std::vector<MPI_Aint> sender_displacements;

    // sender sizes
    std::vector<int> sender_sizes;

    int sender_npieces;

    // mpi file type for i/o
    MPI_Datatype filetype;

    // offset from the beginning of file
    MPI_Offset file_offset;

    // file handle
    MPI_File fh;

    // print all messages
    bool verbose;

    int comm_size(MPI_Comm c) {
        int size;
        MPI_Comm_size(c, &size);
        return size;
    }

    int sub_report_comm_size() {
        return comm_size(sub_report_comm);
    }

    int aggregator_comm_size() {
        return comm_size(aggregator_comm);
    }

    void setup_file_view(const int* sizes, const long long* displacements, int n);
    void create_sender_type(MPI_Datatype& type, int nsteps);
    void setup_communicators();

  public:
    ReportWriter(std::string filename, MPI_Comm comm, bool to_verbose = false);
    long number_steps_can_buffer(long report_size, long max_buffer_size);
    void setup_view(const int* sizes,
                    const long long* displacements,
                    int nelements,
                    MPI_Offset offset,
                    long max_buffer);
    void finalize();
    void write(void* data, int nsteps);
    void print_stat(int nsteps);
};

extern void CHECK_ERROR(int);

#endif  // REPORT_WRITER_H
