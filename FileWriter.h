//
// Created by Kumbhar Pramod Shivaji on 15.08.17.
//

#ifndef MPI_MEM_USAGE_FILE_WRITER_H
#define MPI_MEM_USAGE_FILE_WRITER_H

#include <mpi.h>
#include <string>
#include <vector>

class FileWriter {

    // size of buffer on local rank
    long my_num_bytes;

    // size of buffer set by reportinglib
    long max_buffer_size;

    // number of reporting steps can be bufferred globally
    long num_report_steps_can_buffer;

    // whether current rank is i/o aggregator
    bool is_aggregator;

    // size of buffer for all aggreated bytes when is_aggregator is true
    long aggregated_num_bytes;

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
    std::vector<int> aggregator_buffer_sizes;

    // each cell element has offsets
    std::vector<MPI_Offset > aggregator_buffer_offsets;

    // report data buffer
    void *data;


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

    int sub_report_rank() {
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

    FileWriter(long num_bytes, long buf_size, MPI_Comm comm);
    long number_report_steps_can_buffer();
    long calculate_steps_to_buffer();
    void setup_aggregators();
    void open_file(std::string filename);
    void setup_report_subcomms();
    void setup_writer_subcomms();
    void finalize();
    void setup_view(std::vector<int> &report_sizes, std::vector<MPI_Offset> &report_offsets);
};


#endif //MPI_MEM_USAGE_FILE_WRITER_H
