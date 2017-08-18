//
// Created by Kumbhar Pramod Shivaji on 15.08.17.
//

#include "FileWriter.h"

FileWriter::FileWriter(long num_bytes, long buf_size, MPI_Comm comm) {
    my_num_bytes = num_bytes;
    max_buffer_size = buf_size;
    report_comm = comm;

    MPI_Comm_size(report_comm, &report_comm_size);
    MPI_Comm_rank(report_comm, &report_comm_rank);

    // first check how many report steps this rank can buffer
    long steps = max_buffer_size / my_num_bytes;

    // now calculate global minimum
    MPI_Allreduce(&steps, &num_report_steps_can_buffer, 1, MPI_LONG, MPI_MIN, report_comm);
}

long FileWriter::number_report_steps_can_buffer() {
    return num_report_steps_can_buffer;
}

void FileWriter::open_file(std::string filename) {}

void FileWriter::setup_report_subcomms() {

    std::vector<long> num_bytes;
    std::vector<long> aggregator_num_bytes;

    if (report_comm_rank == 0) {
        num_bytes.reserve(report_comm_size);
    }

    // gather number of bytes to be written by all ranks
    MPI_Gather(&my_num_bytes, 1, MPI_LONG, &num_bytes[0], 1, MPI_LONG, 0, report_comm);

    std::vector<int> comm_ids;

    if (report_comm_rank == 0) {
        comm_ids.reserve(report_comm_size);
        aggregator_num_bytes.reserve(report_comm_size);

        int aggregator_rank = 0;
        long aggregated_bytes = 0;

        // go through all ranks and by looking into report sizes
        // calculate who will be the aggregator ranks. There will
        // be aggregator rank after every max_buffer_size
        for (size_t i = 0; i < report_comm_size; i++) {
            if ((aggregated_bytes +  num_bytes[i]) > max_buffer_size) {
                aggregator_rank = i;
                aggregated_bytes = 0;
            }
            comm_ids[i] = aggregator_rank;
            aggregated_bytes += num_bytes[i];
            aggregator_num_bytes[aggregator_rank] = aggregated_bytes;
        }
    }

    // split the report_comm into small sub-communicators
    int sub_comm_id;
    MPI_Scatter(&comm_ids[0], 1, MPI_INT, &sub_comm_id, 1, MPI_INT, 0, report_comm);
    MPI_Comm_split(report_comm, sub_comm_id, report_comm_rank, &sub_report_comm);
}

void FileWriter::setup_writer_subcomms() {
    int subcomm_rank = 0;
    MPI_Comm_rank(sub_report_comm, &subcomm_rank);
    int comm_id = (subcomm_rank == 0) ? 0 : 1;
    MPI_Comm_split(report_comm, comm_id, report_comm_rank, &aggregator_comm);
}


void FileWriter::setup_view(std::vector<int> &buffer_sizes, std::vector<MPI_Offset> &buffer_offsets) {

    int num_report_elements = buffer_sizes.size();
    std::vector<int> num_buffer_sizes;

    if(sub_report_rank() == 0) {
        num_buffer_sizes.reserve(sub_report_comm_size());
    }

    MPI_Gather(&num_report_elements, 1, MPI_INT, &num_buffer_sizes[0], 1, MPI_INT, 0, sub_report_comm);

    std::vector<int> displacement;
    long total_num_report_elements = 0;

    if (sub_report_rank() == 0) {
        displacement.reserve(sub_report_comm_size());
        size_t offset = 0;

        for (size_t i = 0; i < num_buffer_sizes.size(); i++) {
            displacement[i] = offset;
            offset += num_buffer_sizes[i];
            total_num_report_elements += num_buffer_sizes[i];
        }
    }

    aggregator_buffer_sizes.reserve(total_num_report_elements);
    aggregator_buffer_offsets.reserve(total_num_report_elements);

    MPI_Gatherv(&buffer_sizes[0], num_report_elements, MPI_INT, &aggregator_buffer_sizes[0], &num_buffer_sizes[0], &displacement[0], MPI_INT, 0, sub_report_comm);
    MPI_Gatherv(&buffer_offsets[0], num_report_elements, MPI_INT, &aggregator_buffer_offsets[0], &num_buffer_sizes[0], &displacement[0], MPI_INT, 0, sub_report_comm);

}

void FileWriter::finalize() {
    MPI_Comm_free(&sub_report_comm);
    MPI_Comm_free(&aggregator_comm);
}

void FileWriter::setup_aggregators() {

    setup_report_subcomms();
    setup_writer_subcomms();
    //
#if 0

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
#endif

}

