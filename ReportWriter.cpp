//
// Created by Kumbhar Pramod Shivaji on 15.08.17.
//

#include "ReportWriter.h"

ReportWriter::ReportWriter(std::string rname, std::string fname, MPI_Comm comm) {
    report_name = rname;
    filename = fname;
    report_comm = comm;
    report_size = 0;
    max_report_buffer_size = 0;
    is_aggregator = false;
    aggregator_report_size = 0;

    sub_report_comm = MPI_COMM_NULL;
    aggregator_comm = MPI_COMM_NULL;
    data = NULL;

    MPI_Comm_size(report_comm, &report_comm_size);
    MPI_Comm_rank(report_comm, &report_comm_rank);
}

long ReportWriter::number_steps_can_buffer(long report_size, long max_buffer_size) {
    // first check how many report steps this rank can buffer
    long steps = max_buffer_size / report_size;

    long max_buffer_steps = 0;

    // now calculate global minimum
    MPI_Allreduce(&steps, &max_buffer_steps, 1, MPI_LONG, MPI_MIN, report_comm);

    return max_buffer_steps;
}

void ReportWriter::open_file() {
    // only aggregator ranks open file for writing
    MPI_File_open(aggregator_comm, filename.c_str(), MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);
}

void ReportWriter::setup_subcomms() {
    std::vector<long> num_bytes;
    std::vector<long> aggregator_num_bytes;

    if (report_comm_rank == 0) {
        num_bytes.resize(report_comm_size);
    }

    // gather number of bytes to be written by all ranks
    MPI_Gather(&report_size, 1, MPI_LONG, &num_bytes[0], 1, MPI_LONG, 0, report_comm);

    std::vector<int> comm_ids;

    if (report_comm_rank == 0) {
        comm_ids.resize(report_comm_size);
        aggregator_num_bytes.resize(report_comm_size);

        int aggregator_rank = 0;
        long aggregated_bytes = 0;

        // go through all ranks and by looking into report sizes
        // calculate who will be the aggregator ranks. There will
        // be aggregator rank after every max_report_buffer_size
        for (size_t i = 0; i < report_comm_size; i++) {
            if ((aggregated_bytes + num_bytes[i]) > max_report_buffer_size) {
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

    // once we created subcomms from report_comm, then rank 0 in every
    // sub report comm is writer rank.
    int subcomm_rank = 0;
    MPI_Comm_rank(sub_report_comm, &subcomm_rank);
    is_aggregator = (subcomm_rank == 0);

    int comm_id = is_aggregator ? 0 : 1;
    MPI_Comm_split(report_comm, comm_id, report_comm_rank, &aggregator_comm);

}

void ReportWriter::setup_file_view(int *sizes, long long *displacements, int nelements) {

    std::vector<int> subcomm_report_sizes;
    std::vector<int> subcomm_displacements;
    long subcomm_nelements = 0;

    if (is_aggregator) {
        subcomm_report_sizes.resize(sub_report_comm_size());
    }

    // each rank could have different elements, gather those on master rank in sub comm
    MPI_Gather(&nelements, 1, MPI_INT, &subcomm_report_sizes[0], 1, MPI_INT, 0, sub_report_comm);

    // in order to gather all sizes / displacements, aggregator node need to use
    // gatherv and hence need to calculate displacements
    if (is_aggregator) {
        subcomm_displacements.resize(sub_report_comm_size());
        size_t offset = 0;

        for (size_t i = 0; i < subcomm_report_sizes.size(); i++) {
            subcomm_displacements[i] = offset;
            offset += subcomm_report_sizes[i];
            subcomm_nelements += subcomm_report_sizes[i];
        }
    }

    // allocate memory on aggreegator ranks
    if(is_aggregator) {
        aggregated_report_sizes.resize(subcomm_nelements);
        aggregated_report_displacements.resize(subcomm_nelements);
    }

    // gather sizes
    MPI_Gatherv(sizes, nelements, MPI_INT, &aggregated_report_sizes[0],
                &subcomm_report_sizes[0], &subcomm_displacements[0], MPI_INT, 0, sub_report_comm);

    // gather displacements
    MPI_Gatherv(displacements, nelements, MPI_LONG_LONG, &aggregated_report_displacements[0],
                &subcomm_report_sizes[0], &subcomm_displacements[0], MPI_LONG_LONG, 0, sub_report_comm);
}

void ReportWriter::finalize() {
    MPI_Comm_free(&sub_report_comm);
    MPI_Comm_free(&aggregator_comm);
    MPI_File_close(&fh);
}

void ReportWriter::setup_view(int *sizes, long long *displacements, int n, long buffer_size) {

    max_report_buffer_size = buffer_size;

    for(int i = 0; i < n; i++) {
        report_size += sizes[i];
    }

    // once we calculate report_size and know buffer size then we can setup subcomms
    setup_subcomms();

    // open file for writing
    open_file();

    // setup mpi file view
    setup_file_view(sizes, displacements, n);

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
