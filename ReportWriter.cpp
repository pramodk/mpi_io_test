//
// Created by Kumbhar Pramod Shivaji on 15.08.17.
//

#include <climits>
#include "ReportWriter.h"
#include "utils.h"

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

void ReportWriter::setup_file_view(int* sizes, long long* displacements, int nelements) {
    std::vector<int> subcomm_nelements;
    long total_subcomm_nelements = 0;

    if (is_aggregator) {
        // subcomm_report_sizes.resize(sub_report_comm_size());
        subcomm_nelements.resize(sub_report_comm_size());
    }

    // each rank could have different elements, gather those on master rank in sub comm
    MPI_Gather(&nelements, 1, MPI_INT, &subcomm_nelements[0], 1, MPI_INT, 0, sub_report_comm);

    std::vector<int> s_displacements;

    // in order to gather all sizes / displacements, aggregator node need to use
    // gatherv and hence need to calculate displacements
    if (is_aggregator) {
        s_displacements.resize(sub_report_comm_size());
        for (size_t i = 0; i < subcomm_nelements.size(); i++) {
            s_displacements[i] = total_subcomm_nelements;
            total_subcomm_nelements += subcomm_nelements[i];
        }
    }

    // allocate memory on aggreegator ranks
    if (is_aggregator) {
        aggregated_report_sizes.resize(total_subcomm_nelements);
        aggregated_report_displacements.resize(total_subcomm_nelements);
    }

    // gather sizes
    MPI_Gatherv(sizes, nelements, MPI_INT, &aggregated_report_sizes[0], &subcomm_nelements[0],
                &s_displacements[0], MPI_INT, 0, sub_report_comm);

    // gather displacements
    MPI_Gatherv(displacements, nelements, MPI_LONG_LONG, &aggregated_report_displacements[0],
                &subcomm_nelements[0], &s_displacements[0], MPI_LONG_LONG, 0, sub_report_comm);

    aggregator_report_size = 0;

    if (is_aggregator) {
        std::vector<MPI_Aint> file_displacements;
        file_displacements.reserve(total_subcomm_nelements);
        subcomm_displacements.resize(sub_report_comm_size());
        subcomm_report_sizes.resize(sub_report_comm_size());

        size_t k = 0;
        size_t offset = 0;
        int length = 0;

        for (size_t i = 0; i < sub_report_comm_size(); i++) {
            length = 0;
            for (size_t j = 0; j < subcomm_nelements[i]; j++) {
                file_displacements.push_back((MPI_Aint)aggregated_report_displacements[k]);
                length += aggregated_report_sizes[k];
                aggregator_report_size += aggregated_report_sizes[k];
                k++;
            }

            subcomm_displacements[i] = offset;
            subcomm_report_sizes[i] = length;
            offset += length;
        }

        int error = MPI_Type_create_hindexed(total_subcomm_nelements, &aggregated_report_sizes[0],
                                             &file_displacements[0], MPI_BYTE, &filetype);

        MPI_Type_commit(&filetype);
        check_mpi_error(error);

        error = MPI_File_set_view(fh, start_offset, MPI_BYTE, filetype, "native", MPI_INFO_NULL);

        check_mpi_error(error);
    }
}

void ReportWriter::finalize() {
    MPI_Comm_free(&sub_report_comm);
    MPI_Comm_free(&aggregator_comm);
    MPI_File_close(&fh);
}

void ReportWriter::setup_view(int* sizes,
                              long long* displacements,
                              int nelements,
                              MPI_Offset position,
                              long buffer_size) {
    max_report_buffer_size = buffer_size;
    start_offset = position;

    for (int i = 0; i < nelements; i++) {
        report_size += sizes[i];
    }

    // once we calculate report_size and know buffer size then we can setup subcomms
    setup_subcomms();

    // open file for writing
    open_file();

    // setup mpi file view
    setup_file_view(sizes, displacements, nelements);
}

void ReportWriter::write(void* data) {
    MPI_Status status;

    void* aggregated_data = NULL;
    if (is_aggregator)
        aggregated_data = calloc(1, aggregator_report_size);

    if (aggregator_report_size > INT_MAX) {
        std::cerr << "ERROR : Aggregator report buffer size > INT_MAX \n";
        abort();
    }

    MPI_Barrier(report_comm);

    printf("R. %d REPORT %ld BYTES AGGREGATING %ld BYTES \n", report_comm_rank, report_size,
           aggregator_report_size);

    int error =
        MPI_Gatherv(data, (int)report_size, MPI_BYTE, aggregated_data, &subcomm_report_sizes[0],
                    &subcomm_displacements[0], MPI_BYTE, 0, sub_report_comm);

    check_mpi_error(error);

    if (is_aggregator) {
        error = MPI_File_write_all(fh, aggregated_data, aggregator_report_size, MPI_BYTE, &status);
        check_mpi_error(error);
    }

    if (is_aggregator)
        free(aggregated_data);
}
