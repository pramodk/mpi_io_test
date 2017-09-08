#include <stdlib.h>
#include <climits>
#include <algorithm>
#include "ReportWriter.h"

typedef std::pair<long long, int> OffsetIndexPairType;
bool comparator(const OffsetIndexPairType& l, const OffsetIndexPairType& r) {
    return l.first < r.first;
}

template <typename T>
std::vector<T> makeVector(const T* data, size_t N) {
    return std::vector<T>(data, data + N);
}

ReportWriter::ReportWriter(std::string fname, MPI_Comm comm, bool to_verbose) {
    filename = fname;
    report_comm = comm;
    report_size = 0;
    max_report_buffer_size = 0;
    is_aggregator = false;
    ag_report_size = 0;
    subcomm_report_size = 0;
    verbose = to_verbose;

    sub_report_comm = MPI_COMM_NULL;
    aggregator_comm = MPI_COMM_NULL;

    MPI_Comm_size(report_comm, &report_comm_size);
    MPI_Comm_rank(report_comm, &report_comm_rank);

    verbose = verbose && (report_comm_rank == 0);
}

long ReportWriter::number_steps_can_buffer(long rsize, long max_buffer_size) {
    // first check how many report steps this rank can buffer
    long steps = INT_MAX;

    if (rsize) {
        steps = max_buffer_size / rsize;
    }

    long max_buffer_steps = 0;

    // now calculate global minimum
    MPI_Allreduce(&steps, &max_buffer_steps, 1, MPI_LONG, MPI_MIN, report_comm);

    return max_buffer_steps;
}

void ReportWriter::setup_communicators() {
    std::vector<long> all_report_sizes;
    std::vector<long> aggregator_num_bytes;

    if (report_comm_rank == 0) {
        all_report_sizes.resize(report_comm_size);
    }

    // gather number of bytes to be written by all ranks
    CHECK_ERROR(MPI_Gather(&report_size, 1, MPI_LONG, &all_report_sizes[0], 1, MPI_LONG, 0, report_comm));

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
            if (all_report_sizes[i] > 0) {
                if ((aggregated_bytes + all_report_sizes[i]) > max_report_buffer_size) {
                    aggregator_rank = i;
                    aggregated_bytes = 0;
                }
                comm_ids[i] = aggregator_rank;
                aggregated_bytes += all_report_sizes[i];
                aggregator_num_bytes[aggregator_rank] = aggregated_bytes;
            }
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

    if(report_comm_rank == 0) {
        printf("\n SUMMARY : TOTAL RANKS : %d TOTAL AGGREGATORS : %d \n", report_comm_size, aggregator_comm_size());
    }
}

void ReportWriter::setup_file_view(const int* sizes, const long long* displacements, int npieces) {
    // each cell element could be of different size
    std::vector<int> ag_report_sizes;

    // each cell element has offsets
    std::vector<long long> ag_report_file_disp;

    // receiver i.e. aggregator will gather data in mmeory from offset 0
    // these are offsets at which other ranks will sending data
    std::vector<MPI_Aint> ag_report_mem_disp;

    std::vector<int> subcomm_npieces;

    long total_subcomm_npieces = 0;

    sender_displacements.resize(npieces);

    if (is_aggregator) {
        subcomm_npieces.resize(sub_report_comm_size());
    }

    // each rank could have different elements, gather those on master rank in sub comm
    MPI_Gather(&npieces, 1, MPI_INT, &subcomm_npieces[0], 1, MPI_INT, 0, sub_report_comm);

    std::vector<int> s_displacements;

    // in order to gather all sizes / displacements, aggregator node need to use
    // gatherv and hence need to calculate displacements
    if (is_aggregator) {
        s_displacements.resize(sub_report_comm_size());

        for (size_t i = 0; i < subcomm_npieces.size(); i++) {
            s_displacements[i] = total_subcomm_npieces;
            total_subcomm_npieces += subcomm_npieces[i];
        }
    }

    // allocate memory on aggreegator ranks
    if (is_aggregator) {
        ag_report_sizes.resize(total_subcomm_npieces + 2);
        ag_report_file_disp.resize(total_subcomm_npieces);
        ag_report_mem_disp.resize(total_subcomm_npieces);
    }

    // gather sizes
    MPI_Gatherv((void*)sizes, npieces, MPI_INT, &ag_report_sizes[1], &subcomm_npieces[0], &s_displacements[0], MPI_INT,
                0, sub_report_comm);

    // gather displacements
    MPI_Gatherv((void*)displacements, npieces, MPI_LONG_LONG, &ag_report_file_disp[0], &subcomm_npieces[0],
                &s_displacements[0], MPI_LONG_LONG, 0, sub_report_comm);

    ag_report_size = 0;

    if (is_aggregator) {
        std::vector<MPI_Aint> file_disp;
        std::vector<MPI_Datatype> data_types;

        file_disp.resize(total_subcomm_npieces + 2);
        data_types.resize(total_subcomm_npieces + 2);

        ag_report_sizes[0] = 1;
        ag_report_sizes[total_subcomm_npieces + 1] = 1;

        file_disp[0] = 0;
        file_disp[total_subcomm_npieces + 1] = 0;

        data_types[0] = MPI_LB;
        data_types[total_subcomm_npieces + 1] = MPI_UB;

        size_t offset = 0;

        // first build offset index vector
        std::vector<OffsetIndexPairType> ofsetIndex;

        for (int i = 0; i < total_subcomm_npieces; i++) {
            ofsetIndex.push_back(std::make_pair(ag_report_file_disp[i], i));
        }

        // sort now
        std::sort(ofsetIndex.begin(), ofsetIndex.end(), comparator);
        std::vector<int> indexOrder;

        for (int i = 0; i < total_subcomm_npieces; i++) {
            indexOrder.push_back(ofsetIndex[i].second);
        }

        MPI_Aint displacementTotal = 0;
        std::vector<int> sizes;
        size_t k = 0;

        // just to set size to total_subcomm_npieces +2 and get 0 and last element initialized
        sizes = ag_report_sizes;

        MPI_Aint mem_offset = 0;

        for (size_t i = 0; i < sub_report_comm_size(); i++) {
            for (size_t j = 0; j < subcomm_npieces[i]; j++) {
                int order = indexOrder[k];
                file_disp[k + 1] = ag_report_file_disp[order];
                sizes[k + 1] = ag_report_sizes[order + 1];

                ag_report_mem_disp[order] = mem_offset;
                mem_offset += ag_report_sizes[order + 1];

                ag_report_size += ag_report_sizes[order + 1];

                displacementTotal = file_disp[k + 1] + ag_report_sizes[order + 1];
                data_types[k + 1] = MPI_BYTE;
                k++;
            }
        }

        subcomm_report_size = ag_report_size;

        MPI_Aint max_displacementTotal = 0;
        MPI_Allreduce(&displacementTotal, &max_displacementTotal, 1, MPI_AINT, MPI_MAX, aggregator_comm);

#ifdef DEBUG
        if (max_displacementTotal == displacementTotal) {
            printf(" MAX AGGREGATOR DISPLACEMENT %ld (+ %ld = %ld)\n", displacementTotal, file_offset,
                   displacementTotal + file_offset);
        }
#endif

        file_disp[total_subcomm_npieces + 1] = max_displacementTotal;

        CHECK_ERROR(MPI_Type_struct(total_subcomm_npieces + 2, &sizes[0], &file_disp[0], &data_types[0], &filetype));
        CHECK_ERROR(MPI_Type_commit(&filetype));
        CHECK_ERROR(MPI_File_open(aggregator_comm, (char*)filename.c_str(), MPI_MODE_WRONLY, MPI_INFO_NULL, &fh));
        CHECK_ERROR(MPI_File_set_view(fh, file_offset, MPI_BYTE, filetype, "native", MPI_INFO_NULL));
        CHECK_ERROR(MPI_Type_free(&filetype));
    }

    // provide each rank displacement in aggregators memory
    MPI_Scatterv(&ag_report_mem_disp[0], &subcomm_npieces[0], &s_displacements[0], MPI_AINT, &sender_displacements[0],
                 npieces, MPI_AINT, 0, sub_report_comm);

    // everyone one knows total size in subcomm to calculate
    // datatype used for mpi_put in window
    MPI_Bcast(&subcomm_report_size, 1, MPI_LONG, 0, sub_report_comm);
}

void ReportWriter::create_sender_type(MPI_Datatype& type, int nsteps) {
    std::vector<int> sizes;
    std::vector<MPI_Aint> displacements;

    sizes.resize(sender_npieces * nsteps);
    displacements.resize(sender_npieces * nsteps);

    for (int i = 0; i < nsteps; i++) {
        for (int j = 0; j < sender_npieces; j++) {
            // report sizes remain same for multiple time steps
            sizes[i * sender_npieces + j] = sender_sizes[j];
            // when there are multiple steps, we have to add step_number * report_size_of_aggregator
            displacements[i * sender_npieces + j] = sender_displacements[j] + i * subcomm_report_size;
        }
    }

    CHECK_ERROR(MPI_Type_create_hindexed(sender_npieces * nsteps, &sizes[0], &displacements[0], MPI_BYTE, &type));
    CHECK_ERROR(MPI_Type_commit(&type));
}

void ReportWriter::finalize() {
    MPI_Comm_free(&sub_report_comm);
    MPI_Comm_free(&aggregator_comm);
    if (is_aggregator)
        MPI_File_close(&fh);
}

void ReportWriter::setup_view(const int* sizes,
                              const long long* displacements,
                              int npieces,
                              MPI_Offset offset,
                              long buffer_size) {
    max_report_buffer_size = buffer_size;
    file_offset = offset;
    sender_npieces = npieces;
    sender_sizes = std::vector<int>(sizes, sizes + npieces);

    report_size = 0;
    for (int i = 0; i < npieces; i++) {
        report_size += sizes[i];
    }

    // once we calculate report_size and know buffer size then we can setup subcomms
    setup_communicators();

    setup_file_view(sizes, displacements, npieces);
}

void ReportWriter::print_stat(int nsteps) {
    if (ag_report_size * nsteps > INT_MAX) {
        printf("ERROR : Aggregator report buffer size > INT_MAX (overflow of 4GB on rank %d) \n", report_comm_rank);
        abort();
    }

    if (is_aggregator) {
        MPI_Offset local_size = ag_report_size * nsteps;
        MPI_Offset min_aggregator_size = 0;
        MPI_Offset max_aggregator_size = 0;

        MPI_Allreduce(&local_size, &min_aggregator_size, 1, MPI_OFFSET, MPI_MIN, aggregator_comm);
        MPI_Allreduce(&local_size, &max_aggregator_size, 1, MPI_OFFSET, MPI_MAX, aggregator_comm);

        if (verbose) {
            printf(" AGGREGATOR MAX SIZE  %ld  MIN SIZE %ld\n", max_aggregator_size, min_aggregator_size);
        }
    }
}

void ReportWriter::write(void* data, int nsteps) {
    MPI_Status status;
    MPI_Datatype sender_type;
    void* aggregated_data;

    print_stat(nsteps);

    if (verbose) {
        printf(" STARTED DATA AGGREGATION \n");
    }

    create_sender_type(sender_type, nsteps);

    MPI_Win win;

#ifdef MPI3
    CHECK_ERROR(
        MPI_Win_allocate(MPI_Aint(ag_report_size * nsteps), 1, MPI_INFO_NULL, sub_report_comm, &aggregated_data, &win));
#else
    if(ag_report_size*nsteps) {
        aggregated_data = calloc(ag_report_size, nsteps);
        if(aggregated_data == NULL) {
            printf("ERROR : Memory allocation failed for aggregation\n");
            abort();
        }
    }
    CHECK_ERROR(
        MPI_Win_create(aggregated_data, MPI_Aint(ag_report_size * nsteps), 1, MPI_INFO_NULL, sub_report_comm, &win));
#endif

    CHECK_ERROR(MPI_Win_fence(0, win));
    CHECK_ERROR(MPI_Put(data, report_size * nsteps, MPI_BYTE, 0, 0, 1, sender_type, win));
    CHECK_ERROR(MPI_Win_fence(0, win));

    if (verbose) {
        printf(" FINISHED DATA AGGREGATION \n");
    }

    if (is_aggregator) {
        CHECK_ERROR(MPI_File_write_all(fh, aggregated_data, ag_report_size * nsteps, MPI_BYTE, &status));
    }

    if (verbose) {
        printf(" FINISHED DATA WRITING \n");
    }

#ifndef MPI3
    if (ag_report_size*nsteps)
        free(aggregated_data);
#endif

    CHECK_ERROR(MPI_Type_free(&sender_type));
    CHECK_ERROR(MPI_Win_free(&win));
}
