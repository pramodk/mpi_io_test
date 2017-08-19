#include <vector>
#include "ReportWriter.h"
#include "utils.h"

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int rank, comm_size;

    size_t REPORT_BUFFER_SIZE = 8;

    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (argc < 2) {
        if (rank == 0)
            std::cerr << "Error : pass data dump file as an argument \n";
        exit(1);
    }

    std::string inputFile(argv[1]);

    if (argc == 3)
        REPORT_BUFFER_SIZE = atoi(argv[2]);

    memUsageInfo("START");

    ReportingData rdata;
    loadReportData(inputFile, rdata, rank);

    memUsageInfo("AFTER DATA LOAD");

    std::string report1("demo_program_report_1.bbp");
    demo_program_write_header(MPI_COMM_WORLD, rdata, report1);
    demo_program_write_data(MPI_COMM_WORLD, rdata, report1);

    memUsageInfo("AFTER DEMO PROGRAM");

    std::string report2("demo_program_report_2.bbp");
    int num_bytes = rdata.ntotaldata;

    ReportWriter writer("synapse", report2, MPI_COMM_WORLD);

    int num_report_steps = writer.number_steps_can_buffer(num_bytes, REPORT_BUFFER_SIZE);

    printf("R.%4d REPORT %5d BYTES BUFFER %6zu BYTES BUFFER STEPS %d \n", rank, num_bytes,
           REPORT_BUFFER_SIZE, num_report_steps);

    size_t AGGREGATOR_BUFFER_SIZE = 2*REPORT_BUFFER_SIZE;

    writer.setup_view(rdata.sizes, rdata.disp, rdata.len, AGGREGATOR_BUFFER_SIZE);

    writer.finalize();

    MPI_Barrier(MPI_COMM_WORLD);

    memUsageInfo("END");

    MPI_Finalize();
}
