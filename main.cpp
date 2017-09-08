#include <vector>
#include <string.h>
#include <stdlib.h>
#include "ReportWriter.h"
#include "reportinglib/report_utils.h"

int main(int argc, char** argv) {
    int comm_rank, comm_size;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &comm_rank);

    if (argc < 2) {
        if (comm_rank == 0)
            std::cerr << "Error : pass data dump file as an argument \n";
        exit(1);
    }

    std::string inputFile(argv[1]);

    size_t REPORT_BUFFER_SIZE = 4 * 1024 * 1024;
    int BUFFERED_REPORTING_STEPS = 1;

    // report buffer suze in bytes
    if (argc >= 3)
        REPORT_BUFFER_SIZE = atoi(argv[2]);

    // number of times to duplicate input data dump file
    // this is used for testing different report timesteps
    // which are being accumulation in reportinglib
    if (argc >= 4)
        BUFFERED_REPORTING_STEPS = atoi(argv[3]);

    memUsageInfo("START");

    // load data dump file, we will use BUFFERED_REPORTING_STEPS to
    // duplicate input data for testing
    ReportingData rdata;
    loadReportData(inputFile, rdata, comm_rank, BUFFERED_REPORTING_STEPS);

    // print file size
    calculat_total_file_size(rdata.ngids, rdata.ntotaldata, BUFFERED_REPORTING_STEPS, comm_rank, MPI_COMM_WORLD);

    memUsageInfo("AFTER DATA LOAD");

    std::string report1("demo_program_report_1.bbp");

    // write report header and then report data which is duplicated BUFFERED_REPORTING_STEPS times
    demo_program_write_header(MPI_COMM_WORLD, rdata, report1);
    demo_program_write_data(MPI_COMM_WORLD, rdata, report1, BUFFERED_REPORTING_STEPS);

    memUsageInfo("AFTER DEMO PROGRAM");

    std::string report2("demo_program_report_2.bbp");

    // todo: check size limitations in reportinglib (not going to write more than 4Gb per rank)
    int report_size = rdata.ntotaldata;

    // for each report type there is one instace of ReportWriter
    ReportWriter rwriter(report2, MPI_COMM_WORLD, false);

    // given report_size and REPORT_BUFFER_SIZE on each rank, how many maximum
    // report steps can be buffered globally. We are NOT using this in this example
    // but could be used in reportinglib
    int max_buffer_steps = rwriter.number_steps_can_buffer(report_size, REPORT_BUFFER_SIZE);

#ifdef DEBUG
    printf("R.%4d REPORT SIZE %5d, BUFFER %6zu, BUFFER STEPS %d\n", comm_rank, report_size, REPORT_BUFFER_SIZE,
           max_buffer_steps);
#endif

    // write header using reportinglib function
    demo_program_write_header(MPI_COMM_WORLD, rdata, report2);

    // the offset at which we will write actual report data
    MPI_Offset start_position = sizeof(int) * rdata.ngids;

    // setup view : here we also compute aggregators i.e. writers so that they can write
    // REPORT_BUFFER_SIZE bytes of data. For example, suppose we have REPORT_BUFFER_SIZE=16KB
    // and ranks have following sizes for single reporting step:
    // Rank 0: 8KB, Rank 1: 5KB, Rank 2: 6KB, Rank 3: 12KB, Rank 4: 1KB, Rank 5: 2KB
    // In this case Rank 0 is an aggregator with 13KB, Rank 2 with 6KB, Rank 3 with 15KB
    // Note that the view is calculated considering single reporting timestep
    rwriter.setup_view(rdata.sizes, rdata.disp, rdata.len, start_position, REPORT_BUFFER_SIZE);

    // each rank has data in fakedata in which they have accumulated BUFFERED_REPORTING_STEPS
    // steps (which could be different from max_buffer_steps).
    rwriter.write(rdata.fakedata, BUFFERED_REPORTING_STEPS);

    // close file and delete comms, data types
    rwriter.finalize();

    memUsageInfo("END");
    MPI_Finalize();
}
