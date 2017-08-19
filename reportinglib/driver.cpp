#include <math.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <string.h>
#include "utils.h"

//-----------------------------------------------------------------------------------------------
// Open file with MPI (delete old file if require)
//-----------------------------------------------------------------------------------------------

int open(std::string filename,
         MPI_File& fh,
         MPI_Comm fileComm,
         int amode,
         MPI_Info info = MPI_INFO_NULL) {
    if (amode & MPI_MODE_CREATE) {
        FILE* quickClear = fopen(filename.c_str(), "r");
        if (quickClear) {
            fclose(quickClear);
            MPI_File_delete((char*)filename.c_str(), MPI_INFO_NULL);
            quickClear = NULL;
        }
    }

    int result = MPI_File_open(fileComm, (char*)filename.c_str(), amode, info, &fh);
    return result;
}

//-----------------------------------------------------------------------------------------------
// MPI-IO focued functions
//-----------------------------------------------------------------------------------------------

int prepareFileView(MPI_File& fh,
                    std::string report_filename,
                    MPI_Offset position_to_write,
                    int* sizes,
                    long long* filedisp,
                    int len,
                    MPI_Comm comm_report) {
    int numcells = len;
    // static MPI_Offset position_to_write = 0;

    // generate the view
    int* length = new int[numcells + 2];
    MPI_Aint* disp = new MPI_Aint[numcells + 2];
    MPI_Datatype* type = new MPI_Datatype[numcells + 2];

    length[0] = 1;
    length[numcells + 1] = 1;

    disp[0] = 0;              // lower bound
    disp[numcells + 1] = -1;  // total bytes consumed by save data; to be filled in later

    type[0] = MPI_LB;
    type[numcells + 1] = MPI_UB;

    // Generate the view - note that viewIndex starts at 1 due to MPI using index
    // 0 for sentinal values
    int viewIndex = 1;
    MPI_Offset bufferOffset = 0, displacementTotal = 0;

    int rank, size;
    MPI_Comm_size(comm_report, &size);
    MPI_Comm_rank(comm_report, &rank);

    for (int cellIndex = 0; cellIndex < numcells; cellIndex++) {
        bufferOffset += sizes[cellIndex];

        MPI_Offset displacementcell = filedisp[cellIndex];

        length[viewIndex] = sizes[cellIndex];
        disp[viewIndex] = displacementcell;

        // displacementTotal += sizes[cellIndex];
        displacementTotal = disp[viewIndex] + sizes[cellIndex];

        type[viewIndex] = MPI_BYTE;

        viewIndex++;
    }

    // final displacement contains length of total data which we can now store for
    // save data (handled normal reports above)
    disp[viewIndex] = displacementTotal;

    // Apply the view
    MPI_Datatype filetype;  // file's view of frame.
    MPI_Type_struct(numcells + 2, length, disp, type, &filetype);
    MPI_Type_commit(&filetype);

    // reopen the file
    int error = open(report_filename, fh, comm_report, MPI_MODE_WRONLY);
    check_mpi_error(error);

    error = MPI_File_set_view(fh, position_to_write, MPI_BYTE, filetype, "native", MPI_INFO_NULL);
    check_mpi_error(error);

    MPI_Type_free(&filetype);
    delete[] length;
    delete[] disp;
    delete[] type;

    return 0;
}

//-----------------------------------------------------------------------------------------------
// Scenario preparation functions
//-----------------------------------------------------------------------------------------------

void loadReportData(std::string inputFile, ReportingData& rdata, int rank) {
    FILE* dataLoad = fopen(inputFile.c_str(), "r");
    if (!dataLoad) {
        if (rank == 0)
            fprintf(stderr, "error on %s open\n", inputFile.c_str());
        exit(0);
    }

    // if size < oldsize -> we miss the other data
    // if size > oldsize -> extra cpus will use 0 (this should not deadlock
    // anymore)
    int oldsize;
    int iRes;
    iRes = fscanf(dataLoad, "%d", &oldsize);

    rdata.fakemapping = NULL;
    rdata.fakedata = NULL;
    rdata.ntotaldata = 0;

    // cell data
    rdata.len = 0;
    int cell_pieces = 0;
    rdata.gids = NULL;
    rdata.sizes = NULL;
    rdata.mappingDisp = NULL;
    rdata.disp = NULL;
    rdata.ngids = 0;

    long long int mappingMaxDisp = 0;

    for (int i = 0; i < oldsize; i++) {
        iRes = fscanf(dataLoad, "%d", &cell_pieces);
        if (i == rank) {
            rdata.len = cell_pieces;
            rdata.gids = new int[rdata.len];
            rdata.sizes = new int[rdata.len];
            rdata.mappingDisp = new long long[rdata.len];
            rdata.disp = new long long[rdata.len];
            for (int j = 0; j < rdata.len; j++) {
                iRes = fscanf(dataLoad, "%d %d %lld %lld", &rdata.gids[j], &rdata.sizes[j],
                              &rdata.mappingDisp[j], &rdata.disp[j]);
                rdata.ntotaldata += rdata.sizes[j];
                rdata.mappingDisp[j] *= 4;  // in the original program, mapping is handled as
                                            // ints.  We will handle everything as bytes
                if (rdata.mappingDisp[j] > mappingMaxDisp) {
                    mappingMaxDisp = rdata.mappingDisp[j];
                }
            }
        } else {
            for (int j = 0; j < cell_pieces; j++) {
                iRes = fscanf(dataLoad, "%*d %*d %*lld %*lld");
            }
        }
        rdata.ngids += cell_pieces;
    }
    fclose(dataLoad);

    long long int mappingGlobalMaxDisp = 0;
    MPI_Allreduce(&mappingMaxDisp, &mappingGlobalMaxDisp, 1, MPI_LONG_LONG, MPI_MAX,
                  MPI_COMM_WORLD);

    if (rank == 0) {
        printf("max mapping offset is %lld\n", mappingGlobalMaxDisp);
    }

    // populate fake data
    // TODO: maybe also make it so that instead split cells owuld get same char
    // and would then appear together in final file
    char garbage[65] = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ!?";

    // header info - mapping each piece to total bytes
    rdata.mappingCount = new int[rdata.len];
    rdata.fakemapping = new char[rdata.len * sizeof(int) + 1];
    rdata.fakemapping[rdata.len * sizeof(int)] = 0;

    int fakeOffset = 0;
    for (int j = 0; j < rdata.len; j++) {
        rdata.mappingCount[j] = sizeof(int);
        snprintf(&rdata.fakemapping[fakeOffset], sizeof(int) + 1, "%04X", rank);
        fakeOffset += sizeof(int);
    }

    rdata.fakemapping[rdata.len * sizeof(int)] = 0;

    if (mappingGlobalMaxDisp == mappingMaxDisp) {
        rdata.fakemapping[rdata.len * sizeof(int) - 1] = '\n';
    }

    rdata.fakedata = new char[rdata.ntotaldata + 1];
    fakeOffset = 0;

    // TODO: print meta data into the start of the line.  So far, that meta data
    // has been shorter than the data length :|
    for (int j = 0; j < rdata.len; j++) {
        int adjust =
            sprintf(&rdata.fakedata[fakeOffset], "%d %d - ", rdata.gids[j], rdata.sizes[j]);
        fakeOffset += adjust;

        char selection = garbage[rdata.gids[j] % 64];
        for (int k = adjust; k < rdata.sizes[j] - 1; k++) {
            rdata.fakedata[fakeOffset++] = selection;
        }
        rdata.fakedata[fakeOffset++] = '\n';
    }

    rdata.fakedata[rdata.ntotaldata] = 0;  // we should never print this data, but add the null
                                           // terminator in case we mistakely try
}

void demo_program_write_header(MPI_Comm comm_world,
                               ReportingData& rdata,
                               std::string report_filename) {
    int rank;
    MPI_Comm_rank(comm_world, &rank);

    MPI_Comm comm_report;
    MPI_Comm_split(comm_world, (rdata.len > 0), 0, &comm_report);

    MPI_File fh;
    MPI_Status status;

    if (rank == 0) {
        int error = open(report_filename, fh, MPI_COMM_SELF, MPI_MODE_CREATE | MPI_MODE_WRONLY);
        check_mpi_error(error);
        MPI_File_close(&fh);
    }

    // global writing
    MPI_Offset position_to_write = 0;

    prepareFileView(fh, report_filename, position_to_write, rdata.mappingCount, rdata.mappingDisp,
                    rdata.len, comm_report);
    MPI_File_write_all(fh, rdata.fakemapping, sizeof(int) * rdata.len, MPI_BYTE, &status);

    MPI_File_close(&fh);
    MPI_Comm_free(&comm_report);
}

void demo_program_write_data(MPI_Comm comm_world,
                             ReportingData& rdata,
                             std::string report_filename) {
    int rank;
    MPI_Comm_rank(comm_world, &rank);

    MPI_Comm comm_report;
    MPI_Comm_split(comm_world, (rdata.len > 0), 0, &comm_report);

    MPI_File fh;
    MPI_Status status;

    // after the mapping, the file advances to sizeof(int)*(sum of len across nodes)
    MPI_Offset start_offset = sizeof(int) * rdata.ngids;
    prepareFileView(fh, report_filename, start_offset, rdata.sizes, rdata.disp, rdata.len,
                    comm_report);
    MPI_File_write_all(fh, rdata.fakedata, rdata.ntotaldata, MPI_BYTE, &status);

    MPI_File_close(&fh);
    MPI_Comm_free(&comm_report);
}
