#ifndef _MEM_UTILS_
#define _MEM_UTILS_

#include <stdio.h>
#include <string>
#include <iostream>
#include <mpi.h>

// heap size in MBs
double memUsageInfo(const char* msg = NULL);

struct ReportingData {
    int ngids;
    int ntotaldata;
    int len;
    int* gids;
    int* sizes;
    long long* mappingDisp;
    long long* disp;
    int* mappingCount;
    char* fakemapping;
    char* fakedata;
    ~ReportingData();
};

void loadReportData(std::string inputFile, ReportingData& rdata, int rank);
void demo_program_write_header(MPI_Comm comm_world,
                               ReportingData& rdata,
                               std::string report_filename);
void demo_program_write_data(MPI_Comm comm_world,
                             ReportingData& rdata,
                             std::string report_filename);
void check_mpi_error(int& error);

#endif
