#ifndef _REPORT_UTILS_
#define _REPORT_UTILS_

#include <stdio.h>
#include <string>
#include <iostream>
#include <mpi.h>
#include <vector>

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

void loadReportData(std::string inputFile, ReportingData& rdata,
                    int rank,
                    int duplicate);

void demo_program_write_header(MPI_Comm comm_world,
                               ReportingData& rdata,
                               std::string report_filename);

void demo_program_write_data(MPI_Comm comm_world,
                             ReportingData& rdata,
                             std::string report_filename,
                             int duplicate);

void CHECK_ERROR(int error);

void calculat_total_file_size(int ngids,
                              MPI_Offset report_size,
                              int buffer_steps,
                              int comm_rank,
                              MPI_Comm comm);

#endif
