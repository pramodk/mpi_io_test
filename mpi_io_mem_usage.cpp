#include <mpi.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <math.h>
#include <iostream>
#include <string.h>
#include "utils.h"


int main( int argc, char** argv )
{
    int rank, size;
    MPI_Init( &argc, &argv );

    MPI_Comm_size( MPI_COMM_WORLD, &size );
    MPI_Comm_rank( MPI_COMM_WORLD, &rank );

    int *mappingCount=NULL;
    char *mappingData=NULL;

    // cell data
    int len=0, currlen=0, *gids=NULL, *sizes=NULL;
    long long *mappingDisp=NULL, *disp=NULL;
    int ngids = 0;

    srand(time(NULL) + 1);

    len = randomNumber(1, 10);


    memUsageInfo("Done data load" );

    if( rank == 0 ) {
        printf( "Total number of ranks = %d\n", ngids );
    }

    int num_bytes = sizeof(int) * len; 

    std::cout << " I am rank " << rank  << " and allocating " << num_bytes << "\n";

    MPI_File fh;
    MPI_Status status;

    int result = MPI_File_open(MPI_COMM_WORLD, "report.bbp", MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);

    if( result != MPI_SUCCESS )
        std::cerr << "Error opening the file " << std::endl;


    MPI_Barrier( MPI_COMM_WORLD );

    // global writing
    //MPI_Offset position_to_write = 0;

    memUsageInfo( "Begin global file operations" );

    mappingData = new char[num_bytes];

    //prepareFileView( fh, position_to_write, mappingCount, mappingDisp, len, comm_report );
    //memUsageInfo( "created mapping view" );

    //MPI_File_write_all(fh, mappingData, sizeof(int)*len, MPI_BYTE, &status);

    MPI_File_write_at(fh, rank*num_bytes, mappingData, num_bytes, MPI_BYTE, &status);


    memUsageInfo( "wrote mapping" );;

    MPI_File_close(&fh);


    MPI_Finalize();
}


