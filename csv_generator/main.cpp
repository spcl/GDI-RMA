// Copyright (c) 2021 ETH-Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#include "command_line.h"
#include "mpi.h"

extern "C" {
#include "graph.h"
#include "lpg_graph500_csv.h"
}

int main( int argc, char* argv[] ) {
  MPI_Init(&argc, &argv);

  CLBase cli(argc, argv, "LPG-based Graph500 CSV Generator");
  if (!cli.ParseArgs()) {
    return -1;
  }

  /**
    load edges from file or generate them via the Graph500 Kronecker module
   */
  MPI_Offset edge_count;
  packed_edge* edges;
  uint64_t nglobalverts;

  std::string filename = cli.filename();
  bool startAtOne = cli.startAtOne();

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if( !(filename.empty()) ) {
    /* command line parameter */
    nglobalverts = cli.nglobalverts();

    if( nglobalverts == 0 ) {
      if( rank == 0 ) {
        fprintf( stderr, "verts = number of vertices\nThe number of vertices should not be zero.\n" );
      }
      MPI_Abort( MPI_COMM_WORLD, 1 );
    }

    loadEdgesFromEdgeListFile( filename.c_str(), startAtOne, &edge_count, &edges );
  } else {
    /* command line parameter */
    uint32_t SCALE = cli.scale();
    uint32_t edge_factor = cli.edgefactor();

    /**
      sanity check for the command line parameters
     */
    if( edge_factor == 0 ) {
      if( rank == 0 ) {
        fprintf( stderr, "edgefactor = (# edges) / (# vertices) = .5 * (average vertex degree) [integer]\nedgefactor shouldn't be zero.\n" );
      }
      MPI_Abort( MPI_COMM_WORLD, 1 );
    }

    if( SCALE == 0 ) {
      if( rank == 0 ) {
        fprintf( stderr, "SCALE = log_2(# vertices) [integer]\nSCALE shouldn't be zero.\n" );
      }
      MPI_Abort( MPI_COMM_WORLD, 1 );
    }

    if( startAtOne ) {
      if( rank == 0 ) {
        fprintf( stderr, "startAtOne (-o) should only be used, when edges are loaded from a file. Parameter is ignored.\n" );
      }
    }

    nglobalverts = (uint64_t)(1) << SCALE;

    generateEdgeGraph500Kronecker( edge_factor, SCALE, &edge_count, &edges );
  }

#ifdef GDEBUG
  uint64_t local_edge_count, global_edge_count;
  local_edge_count = edge_count;
  MPI_Reduce( &local_edge_count, &global_edge_count, 1, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD );
  if( rank == 0 ) {
    printf( "%" PRIu64 " edges in buffer.\n", global_edge_count );
  }
#endif

  std::string output_prefix = cli.output_prefix();

  lpg_graph500_csv( nglobalverts, edge_count, edges, output_prefix.c_str() );

  MPI_Finalize();

  return 0;
}
