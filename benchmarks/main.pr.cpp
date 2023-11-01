// Copyright (c) 2023 ETH-Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#include <assert.h>

#include "command_line.h"
#include "rma.h"

extern "C" {
#include "benchmark.h"
#include "data_scheme_1.h"
#include "graph.h"
}

int main( int argc, char* argv[] ) {

  CLBase cli(argc, argv, "GDI Benchmark");
  if (!cli.ParseArgs()) {
    return -1;
  }

  RMA_Init( &argc, &argv );

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  /**
    load edges from file or generate them via the Graph500 Kronecker module
   */
  MPI_Offset edge_count;
  packed_edge* edges;
  uint64_t nglobalverts;

  std::string filename = cli.filename();
  bool startAtOne = cli.startAtOne();

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

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

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

  /**
    create graph database
   */
  int status;

  status = GDI_Init( &argc, &argv );
  assert( status == GDI_SUCCESS );

  GDI_Database db;
  GDI_Label* vlabels;
  GDI_Label* elabels;
  GDI_PropertyType* ptypes;

  /* command line parameter */
  uint32_t block_size = cli.blocksize();
  uint64_t memory_size = cli.memorysize();
  bool directed = cli.directed();

  createGraphDatabase( block_size, memory_size, nglobalverts, edge_count, edges, directed, &db, &vlabels, &elabels, &ptypes );

  /**
    run PageRank benchmark:
    * number of runs is hardcoded to 10
   */
  if( !directed ) {
    if( rank == 0 ) {
      fprintf( stderr, "PageRank algorithm currently only supports directed edges.\n" );
    }
    MPI_Abort( MPI_COMM_WORLD, 1 );
  }

  uint32_t rcount = 10;
  uint32_t iterations = cli.iterations();
  double damping_factor = cli.dampingfactor();
  benchmark_pagerank( db, /* vlabels, */ nglobalverts, damping_factor, iterations, rcount );

  /**
    clean up
   */
  status = GDI_FreeDatabase( &db );
  assert( status == GDI_SUCCESS );

  status = GDI_Finalize();
  assert( status == GDI_SUCCESS );

  data_scheme_1_finalize();
  free( vlabels );
  free( elabels );
  free( ptypes );

  RMA_Finalize();

  return 0;
}
