// Copyright (c) 2022 ETH-Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "make_graph.h"
#include "utils.h"

#include "mpi.h"

/**
  code of determineByteRange modeled after respective function from the
  dist-graph-convert tool of Galois C++ library

  license agreement from the file
  Galois/tools/dist-graph-convert/dist-graph-convert-helpers.cpp:

  This file belongs to the Galois project, a C++ library for exploiting
  parallelism. The code is being released under the terms of the 3-Clause BSD
  License (a copy is located in LICENSE.txt at the top-level directory).

  Copyright (C) 2018, The University of Texas at Austin. All rights reserved.
  UNIVERSITY EXPRESSLY DISCLAIMS ANY AND ALL WARRANTIES CONCERNING THIS
  SOFTWARE AND DOCUMENTATION, INCLUDING ANY WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR ANY PARTICULAR PURPOSE, NON-INFRINGEMENT AND WARRANTIES OF
  PERFORMANCE, AND ANY WARRANTY THAT MIGHT OTHERWISE ARISE FROM COURSE OF
  DEALING OR USAGE OF TRADE.  NO WARRANTY IS EITHER EXPRESS OR IMPLIED WITH
  RESPECT TO THE USE OF THE SOFTWARE OR DOCUMENTATION. Under no circumstances
  shall University be liable for incidental, special, indirect, direct or
  consequential damages or loss of profits, interruption of business, or
  related expenses which may arise from use of Software or Documentation,
  including but not limited to those resulting from defects in Software and/or
  Documentation, or loss or inaccuracy of data of any kind.

  original license:
  The 3-Clause BSD License
  Copyright 2018 The University of Texas at Austin

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are met:

  1. Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

  2. Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

  3. Neither the name of the copyright holder nor the names of its contributors
  may be used to endorse or promote products derived from this software without
  specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
  FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

/**
  startByte points afterwards to first Byte to read
  endByte points afterwards to the first Bytes not to read (non-inclusive): last Byte to read + 1
 */
void determineByteRange( FILE* fp, int rank, int commsize, MPI_Comm comm, uint64_t* startByte, uint64_t* endByte ) {
  /**
    file size
   */
  if( fseek( fp, 0, SEEK_END ) == -1 ) {
    fprintf( stderr, "%i: Could not seek.\n", rank );
    MPI_Abort( comm, -1 );
  }

  long temp = ftell( fp );
  if( temp == -1L ) {
    fprintf( stderr, "%i: ftell didn't work\n", rank );
    MPI_Abort( comm, -1 );
  }

  uint64_t fileSize = temp; /* to avoid compiler warnings */

  rewind( fp );

  char dummy[10000];
#if 0
  /**
    omit the first line, since it only contains a heading
   */

  fgets( dummy, 10000, fp );
#endif

  uint64_t initialStart = ftell( fp );
  uint64_t initialEnd = fileSize;

  /**
    determine initial range
   */
  uint64_t distance = initialEnd - initialStart;
  uint64_t range_per_process = (distance + commsize - 1 /* round up */) / commsize;
  if( range_per_process == 0 ) {
    range_per_process = 1;
  }
  uint64_t A = range_per_process * rank;
  if( A > distance ) {
    A = distance;
  }
  uint64_t B = range_per_process * (rank + 1 );
  if( B > distance ) {
    B = distance;
  }
  initialStart += A;
  if( B != distance ) {
    initialEnd = initialStart;
    initialEnd += (B - A);
  }

  /**
    determine the actual range, so we start and end
    at linebreaks
   */
  bool startGood = false;
  if (initialStart != 0) {
    /**
      good starting point if the prev char was a new line
      (i.e. this start location is the beginning of a line)
     */
    fseek( fp, initialStart-1, SEEK_SET );
    int testChar = fgetc( fp );
    if( testChar == '\n' ) {
      startGood = true;
    }
  } else {
    /**
      start is 0; perfect starting point, need no adjustment
     */
    startGood = true;
  }

  bool endGood = false;
  if (initialEnd != fileSize && initialEnd != 0) {
    /**
      good end point if the prev char was a new line (i.e. this end
      location is the beginning of a line; recall non-inclusive)
     */
    fseek( fp, initialEnd-1, SEEK_SET );
    int testChar = fgetc( fp );
    if( testChar == '\n' ) {
      endGood = true;
    }
  } else {
    endGood = true;
  }

  *startByte = initialStart;
  if( !startGood ) {
    /**
      find next new line
     */
    fseek( fp, initialStart, SEEK_SET );
    fgets( dummy, 10000, fp );
    *startByte = ftell( fp );
  }

  *endByte = initialEnd;
  if( !endGood ) {
    /**
      find next new line
     */
    fseek( fp, initialEnd, SEEK_SET );
    fgets( dummy, 10000, fp );
    *endByte = ftell( fp );
  }
}


/**
  code of this function is mostly borrowed from Galois dist-graph-convert
 */
void loadEdgesFromEdgeListFile( const char* inputFilename, const bool startAtOne, MPI_Offset* edge_count, packed_edge** edges ) {

  FILE* fp;

  int rank, commsize;
  MPI_Comm_rank( MPI_COMM_WORLD, &rank );
  MPI_Comm_size( MPI_COMM_WORLD, &commsize );

  char* line = malloc( 10000 * sizeof(char) );

  fp = fopen( inputFilename, "r" );
  if( fp == NULL ) {
    fprintf( stderr, "%i: Could not open %s\n", rank, inputFilename );
    MPI_Abort( MPI_COMM_WORLD, -1 );
  }

  /**
    determine range offsets to read
   */
  uint64_t startByte, endByte;
  determineByteRange( fp, rank, commsize, MPI_COMM_WORLD, &startByte, &endByte );

  fseek( fp, startByte, SEEK_SET );

  /**
    determine number of edges to read
   */
  *edge_count = 0;
  while( (uint64_t)ftell(fp) != endByte ) {
    fgets( line, 10000, fp );
    (*edge_count)++;
  }

  *edges = (packed_edge*) xmalloc( (*edge_count) * sizeof(packed_edge) );

  /* reset file pointer to the start of the range to read */
  fseek( fp, startByte, SEEK_SET );

  /**
    read edges
   */
  uint64_t offset = 0;
  while( (uint64_t)ftell(fp) != endByte ) {
    fgets( line, 10000, fp );

    char* field_startptr;
    char* field_endptr;
    int64_t origin = strtoll( line, &field_endptr, 10 /* base */ );
    assert( field_endptr != NULL ); /* field_endptr points to the position of ' ' */

    field_startptr = field_endptr + 1;

    int64_t target = strtoll( field_startptr, &field_endptr, 10 /* base */ );

    if (startAtOne) {
      origin--;
      target--;
    }

    write_edge( (*edges)+offset, origin, target );
    offset++;
  }

  /**
    clean up
   */
  free( line );

  fclose( fp );
}


void generateEdgeGraph500Kronecker( uint32_t edge_factor, uint32_t SCALE, MPI_Offset* edge_count, packed_edge** buf ) {
  int64_t nglobaledges = (int64_t)(edge_factor) << SCALE;

  int rank, commsize;
  MPI_Comm_rank( MPI_COMM_WORLD, &rank );
  MPI_Comm_size( MPI_COMM_WORLD, &commsize );

	/* Make the raw graph edges. */

  /* Spread the two 64-bit numbers into five nonzero values in the correct
   * range. */
  uint_fast32_t seed[5];
	uint64_t seed1 = 2, seed2 = 3;
  make_mrg_seed(seed1, seed2, seed);

  /**
    generate the edges of the graph
   */
  int64_t nlocaledges = (nglobaledges + commsize - 1 /* round up */)/commsize;
  MPI_Offset start_edge_index = nlocaledges * rank < nglobaledges ? nlocaledges * rank : nglobaledges; /* minimum( nlocaledges * rank, nglobaledges ) */
  *edge_count = nglobaledges - start_edge_index < nlocaledges ? nglobaledges - start_edge_index : nlocaledges; /* minimum( nglobaledges - start_edge_index, nlocaledges ) */

  *buf = (packed_edge*)xmalloc((*edge_count) * sizeof(packed_edge));
  generate_kronecker_range(seed, SCALE, start_edge_index, start_edge_index + (*edge_count), *buf);
}
