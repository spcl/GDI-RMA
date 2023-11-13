// Copyright (c) 2023 ETH-Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "gda_lightweight_edges.h"

#include "make_graph.h"
#include "utils.h"

#include "data_scheme_1.h"
#include "queries.h"

#define PROPERTY_TYPE_COUNT 13

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


/**
  deallocates buf at the end
 */
void createGraphDatabase( uint32_t block_size, uint64_t memory_size, uint64_t nglobalverts, MPI_Offset edge_count, packed_edge* buf, bool directed,
  GDI_Database* graph_db, GDI_Label** vertexlabels, GDI_Label** edgelabels, GDI_PropertyType** propertytypes ) {
  int rank, commsize;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &commsize);

  uint64_t necessary_memory_size = nglobalverts * block_size / commsize;
  if( necessary_memory_size > memory_size ) {
    if( rank == 0 ) {
      fprintf( stderr, "Not enough memory to store all vertices in the database. Increase memory size.\n" );
    }
    MPI_Abort( MPI_COMM_WORLD, 1 );
  }

#ifdef GDEBUG
  for( int64_t i=0 ; i<edge_count ; i++ ) {
    int64_t origin = get_v0_from_edge( buf+i );
    int64_t target = get_v1_from_edge( buf+i );
    printf( "%i: %" PRId64 " -> %" PRId64 "\n", rank, origin, target );
  }
#endif

  /**
    distribute the generated edges to the processes that store the origin and
    the target vertices

    step 1: determine the necessary buffer size in Bytes for each process
    step 2a: start the distribution of the send count (non-blocking MPI_Alltoall)
    step 3: pack the buffers for each process
    step 2b: finish up the distribution of the send count
    step 4: distribute the edges (MPI_Alltoallv)
   */

  /**
    step 1: determine the necessary buffer size in Bytes for each process
   */
  int* send_count = calloc( commsize, sizeof(int) );
  if( send_count == NULL ) {
    fprintf( stderr, "%i - %s line %i: Not able to allocate memory\n", rank, (char*) __FILE__, __LINE__ );
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  /**
    vertex distribution:
    rank        0   1   2  ...  m
    UID:        0   1   2  ...  m
    UID:       m+1 m+2 m+3 ... 2m
    and so on
   */
  for( int64_t i=0 ; i<edge_count ; i++ ) {
    int64_t origin_vertex = get_v0_from_edge( buf+i );
    int64_t target_vertex = get_v1_from_edge( buf+i );

    int origin_process = origin_vertex % commsize;
    int target_process = target_vertex % commsize;

    assert( (origin_process >= 0) && (origin_process < commsize) );
    assert( (target_process >= 0) && (target_process < commsize) );

    send_count[origin_process] += sizeof(packed_edge);

    if( origin_process != target_process ) {
      send_count[target_process] += sizeof(packed_edge);
    }
  }

  /**
    step 2a: start the distribution of the send count
   */
  int* recv_count = malloc( commsize * sizeof(int) );
  if( recv_count == NULL ) {
    fprintf( stderr, "%i - %s line %i: Not able to allocate memory\n", rank, (char*) __FILE__, __LINE__ );
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  MPI_Request req;

  MPI_Ialltoall( send_count, 1, MPI_INT, recv_count, 1, MPI_INT, MPI_COMM_WORLD, &req );

  /**
    determine displacements
   */
  int* send_displacement = malloc( commsize * sizeof(int) );
  if( send_displacement == NULL ) {
    fprintf( stderr, "%i - %s line %i: Not able to allocate memory\n", rank, (char*) __FILE__, __LINE__ );
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  send_displacement[0] = 0;
  for( int i=1 ; i<commsize ; i++ ) {
    send_displacement[i] = send_displacement[i-1] + send_count[i-1];
  }

  size_t total_send_count = send_displacement[commsize-1] + send_count[commsize-1]; /* in Bytes */

  /**
    step 3: pack the buffers for each process
   */
  char* edge_distribution = malloc( total_send_count );
  if( edge_distribution == NULL ) {
    fprintf( stderr, "%i - %s line %i: Not able to allocate memory\n", rank, (char*) __FILE__, __LINE__ );
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  uint32_t* idx = calloc( commsize, sizeof(uint32_t) );
  if( idx == NULL ) {
    fprintf( stderr, "%i - %s line %i: Not able to allocate memory\n", rank, (char*) __FILE__, __LINE__ );
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  for( int64_t i=0 ; i<edge_count ; i++ ) {
    int64_t origin_vertex = get_v0_from_edge( buf+i );
    int64_t target_vertex = get_v1_from_edge( buf+i );

    int origin_process = origin_vertex % commsize;
    int target_process = target_vertex % commsize;

    assert( (origin_process >= 0) && (origin_process < commsize) );
    assert( (target_process >= 0) && (target_process < commsize) );

    /**
      copy edge into the process buffers
     */
#ifdef GDEBUG
    printf( "%i: write edge %" PRId64 " -> %" PRId64 " into buffer for process %i at index %lu\n", rank, origin_vertex, target_vertex, origin_process, idx[origin_process] / sizeof(packed_edge) );
#endif
    memcpy( edge_distribution+send_displacement[origin_process]+idx[origin_process], buf+i, sizeof(packed_edge) );
    idx[origin_process] += sizeof(packed_edge);

    if( origin_process != target_process ) {
#ifdef GDEBUG
      printf( "%i: write edge %" PRId64 " -> %" PRId64 " into buffer for process %i at index %lu\n", rank, origin_vertex, target_vertex, target_process, idx[target_process] / sizeof(packed_edge) );
#endif
      memcpy( edge_distribution+send_displacement[target_process]+idx[target_process], buf+i, sizeof(packed_edge) );
      idx[target_process] += sizeof(packed_edge);
    }
  }

  free(buf);
  free(idx);

  /**
    step 2b: finish up the distribution of the send count
   */
  MPI_Wait( &req, MPI_STATUS_IGNORE );

#ifdef GDEBUG
  for( int i=0 ; i<commsize ; i++ ) {
    printf( "%i: recv_count[%i] = %li\n", rank, i, recv_count[i] / sizeof(packed_edge) );
  }
#endif

  /**
    step 4: distribute the edges
   */
  int* recv_displacement = malloc( commsize * sizeof(int) );
  if( recv_displacement == NULL ) {
    fprintf( stderr, "%i - %s line %i: Not able to allocate memory\n", rank, (char*) __FILE__, __LINE__ );
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  recv_displacement[0] = 0;
  for( int i=1 ; i<commsize ; i++ ) {
    recv_displacement[i] = recv_displacement[i-1] + recv_count[i-1];
  }

  size_t total_recv_count = recv_displacement[commsize-1] + recv_count[commsize-1]; /* in Bytes */

  buf = (packed_edge*) malloc( total_recv_count );

  MPI_Alltoallv( edge_distribution, send_count, send_displacement, MPI_CHAR, buf, recv_count, recv_displacement, MPI_CHAR, MPI_COMM_WORLD );

  int received_total_edge_count = total_recv_count/sizeof(packed_edge); /* in number of edges */
#ifdef GDEBUG
  for( int i=0 ; i<received_total_edge_count ; i++ ) {
    int64_t origin = get_v0_from_edge( buf+i );
    int64_t target = get_v1_from_edge( buf+i );
    printf( "%i: %" PRId64 " -> %" PRId64 "\n", rank, origin, target );
  }
#endif

  /**
    clean up
   */
  free( edge_distribution );
  free( send_count );
  free( recv_count );
  free( send_displacement );
  free( recv_displacement );

  /**
    step 5: database creation
   */
  GDI_Database db;
  GDI_Transaction transaction;
  int status;

  GDA_Init_params parameters;
  parameters.block_size = block_size;
  parameters.memory_size = memory_size;
  parameters.comm = MPI_COMM_WORLD;

  status = GDI_CreateDatabase( &parameters, sizeof(GDA_Init_params), &db );
  assert( status == GDI_SUCCESS );

  data_scheme_1_init( nglobalverts );

  GDI_Label* vlabels = malloc( VERTEX_LABEL_COUNT * sizeof(GDI_Label) );

  for( uint8_t i=0 ; i<VERTEX_LABEL_COUNT ; i++ ) {
    vlabels[i] = GDI_LABEL_NULL;
    status = GDI_CreateLabel( vertex_label_names[i], db, &vlabels[i] );
    assert( status == GDI_SUCCESS );
  }

  GDI_Label* elabels = malloc( EDGE_LABEL_COUNT * sizeof(GDI_Label) );

  for( uint8_t i=0 ; i<EDGE_LABEL_COUNT ; i++ ) {
    elabels[i] = GDI_LABEL_NULL;
    status = GDI_CreateLabel( edge_label_names[i], db, &elabels[i] );
    assert( status == GDI_SUCCESS );
  }

  GDI_PropertyType* ptypes = malloc( PROPERTY_TYPE_COUNT * sizeof(GDI_PropertyType) );

  for( uint8_t i=0 ; i<PROPERTY_TYPE_COUNT ; i++ ) {
    ptypes[i] = GDI_PROPERTY_TYPE_NULL;
  }

  /**
    ptypes[ 0]: name
    ptypes[ 1]: type
    ptypes[ 2]: revenue
    ptypes[ 3]: firstName
    ptypes[ 4]: lastName
    ptypes[ 5]: email
    ptypes[ 6]: birthday
    ptypes[ 7]: longitude
    ptypes[ 8]: latitude
    ptypes[ 9]: budget
    ptypes[10]: density
    ptypes[11]: meltingPoint
    ptypes[12]: formula
   */

  status = GDI_CreatePropertyType( "name", GDI_SINGLE_ENTITY, GDI_CHAR, GDI_MAX_SIZE, 100, db, &ptypes[0] );
  assert( status == GDI_SUCCESS );

  status = GDI_CreatePropertyType( "type", GDI_SINGLE_ENTITY, GDI_CHAR, GDI_MAX_SIZE, 10, db, &ptypes[1] );
  assert( status == GDI_SUCCESS );

  status = GDI_CreatePropertyType( "revenue", GDI_SINGLE_ENTITY, GDI_UINT64_T, GDI_FIXED_SIZE, 1, db, &ptypes[2] );
  assert( status == GDI_SUCCESS );

  status = GDI_CreatePropertyType( "firstName", GDI_SINGLE_ENTITY, GDI_CHAR, GDI_MAX_SIZE, 100, db, &ptypes[3] );
  assert( status == GDI_SUCCESS );

  status = GDI_CreatePropertyType( "lastName", GDI_SINGLE_ENTITY, GDI_CHAR, GDI_MAX_SIZE, 100, db, &ptypes[4] );
  assert( status == GDI_SUCCESS );

  status = GDI_CreatePropertyType( "email", GDI_SINGLE_ENTITY, GDI_CHAR, GDI_MAX_SIZE, 1000, db, &ptypes[5] );
  assert( status == GDI_SUCCESS );

  status = GDI_CreatePropertyType( "birthday", GDI_SINGLE_ENTITY, GDI_DATE, GDI_FIXED_SIZE, 1, db, &ptypes[6] );
  assert( status == GDI_SUCCESS );

  status = GDI_CreatePropertyType( "longitude", GDI_SINGLE_ENTITY, GDI_UINT32_T, GDI_FIXED_SIZE, 1, db, &ptypes[7] );
  assert( status == GDI_SUCCESS );

  status = GDI_CreatePropertyType( "latitude", GDI_SINGLE_ENTITY, GDI_UINT32_T, GDI_FIXED_SIZE, 1, db, &ptypes[8] );
  assert( status == GDI_SUCCESS );

  status = GDI_CreatePropertyType( "budget", GDI_SINGLE_ENTITY, GDI_UINT32_T, GDI_FIXED_SIZE, 1, db, &ptypes[9] );
  assert( status == GDI_SUCCESS );

  status = GDI_CreatePropertyType( "density", GDI_SINGLE_ENTITY, GDI_UINT32_T, GDI_FIXED_SIZE, 1, db, &ptypes[10] );
  assert( status == GDI_SUCCESS );

  status = GDI_CreatePropertyType( "meltingPoint", GDI_SINGLE_ENTITY, GDI_UINT32_T, GDI_FIXED_SIZE, 1, db, &ptypes[11] );
  assert( status == GDI_SUCCESS );

  status = GDI_CreatePropertyType( "formula", GDI_SINGLE_ENTITY, GDI_CHAR, GDI_MAX_SIZE, 100, db, &ptypes[12] );
  assert( status == GDI_SUCCESS );

  /**
    add vertices to database
   */
  status = GDI_StartTransaction( db, &transaction );
  assert( status == GDI_SUCCESS );

  GDI_VertexHolder vertex;

  /* use seed from Graph500 code */
  uint_fast32_t seed[5];
  uint64_t seed1 = 2, seed2 = 3;
  make_mrg_seed(seed1, seed2, seed);

  for( uint64_t vertex_id=rank ; vertex_id<nglobalverts ; vertex_id += commsize ) {
    srand( *seed + vertex_id );

    status = GDI_CreateVertex( &vertex_id, 8, transaction, &vertex );
    if( status != GDI_SUCCESS ) {
      fprintf( stderr, "%i: GDI_CreateVertex returned with error code %i while creating vertex %" PRIu64 ".\n", rank, status, vertex_id );
      MPI_Abort( MPI_COMM_WORLD, 1 );
    }

    /**
      add label to vertex
     */
    uint8_t idx;
    for( idx=0 ; idx<VERTEX_LABEL_COUNT ; idx++ ) {
      if( vertex_id < vlabel_range[idx] ) {
        break;
      }
    }
    status = GDI_AddLabelToVertex( vlabels[idx], vertex );
    assert( status == GDI_SUCCESS );

#ifdef GDEBUG
    printf( "%i: created vertex with id %" PRIu64 ", vertex uid %" PRIu64 " and label %s\n", rank, vertex_id, *(uint64_t*) vertex->blocks->data, vlabels[idx]->name );
#endif
    if( idx == 0 ) {
      /* company */

      int numBytes = rand() % 100;
      char* string = create_string_property( numBytes );
      status = GDI_AddPropertyToVertex( string, numBytes, ptypes[0] /* name */, vertex );
      assert( status == GDI_SUCCESS );
#ifdef GDEBUG
      printf("%" PRIu64" name = %s\n", vertex_id, string);
#endif
      free( string );

      /* TODO: could be improved by having a few distinct types */
      numBytes = rand() % 10;
      string = create_string_property( numBytes );
      status = GDI_AddPropertyToVertex( string, numBytes, ptypes[1] /* type */, vertex );
      assert( status == GDI_SUCCESS );
#ifdef GDEBUG
      printf("%" PRIu64" type = %s\n", vertex_id, string);
#endif
      free( string );

      uint64_t num = create_uint64_property( 1000000000 /* at most 10.000.000,00 of some currency */ );
      status = GDI_AddPropertyToVertex( &num, 1, ptypes[2] /* revenue */, vertex );
      assert( status == GDI_SUCCESS );
#ifdef GDEBUG
      printf("%" PRIu64" revenue = %" PRIu64 "\n", vertex_id, num);
#endif
    } else {
      if( idx == 1 ) {
        /* person */

        int numBytes = rand() % 100;
        char* string = create_string_property( numBytes );
        status = GDI_AddPropertyToVertex( string, numBytes, ptypes[3] /* firstName */, vertex );
        assert( status == GDI_SUCCESS );
#ifdef GDEBUG
        printf("%" PRIu64" firstName = %s\n", vertex_id, string);
#endif
        free( string );

        numBytes = rand() % 100;
        string = create_string_property( numBytes );
        status = GDI_AddPropertyToVertex( string, numBytes, ptypes[4] /* lastName */, vertex );
        assert( status == GDI_SUCCESS );
#ifdef GDEBUG
        printf("%" PRIu64" lastName = %s\n", vertex_id, string);
#endif
        free( string );

        numBytes = rand() % 1000;
        string = create_string_property( numBytes );
        status = GDI_AddPropertyToVertex( string, numBytes, ptypes[5] /* email */, vertex );
        assert( status == GDI_SUCCESS );
#ifdef GDEBUG
        printf("%" PRIu64" email = %s\n", vertex_id, string);
#endif
        free( string );

        GDI_Date birthdate = create_birthdate_property();
        status = GDI_AddPropertyToVertex( &birthdate, 1, ptypes[6] /* birthday */, vertex );
        assert( status == GDI_SUCCESS );
#ifdef GDEBUG
        uint16_t year;
        uint8_t month, day;
        status = GDI_GetDate( &year, &month, &day, &birthdate );
        assert( status == GDI_SUCCESS );
        printf("%" PRIu64" birthdate = %u-%02u-%02u\n", vertex_id, year, month, day);
#endif
      } else {
        if( idx == 2 ) {
          /* place */

          int numBytes = rand() % 100;
          char* string = create_string_property( numBytes );
          status = GDI_AddPropertyToVertex( string, numBytes, ptypes[0] /* name */, vertex );
          assert( status == GDI_SUCCESS );
#ifdef GDEBUG
          printf("%" PRIu64" name = %s\n", vertex_id, string);
#endif
          free( string );

          uint32_t num = create_uint32_property( 12960000 /* max: 360 degree * 60 minutes * 60 seconds * 10 fraction */ );
          status = GDI_AddPropertyToVertex( &num, 1, ptypes[7] /* longitude */, vertex );
          assert( status == GDI_SUCCESS );
#ifdef GDEBUG
          printf("%" PRIu64" longitude = %u\n", vertex_id, num);
#endif

          num = create_uint32_property( 6480000 /* 180 degree * 60 minutes * 60 seconds * 10 fraction */ );
          status = GDI_AddPropertyToVertex( &num, 1, ptypes[8] /* latitude */, vertex );
          assert( status == GDI_SUCCESS );
#ifdef GDEBUG
          printf("%" PRIu64" latitude = %u\n", vertex_id, num);
#endif
        } else {
          if( idx == 3 ) {
            /* project */

            int numBytes = rand() % 100;
            char* string = create_string_property( numBytes );
            status = GDI_AddPropertyToVertex( string, numBytes, ptypes[0] /* name */, vertex );
            assert( status == GDI_SUCCESS );
#ifdef GDEBUG
            printf("%" PRIu64" name = %s\n", vertex_id, string);
#endif
            free( string );

            uint32_t num = create_uint32_property( 0xFFFFFFFF );
            status = GDI_AddPropertyToVertex( &num, 1, ptypes[9] /* budget */, vertex );
            assert( status == GDI_SUCCESS );
#ifdef GDEBUG
            printf("%" PRIu64" budget = %u\n", vertex_id, num);
#endif
          } else {
            /* ressource */
            assert( idx == 4 );

            int numBytes = rand() % 100;
            char* string = create_string_property( numBytes );
            status = GDI_AddPropertyToVertex( string, numBytes, ptypes[0] /* name */, vertex );
            assert( status == GDI_SUCCESS );
#ifdef GDEBUG
            printf("%" PRIu64" name = %s\n", vertex_id, string);
#endif
            free( string );

            numBytes = rand() % 100;
            string = create_string_property( numBytes );
            status = GDI_AddPropertyToVertex( string, numBytes, ptypes[12] /* formula */, vertex );
            assert( status == GDI_SUCCESS );
#ifdef GDEBUG
            printf("%" PRIu64" formula = %s\n", vertex_id, string);
#endif
            free( string );

            uint32_t num = create_uint32_property( 1000000 /* in g/L */ );
            status = GDI_AddPropertyToVertex( &num, 1, ptypes[10] /* density */, vertex );
            assert( status == GDI_SUCCESS );
#ifdef GDEBUG
            printf("%" PRIu64" density = %u\n", vertex_id, num);
#endif

            num = create_uint32_property( 100000 /* in 0.01 K */ );
            status = GDI_AddPropertyToVertex( &num, 1, ptypes[11] /* meltingPoint */, vertex );
            assert( status == GDI_SUCCESS );
#ifdef GDEBUG
            printf("%" PRIu64" meltingPoint = %u\n", vertex_id, num);
#endif
          }
        }
      }
    }
  }

  status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_COMMIT );
  assert( status == GDI_SUCCESS );

  /**
    add edges to database
   */
  status = GDI_StartTransaction( db, &transaction );
  assert( status == GDI_SUCCESS );

  for( int i=0 ; i<received_total_edge_count ; i++ ) {
    int64_t origin = get_v0_from_edge( buf+i );
    int64_t target = get_v1_from_edge( buf+i );

    uint64_t origin_process, origin_block;
    uint64_t target_process, target_block;

    origin_process = origin % commsize;
    origin_block = origin / commsize * parameters.block_size;

    target_process = target % commsize;
    target_block = target / commsize * parameters.block_size;

    GDI_Vertex_uid origin_vertex_uid, target_vertex_uid;

    GDA_SetDPointer( origin_block, origin_process, &origin_vertex_uid );
    GDA_SetDPointer( target_block, target_process, &target_vertex_uid );

    uint8_t elabel_int_handle = data_scheme_1_assign_elabel( origin, target ) + elabels[0]->int_handle;

    int origin_rank = origin_process;

    if( origin_rank == rank ) {
#ifdef GDEBUG
      printf( "%i: edge origin %" PRId64 " translates into process %i and block offset %" PRIu64 " - generated vertex uid %" PRIu64 "\n", rank, origin, origin_rank, origin_block, origin_vertex_uid );
      printf( "%i: assign label %s to edge %" PRId64 "-%" PRId64 "\n", rank, elabels[elabel_int_handle-elabels[0]->int_handle]->name, origin, target );
#endif
      status = GDI_AssociateVertex( origin_vertex_uid, transaction, &vertex );
      assert( status == GDI_SUCCESS );

      uint32_t edge_offset;
      int edgetype;

      if( directed ) {
        edgetype = GDI_EDGE_OUTGOING;
      } else {
        edgetype = GDI_EDGE_UNDIRECTED;
      }

      GDA_LightweightEdgesAddEdge( edgetype, target_vertex_uid, vertex, &edge_offset );
      GDA_LightweightEdgesSetLabel( elabel_int_handle, edge_offset, vertex );

      vertex->write_flag = true;
    }

    int target_rank = target_process;
    if( target_rank == rank ) {
#ifdef GDEBUG
      printf( "%i: edge target %" PRId64 " translates into process %i and block offset %" PRIu64 " - generated vertex uid %" PRIu64 "\n", rank, target, target_rank, target_block, target_vertex_uid );
      printf( "%i: assign label %s to edge %" PRId64 "-%" PRId64 "\n", rank, elabels[elabel_int_handle-elabels[0]->int_handle]->name, origin, target );
#endif
      status = GDI_AssociateVertex( target_vertex_uid, transaction, &vertex );
      assert( status == GDI_SUCCESS );

      uint32_t edge_offset;
      int edgetype;

      if( directed ) {
        edgetype = GDI_EDGE_INCOMING;
      } else {
        edgetype = GDI_EDGE_UNDIRECTED;
      }

      GDA_LightweightEdgesAddEdge( edgetype, origin_vertex_uid, vertex, &edge_offset );
      GDA_LightweightEdgesSetLabel( elabel_int_handle, edge_offset, vertex );

      vertex->write_flag = true;
    }
  }

  transaction->write_flag = true;

  status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_COMMIT );
  assert( status == GDI_SUCCESS );

  free( buf );

  /* set output parameters */
  *graph_db = db;
  *vertexlabels = vlabels;
  *edgelabels = elabels;
  *propertytypes = ptypes;

  MPI_Barrier( MPI_COMM_WORLD );
}
