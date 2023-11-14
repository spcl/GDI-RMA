// Copyright (c) 2021 ETH-Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#include <assert.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>

#include "make_graph.h"
#include "utils.h"

#include "mpi.h"

#include "data_scheme_1.h"

#define WRITE_BUF_SIZE 10000

uint8_t getNumDigits( uint64_t number ) {
  uint8_t numDigits = 0;

  if( number == 0 ) {
    return 1;
  }

  while( number > 0 ) {
    number = number / 10;
    numDigits++;
  }

  return numDigits;
}


void lpg_graph500_csv( uint64_t nglobalverts, MPI_Offset edge_count, packed_edge* edges, const char* output_prefix ) {
  int rank, commsize;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &commsize);

  if( WRITE_BUF_SIZE <= 72 ) {
    if( rank == 0 ) {
      fprintf( stderr, "WRITE_BUF_SIZE should be at least 73.\n" );
    }
    MPI_Abort( MPI_COMM_WORLD, 1 );
  }

  data_scheme_1_init( nglobalverts );

  char* wbuf;

  size_t prefix_len = strlen( output_prefix );
  char* filename = NULL;
  size_t filename_len = 0;

  /**
    determine the number of Bytes to write into the edge files
   */
  wbuf = malloc( EDGE_LABEL_COUNT * WRITE_BUF_SIZE * sizeof(char) );

  uint64_t* numBytesPerEdgeLabel = (uint64_t*) calloc( EDGE_LABEL_COUNT, sizeof(uint64_t) );
  for( int64_t i=0 ; i<edge_count ; i++ ) {
    int64_t origin = get_v0_from_edge( edges+i );
    int64_t target = get_v1_from_edge( edges+i );

    uint8_t elabel = data_scheme_1_assign_elabel( origin, target );

    numBytesPerEdgeLabel[elabel] += getNumDigits( origin ) + getNumDigits( target ) + 2 /* white space characters */;
  }

  if( rank == 0 ) {
    /* take header into account */
    for( uint8_t i=0 ; i<EDGE_LABEL_COUNT ; i++ ) {
      uint8_t origin_label_idx = 0;
      uint8_t target_label_idx = 0;
      for( uint8_t j=0 ; j<VERTEX_LABEL_COUNT*VERTEX_LABEL_COUNT; j++ ) {
        if( edge_matrix[origin_label_idx][target_label_idx] == i ) {
          break;
        }
        target_label_idx++;
        if( target_label_idx == VERTEX_LABEL_COUNT ) {
          target_label_idx = 0;
          origin_label_idx++;
        }
      }
      assert( origin_label_idx < VERTEX_LABEL_COUNT );
      assert( target_label_idx < VERTEX_LABEL_COUNT );
      numBytesPerEdgeLabel[i] += sprintf( wbuf + i * WRITE_BUF_SIZE, ":START_ID(%s),:END_ID(%s)\n", vertex_label_names[origin_label_idx], vertex_label_names[target_label_idx] ); /* header */
    }
  }

  uint64_t* byteCountPerRankAndEdgeLabel = (uint64_t*) malloc( commsize * EDGE_LABEL_COUNT * sizeof(uint64_t) );

  MPI_Allgather( numBytesPerEdgeLabel, EDGE_LABEL_COUNT /* send count */, MPI_UINT64_T, byteCountPerRankAndEdgeLabel, EDGE_LABEL_COUNT /* receive count from every rank */, MPI_UINT64_T, MPI_COMM_WORLD );

  free( numBytesPerEdgeLabel );

  MPI_Offset* writeOffsetPerEdgeLabel = (MPI_Offset*) calloc( EDGE_LABEL_COUNT, sizeof(MPI_Offset) );
  for( uint8_t i=0 ; i<EDGE_LABEL_COUNT ; i++ ) {
    for( int j=0 ; j<rank ; j++ ) {
      writeOffsetPerEdgeLabel[i] += byteCountPerRankAndEdgeLabel[i+j*EDGE_LABEL_COUNT];
    }
  }

  free( byteCountPerRankAndEdgeLabel );

  size_t* position = (size_t*) calloc( EDGE_LABEL_COUNT, sizeof(size_t) );

  MPI_File* edgeFiles = (MPI_File*) malloc( EDGE_LABEL_COUNT * sizeof(MPI_File) );

  for( uint8_t i=0 ; i<EDGE_LABEL_COUNT ; i++ ) {
    /**
      figure out the filename and adjust if necessary the string buffers
     */
    size_t label_len = strlen( edge_label_names[i] );
    if( filename_len < (prefix_len + label_len + 20) ) {
      filename = realloc( filename, prefix_len + label_len + 20 );
      filename_len = prefix_len + label_len + 20;
    }

    snprintf( filename, filename_len, "%sedges_%s.csv", output_prefix, edge_label_names[i] );

    MPI_File_open( MPI_COMM_WORLD, filename, MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &edgeFiles[i] );

    if( rank == 0 ) {
      uint8_t origin_label_idx = 0;
      uint8_t target_label_idx = 0;
      for( uint8_t j=0 ; j<VERTEX_LABEL_COUNT*VERTEX_LABEL_COUNT; j++ ) {
        if( edge_matrix[origin_label_idx][target_label_idx] == i ) {
          break;
        }
        target_label_idx++;
        if( target_label_idx == VERTEX_LABEL_COUNT ) {
          target_label_idx = 0;
          origin_label_idx++;
        }
      }
      assert( origin_label_idx < VERTEX_LABEL_COUNT );
      assert( target_label_idx < VERTEX_LABEL_COUNT );
      position[i] = sprintf( wbuf + i * WRITE_BUF_SIZE, ":START_ID(%s),:END_ID(%s)\n", vertex_label_names[origin_label_idx], vertex_label_names[target_label_idx] ); /* header */
    }
  }

  MPI_Status writeStatus;

  for( int64_t i=0 ; i<edge_count ; i++ ) {
    uint64_t origin = get_v0_from_edge( edges+i ); /* conversion from int64_t to uint64_t */
    uint64_t target = get_v1_from_edge( edges+i ); /* conversion from int64_t to uint64_t */

    uint8_t elabel = data_scheme_1_assign_elabel( origin, target );

    uint8_t origin_label;
    for( origin_label=0 ; origin_label<VERTEX_LABEL_COUNT-1 ; origin_label++ ) {
      if( origin < vlabel_range[origin_label] ) {
        break;
      }
    }

    uint8_t target_label;
    for( target_label=0 ; target_label<VERTEX_LABEL_COUNT-1 ; target_label++ ) {
      if( target < vlabel_range[target_label] ) {
        break;
      }
    }

    assert( origin_label < VERTEX_LABEL_COUNT );
    assert( target_label < VERTEX_LABEL_COUNT );

    if( origin_label < target_label ) {
      position[elabel] += sprintf( wbuf + elabel * WRITE_BUF_SIZE + position[elabel], "%" PRId64 ",%" PRId64 "\n", origin, target );
    } else {
      position[elabel] += sprintf( wbuf + elabel * WRITE_BUF_SIZE + position[elabel], "%" PRId64 ",%" PRId64 "\n", target, origin );
    }

    if( position[elabel] + 42 /* 64 Bit integer has at most 20 decimal digits + two charactes for white space */ > WRITE_BUF_SIZE ) {
      MPI_File_write_at( edgeFiles[elabel], writeOffsetPerEdgeLabel[elabel], &wbuf[elabel * WRITE_BUF_SIZE], position[elabel] /* can also be used as size */, MPI_CHAR, &writeStatus );
      writeOffsetPerEdgeLabel[elabel] += position[elabel];
      position[elabel] = 0;
    }
  }

  for( uint8_t i=0 ; i<EDGE_LABEL_COUNT ; i++ ) {
    if( position[i] != 0 ) {
      MPI_File_write_at( edgeFiles[i], writeOffsetPerEdgeLabel[i], &wbuf[i * WRITE_BUF_SIZE], position[i] /* size */, MPI_CHAR, &writeStatus );
    }
    MPI_File_close( &edgeFiles[i] );
  }

  /**
    clean up
   */
  free( edges );
  free( position );
  free( wbuf );
  free( edgeFiles );
  free( writeOffsetPerEdgeLabel );

  /**
    generate the vertex CSV files
   */
  wbuf = malloc( VERTEX_LABEL_COUNT * WRITE_BUF_SIZE * sizeof(char) );

  /**
    determine the number of Bytes to write into the vertex files
   */
  uint64_t nlocalverts = nglobalverts / commsize;
  uint64_t startvertex = rank * nlocalverts;
  uint64_t endvertex = (rank + 1) == commsize  ? nglobalverts : startvertex + nlocalverts;

  /* determine initial vertex label */
  uint8_t idx;
  for( idx=0 ; idx<VERTEX_LABEL_COUNT ; idx++ ) {
    if( startvertex < vlabel_range[idx] ) {
      break;
    }
  }
  assert( idx < VERTEX_LABEL_COUNT );

  /* use seed from Graph500 code */
  uint_fast32_t seed[5];
  uint64_t seed1 = 2, seed2 = 3;
  make_mrg_seed(seed1, seed2, seed);

  uint64_t* numBytesPerVertexLabel = (uint64_t*) calloc( VERTEX_LABEL_COUNT, sizeof(uint64_t) );
  for( uint64_t i=startvertex ; i<endvertex ; i++ ) {
    /* determine vertex label */
    while( i>= vlabel_range[idx] ) {
      idx++;
    }
    assert( idx < VERTEX_LABEL_COUNT );

    numBytesPerVertexLabel[idx] += getNumDigits( i ) + 1 /* newline */;

    srand( *seed + i );

    /**
      properties
     */
    if( idx == 0 ) {
      /**
        company
       */

      /* name */
      int numBytes = rand() % 100;
      char* string = create_string_property( numBytes ); /* make sure we follow the same random number sequence, albeit it is not necessary in this step */
      free( string );
      numBytesPerVertexLabel[0] += numBytes + 1 /* comma */;

      /* type */
      numBytes = rand() % 10;
      string = create_string_property( numBytes );
      free( string );
      numBytesPerVertexLabel[0] += numBytes + 1 /* comma */;

      /* revenue */
      uint64_t num = create_uint64_property( 1000000000 /* at most 10.000.000,00 of some currency */ );
      numBytesPerVertexLabel[0] += getNumDigits( num ) + 1 /* comma */;
    } else {
      if( idx == 1 ) {
        /**
          person
         */
        /* first name */
        int numBytes = rand() % 100;
        char* string = create_string_property( numBytes ); /* make sure we follow the same random number sequence, albeit it is not necessary in this step */
        free( string );
        numBytesPerVertexLabel[1] += numBytes + 1 /* comma */;

        /* last name */
        numBytes = rand() % 100;
        string = create_string_property( numBytes ); /* make sure we follow the same random number sequence, albeit it is not necessary in this step */
        free( string );
        numBytesPerVertexLabel[1] += numBytes + 1 /* comma */;

        /* email */
        numBytes = rand() % 1000;
        string = create_string_property( numBytes ); /* make sure we follow the same random number sequence, albeit it is not necessary in this step */
        free( string );
        numBytesPerVertexLabel[1] += numBytes + 1 /* comma */;

        /* birth date */
        numBytesPerVertexLabel[1] += 10 /* YYYY-MM-DD */ + 1 /* comma */;
      } else {
        if( idx == 2 ) {
          /**
            place
           */

          /* name */
          int numBytes = rand() % 100;
          char* string = create_string_property( numBytes ); /* make sure we follow the same random number sequence, albeit it is not necessary in this step */
          free( string );
          numBytesPerVertexLabel[2] += numBytes + 1 /* comma */;

          /* longitude */
          uint32_t num = create_uint32_property( 12960000 /* max: 360 degree * 60 minutes * 60 seconds * 10 fraction */ );
          numBytesPerVertexLabel[2] += getNumDigits( num ) + 1 /* comma */;

          /* latitude */
          num = create_uint32_property( 6480000 /* 180 degree * 60 minutes * 60 seconds * 10 fraction */ );
          numBytesPerVertexLabel[2] += getNumDigits( num ) + 1 /* comma */;
        } else {
          if( idx == 3 ) {
            /**
              project
             */

            /* name */
            int numBytes = rand() % 100;
            char* string = create_string_property( numBytes ); /* make sure we follow the same random number sequence, albeit it is not necessary in this step */
            free( string );
            numBytesPerVertexLabel[3] += numBytes + 1 /* comma */;

            /* budget */
            uint32_t num = create_uint32_property( 0xFFFFFFFF );
            numBytesPerVertexLabel[3] += getNumDigits( num ) + 1 /* comma */;
          } else {
            /**
              ressource
             */
            assert( idx == 4 );

            /* name */
            int numBytes = rand() % 100;
            char* string = create_string_property( numBytes ); /* make sure we follow the same random number sequence, albeit it is not necessary in this step */
            free( string );
            numBytesPerVertexLabel[4] += numBytes + 1 /* comma */;

            /* formula */
            numBytes = rand() % 100;
            string = create_string_property( numBytes ); /* make sure we follow the same random number sequence, albeit it is not necessary in this step */
            free( string );
            numBytesPerVertexLabel[4] += numBytes + 1 /* comma */;

            /* density */
            uint32_t num = create_uint32_property( 1000000 /* in g/L */ );
            numBytesPerVertexLabel[4] += getNumDigits( num ) + 1 /* comma */;

            /* melting point */
            num = create_uint32_property( 100000 /* in 0.01 K */ );
            numBytesPerVertexLabel[4] += getNumDigits( num ) + 1 /* comma */;
          }
        }
      }
    }
  }

  if( rank == 0 ) {
    /* take header into account */
    numBytesPerVertexLabel[0] += sprintf( wbuf, "%sId:ID(%s),name,type,revenue\n", vertex_label_names[0], vertex_label_names[0] );
    numBytesPerVertexLabel[1] += sprintf( wbuf, "%sId:ID(%s),firstName,lastName,email,birthday\n", vertex_label_names[1], vertex_label_names[1] );
    numBytesPerVertexLabel[2] += sprintf( wbuf, "%sId:ID(%s),name,longitude,latitude\n", vertex_label_names[2], vertex_label_names[2] );
    numBytesPerVertexLabel[3] += sprintf( wbuf, "%sId:ID(%s),name,budget\n", vertex_label_names[3], vertex_label_names[3] );
    numBytesPerVertexLabel[4] += sprintf( wbuf, "%sId:ID(%s),name,formula,density,meltingPoint\n", vertex_label_names[4], vertex_label_names[4] );
  }

  uint64_t* byteCountPerRankAndVertexLabel = (uint64_t*) malloc( commsize * VERTEX_LABEL_COUNT * sizeof(uint64_t) );

  MPI_Allgather( numBytesPerVertexLabel, VERTEX_LABEL_COUNT /* send count */, MPI_UINT64_T, byteCountPerRankAndVertexLabel, VERTEX_LABEL_COUNT /* receive count from every rank */, MPI_UINT64_T, MPI_COMM_WORLD );

  free( numBytesPerVertexLabel );

  MPI_Offset* writeOffsetPerVertexLabel = (MPI_Offset*) calloc( VERTEX_LABEL_COUNT, sizeof(MPI_Offset) );
  for( uint8_t i=0 ; i<VERTEX_LABEL_COUNT ; i++ ) {
    for( int j=0 ; j<rank ; j++ ) {
      writeOffsetPerVertexLabel[i] += byteCountPerRankAndVertexLabel[i+j*VERTEX_LABEL_COUNT];
    }
  }

  free( byteCountPerRankAndVertexLabel );

  position = (size_t*) calloc( VERTEX_LABEL_COUNT, sizeof(size_t) );
  MPI_File* vertexFiles = (MPI_File*) malloc( VERTEX_LABEL_COUNT * sizeof(MPI_File) );

  for( uint8_t i=0 ; i<VERTEX_LABEL_COUNT ; i++ ) {
    /**
      figure out the filename and adjust if necessary the string buffers
     */
    size_t label_len = strlen( vertex_label_names[i] );
    if( filename_len < (prefix_len + label_len + 20) ) {
      filename = realloc( filename, prefix_len + label_len + 20 );
      filename_len = prefix_len + label_len + 20;
    }

    snprintf( filename, filename_len, "%snodes_%s.csv", output_prefix, vertex_label_names[i] );

    MPI_File_open( MPI_COMM_WORLD, filename, MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &vertexFiles[i] );
  }

  if( rank == 0 ) {
    /* write header into each file */
    position[0] = sprintf( wbuf, "%sId:ID(%s),name,type,revenue\n", vertex_label_names[0], vertex_label_names[0] );
    position[1] = sprintf( wbuf + WRITE_BUF_SIZE, "%sId:ID(%s),firstName,lastName,email,birthday\n", vertex_label_names[1], vertex_label_names[1] );
    position[2] = sprintf( wbuf + 2 * WRITE_BUF_SIZE, "%sId:ID(%s),name,longitude,latitude\n", vertex_label_names[2], vertex_label_names[2] );
    position[3] = sprintf( wbuf + 3 * WRITE_BUF_SIZE, "%sId:ID(%s),name,budget\n", vertex_label_names[3], vertex_label_names[3] );
    position[4] = sprintf( wbuf + 4 * WRITE_BUF_SIZE, "%sId:ID(%s),name,formula,density,meltingPoint\n", vertex_label_names[4], vertex_label_names[4] );

    for( uint8_t i=0 ; i<VERTEX_LABEL_COUNT ; i++ ) {
      *(wbuf + i * WRITE_BUF_SIZE) = tolower( *(wbuf + i * WRITE_BUF_SIZE) );
    }
  }

  for( idx=0 ; idx<VERTEX_LABEL_COUNT ; idx++ ) {
    if( startvertex < vlabel_range[idx] ) {
      break;
    }
  }
  assert( idx < VERTEX_LABEL_COUNT );

  for( uint64_t i=startvertex ; i<endvertex ; i++ ) {
    /* determine vertex label */
    while( i>= vlabel_range[idx] ) {
      idx++;
    }
    assert( idx < VERTEX_LABEL_COUNT );

    srand( *seed + i );

    if( idx == 0 ) {
      /* company */

      int numBytes = rand() % 100;
      char* name = create_string_property( numBytes );

      numBytes = rand() % 10;
      char* type = create_string_property( numBytes );

      uint64_t revenue = create_uint64_property( 1000000000 /* at most 10.000.000,00 of some currency */ );

      position[0] += sprintf( wbuf + position[0], "%" PRIu64 ",%s,%s,%" PRIu64 "\n", i, name, type, revenue );

      free( name );
      free( type );
    } else {
      if( idx == 1 ) {
        /* person */

        int numBytes = rand() % 100;
        char* firstName = create_string_property( numBytes );

        numBytes = rand() % 100;
        char* lastName = create_string_property( numBytes );

        numBytes = rand() % 1000;
        char* email = create_string_property( numBytes );

        /**
          birth date:
          changes to this code should also be reflected in the GDI version
         */
        uint16_t year = get_random_uint16_t( 1900, 2000 );
        uint8_t month = get_random_uint8_t( 1, 12 );
        uint8_t day = get_random_uint8_t( 1, 28 );

        position[1] += sprintf( wbuf + WRITE_BUF_SIZE + position[1], "%" PRIu64 ",%s,%s,%s,%u-%02u-%02u\n", i, firstName, lastName, email, year, month, day );

        free( firstName );
        free( lastName );
        free( email );
      } else {
        if( idx == 2 ) {
          /* place */

          int numBytes = rand() % 100;
          char* name = create_string_property( numBytes );

          uint32_t longitude = create_uint32_property( 12960000 /* max: 360 degree * 60 minutes * 60 seconds * 10 fraction */ );

          uint32_t latitude = create_uint32_property( 6480000 /* 180 degree * 60 minutes * 60 seconds * 10 fraction */ );

          position[2] += sprintf( wbuf + 2 * WRITE_BUF_SIZE + position[2], "%" PRIu64 ",%s,%u,%u\n", i, name, longitude, latitude );

          free( name );
        } else {
          if( idx == 3 ) {
            /* project */

            int numBytes = rand() % 100;
            char* name = create_string_property( numBytes );

            uint32_t budget = create_uint32_property( 0xFFFFFFFF );

            position[3] += sprintf( wbuf + 3 * WRITE_BUF_SIZE + position[3], "%" PRIu64 ",%s,%u\n", i, name, budget );

            free( name );
          } else {
            /* ressource */
            assert( idx == 4 );

            int numBytes = rand() % 100;
            char* name = create_string_property( numBytes );

            numBytes = rand() % 100;
            char* formula = create_string_property( numBytes );

            uint32_t density = create_uint32_property( 1000000 /* in g/L */ );

            uint32_t melting_point = create_uint32_property( 100000 /* in 0.01 K */ );

            position[4] += sprintf( wbuf + 4 * WRITE_BUF_SIZE + position[4], "%" PRIu64 ",%s,%s,%u,%u\n", i, name, formula, density, melting_point );

            free( name );
            free( formula );
          }
        }
      }
    }

    if( position[idx] + 1235 /* person: char[100] + char[100] + char[1000] + birthday (10) + ID (at most 20 digits) + 5 white space characters */ > WRITE_BUF_SIZE ) {
       MPI_File_write_at( vertexFiles[idx], writeOffsetPerVertexLabel[idx], &wbuf[idx * WRITE_BUF_SIZE], position[idx] /* can also be used as size */, MPI_CHAR, &writeStatus );
       writeOffsetPerVertexLabel[idx] += position[idx];
       position[idx] = 0;
    }
  }

  for( uint8_t i=0 ; i<VERTEX_LABEL_COUNT ; i++ ) {
    if( position[i] != 0 ) {
      MPI_File_write_at( vertexFiles[i], writeOffsetPerVertexLabel[i], &wbuf[i * WRITE_BUF_SIZE], position[i] /*size */, MPI_CHAR, &writeStatus );
    }
    MPI_File_close( &vertexFiles[i] );
  }

  /**
    clean up
   */
  free( wbuf );
  free( filename );
  free( position );
  free( vertexFiles );
  free( writeOffsetPerVertexLabel );

  data_scheme_1_finalize();
}
