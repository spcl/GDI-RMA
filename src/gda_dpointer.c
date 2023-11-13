// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

/**
  Inspired by the original DPointer implementation of Emanuel Peter,
  which was written for the bachelor thesis:
 
  Towards a High-Performance Distributed In-Memory RMA Graph Database
  by Emanuel Peter
*/

#include <stdio.h>

#include "gda_dpointer.h"
#include "rma.h" /* equal to #include <mpi.h> */

/**
  A DPointer consists of two fields: rank and offset, where each
  field occupies a certain amount of bits.
  * The rank can be found in the higher order bits of a dpointer.
  * The offset can be found in the lower order bits of a dpointer.

   --------------------------------
  |      rank      |     offset    |
   --------------------------------

  The number of bits used for the offset is controlled by the
  constant GDA_DPOINTER_OFFSETBITS. The number of bits used for the
  rank is 64 - GDA_DPOINTER_OFFSETBITS.
*/

/**
  both input parameter are 64 bit integer to allow for a different
  amount of offset bits
*/
// TODO: Make this function static inline?
void GDA_SetDPointer(uint64_t offset, uint64_t rank, GDA_DPointer* dpointer) {
#ifndef NDEBUG
  uint64_t rank_upper_bound = (uint64_t)1 << (64 - GDA_DPOINTER_OFFSETBITS);
  uint64_t offset_upper_bound = (uint64_t)1 << GDA_DPOINTER_OFFSETBITS;

  if( rank >= rank_upper_bound ) {
    fprintf( stderr, "GDA_SetDPointer: rank parameter (%" PRIu64 ") is outside its bounds.\n", rank);
    MPI_Abort( MPI_COMM_WORLD, -1 );
  }

  if( offset >= offset_upper_bound ) {
    fprintf( stderr, "GDA_SetDPointer: offset parameter (%" PRIu64 ") is outside its bounds.\n", offset);
    MPI_Abort( MPI_COMM_WORLD, -1 );
  }
#endif /* #ifndef NDEBUG */

  *dpointer = (rank << GDA_DPOINTER_OFFSETBITS) | offset;
}

// TODO: Make this function static inline?
void GDA_GetDPointer(uint64_t* offset, uint64_t* rank, GDA_DPointer dpointer) {
  /**
    remove the block offset field first
  */
  *rank = dpointer >> GDA_DPOINTER_OFFSETBITS;

  /**
    remove the rank field first
  */
  *offset = (dpointer << (64 - GDA_DPOINTER_OFFSETBITS)) >> (64 - GDA_DPOINTER_OFFSETBITS);
}

bool GDA_DPointerIsNull(GDA_DPointer dpointer) {
  return dpointer == GDA_DPOINTER_NULL;
}

void GDA_PrintDPointer(GDA_DPointer dpointer) {
  uint64_t rank, offset;

  if( GDA_DPointerIsNull( dpointer ) ) {
    /**
      DPointer is NULL
    */
    printf("DPointer: NULL\n");
  } else {
    /**
      DPointer is valid.
     */
    GDA_GetDPointer( &offset, &rank, dpointer );
    
    printf("DPointer: rank = %" PRIu64 ", offset = %" PRIu64 "\n", rank, offset);
  }
}
