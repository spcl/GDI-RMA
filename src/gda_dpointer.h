// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#ifndef __GDA_DPOINTER_H
#define __GDA_DPOINTER_H

#include <inttypes.h>
#include <stdbool.h>

/**
  This header provides functions for so called DPointers. A DPointer is
  a data structure that indicates a remote memory location. The remote
  memory location is identified by a (target) rank and an offset.

  A DPointer consists of two fields: rank and offset, where each field
  occupies a certain amount of bits.
  - The rank can be found in the higher order bits of a dpointer.
  - The offset can be found in the lower order bits of a dpointer.

   --------------------------------
  |      rank      |     offset    |
   --------------------------------
*/


/**
  constant definitions
 */

// TODO: move this constant definition somewhere else, since it might actually
// be a user-defined constant?
/**
  This constant controls the number of bits used for the offset field of the
  DPointer. In doing so, it controls also the maximum amount of memory per rank,
  that can be used for the graph database.

  rank field uses 64 - GDA_DPOINTER_OFFSETBITS bits, so more memory per rank also
  means a smaller number of ranks in total and vice versa.
*/
#define GDA_DPOINTER_OFFSETBITS 32

#define GDA_DPOINTER_NULL 0xFFFFFFFFFFFFFFFF


/**
  data type definitions
 */

typedef uint64_t GDA_DPointer;


/**
  function prototypes
 */

/**
  Sets the DPointer to a new value.
*/
void GDA_SetDPointer(uint64_t offset, uint64_t rank, GDA_DPointer* dpointer);

/**
  Retrieves the rank and the offset of a DPointer.
  Doesn't check whether the DPointer is NULL.
*/
void GDA_GetDPointer(uint64_t* offset, uint64_t* rank, GDA_DPointer dpointer);

/**
  Checks whether a DPointer is invalid.
*/
bool GDA_DPointerIsNull(GDA_DPointer dpointer);

/**
  Prints the content of a DPointer. Intended for debugging.
  Will also work for a DPointer that is NULL.
*/
void GDA_PrintDPointer(GDA_DPointer dpointer);

#endif // #ifndef __GDA_DPOINTER_H
