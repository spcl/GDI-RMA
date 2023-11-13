// Copyright (c) 2014, 2018, 2021 ETH Zurich.
//                                All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Maciej Besta, Robert Gerstenberger
//
// The following source code is an improved version of the distributed
// hashtable code by Maciej Besta, which was released alongside foMPI.
//
// The improvements (including a delete call) were made by Robert
// Gerstenberger, while working on a different project.

#ifndef __GDA_DISTRIBUTED_HASHTABLE_H
#define __GDA_DISTRIBUTED_HASHTABLE_H

#include <stdlib.h>

#include "gda_dpointer.h"
#include "rma.h"


/**
  constant definitions
 */

#define GDA_HASHINT_NULL 0xFFFFFFFFFFFFFFFF


/**
  data type definitions
 */
typedef struct GDA_RMAHashMap_Element_desc {
  /**
    should be initially NULL
   */
  uint64_t key;
  uint64_t value;
  /**
    uin32_t would be enough, but to keep the alignment intact we use
    a 64-Bit integer
   */
  uint64_t incarnation;
  /**
    pointer to the heap
   */
  GDA_DPointer next;
} GDA_RMAHashMap_Element;

/**
  quick explanation:
  Table has DPointers to elements belonging to a certain hash-entry.
  These elements build a linked list.

  // TODO: sort this
 */
typedef struct GDA_RMAHashMap_desc {
  /**
    communicator over which hash map should span
   */
  MPI_Comm comm;
  size_t comm_size;
  /**
    leave this as int since it is also used as input for MPI calls
   */
  int comm_rank;

  /**
    MPI windows:
   */
  /**
    table (holds DPointers to elements)
   */
  RMA_Win win_table;
  /**
    basepointer
   */
  GDA_DPointer *table;
  /**
    heap (holds elements)
   */
  RMA_Win win_heap;
  /**
    basepointer
   */
  GDA_RMAHashMap_Element *heap;
  RMA_Win win_heap_counter;
  uint64_t* heap_counter;

  /**
    table_size -> where hash into (the more the less collisions) -> DPointer each
    heap_size -> how much can hold in heap -> element each
    numbers may be rounded up to get symmetric space requirements
   */
  size_t table_size_total;
  size_t table_size_local;
  size_t heap_size_local;

#if 0
  /**
    things for random key extractions:
   */
  bool random_extraction_ready;
  int64_t* heap_sizes;
  size_t heap_sizes_sum;
#endif
} GDA_RMAHashMap_desc_t;

typedef GDA_RMAHashMap_desc_t* GDA_RMAHashMap;


/**
  function prototypes
 */

void GDA_CreateRMAHashMap( size_t table_size, size_t heap_size, MPI_Comm comm, GDA_RMAHashMap* hashmap );
void GDA_FreeRMAHashMap( GDA_RMAHashMap* hashmap );
void GDA_InsertElementIntoRMAHashMap( uint64_t hashed_key, uint64_t key, uint64_t value, uint64_t incarnation, GDA_RMAHashMap hashmap );
void GDA_FindElementInRMAHashMap( uint64_t hashed_key, uint64_t key, uint64_t* value, uint64_t* incarnation, bool* found_flag, GDA_RMAHashMap hashmap );
bool GDA_RemoveElementFromRMAHashMap( uint64_t hashed_key, uint64_t key, GDA_RMAHashMap hashmap );

#endif // #ifndef __GDA_DISTRIBUTED_HASHTABLE_H
