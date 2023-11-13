// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#ifndef __GDA_BLOCK_H
#define __GDA_BLOCK_H

#include "gda_dpointer.h"
#include "rma.h" /* equal to #include <mpi.h> */

/**
  header file for the NOD block management

  NOD divides the graph database memory into fixed sized blocks. This
  header provides functions to set up the block structure and do the
  free memory management: acquire free blocks and release blocks.

  NOD uses three MPI windows to manage the memory:
  - block window, which contains the actual blocks that contain the
    graph database data
  - usage window, which acts as a linked list. The elements in the
    linked list have a 1-on-1 relationship to the blocks in the main
    window. An element either indicates that the respective block is
    in use or points to the next free element in the list.
  - system window, which currently only has one element: the pointer
    to the first free element in the linked list of the usage window
 */


/**
  constant definitions
 */

/**
  indicates whether a block is in use in the usage window

  the constant is used with uint32_t elements
 */
#define GDA_BLOCK_INUSE 0xFFFFFFFE

/**
  indicates the end of the unused block list

  the constant is used with uint32_t elements
 */
#define GDA_BLOCK_NULL  0xFFFFFFFF


/**
  function prototypes
 */

/**
  Inits the block memory management

  collective call

  significant input parameter:
  - memsize
  - comm
  - commsize

  significant output parameter:
  - win_{blocks|system|usage}
  - win_{blocks|system|usage}_baseptr
  - win_{blocks|system|usage}_size
 */
void GDA_InitBlock( GDI_Database graph_db );

/**
  Frees the block memory management

  collective call

  significant input/output parameter:
  - win_{blocks|system|usage}
 */
void GDA_FreeBlock( GDI_Database graph_db );

/**
  Acquires a new unused block

  assumes that target rank is valid for the graph database

  the algorithm will first try to acquire an unused block on
  target_rank, and if that fails, it will try other processes

  if no unused block are available, GDA_DPOINTER_NULL is
  returned
 */

GDA_DPointer GDA_AllocateBlock( int target_rank, GDI_Database graph_db );

/**
  Releases a block

  assumes that block is valid and its offset a multiple
  of the block size of the database

  algorithm will retry until it succeeds
 */
void GDA_DeallocateBlock( GDA_DPointer block, GDI_Database graph_db );

/**
  Fetches a block from the database

  non-blocking operation

  assumes that the size of buf is at least the database block size
 */
void GDA_GetBlock( void* buf, GDA_DPointer dpointer, GDI_Database graph_db );

/**
  Push a block into the database

  non-blocking operation

  assumes that the size of buf is at least the database block size
 */
void GDA_PutBlock( const void* buf, GDA_DPointer dpointer, GDI_Database graph_db );

#endif // #ifndef __GDA_BLOCK_H
