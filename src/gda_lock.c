// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Wojciech Chlapek

#include <assert.h>

#include "gda_lock.h"
#include "rma.h"

/**
  constant definitions
 */
#define LOCK_READER_INCREMENT_VALUE   1
#define LOCK_WRITER_INCREMENT_VALUE   0x80000000LL
#define LOCK_SINGLE_READER            LOCK_READER_INCREMENT_VALUE
#define LOCK_SINGLE_WRITER            LOCK_WRITER_INCREMENT_VALUE
#define LOCK_WRITER_MASK              LOCK_WRITER_INCREMENT_VALUE

/**
  a lock has the following layout:

                   31
   -------------------------------
  |  incarnation  |w|    reader   |
   -------------------------------
  63             32               0

  incarnation field has width of 32 Bit
  writer bit
  reader field has width of 31 Bit
 */

/**
  Helper function which finds a primary block and returns the rank and the displacement in the general window.
 */
static inline void FindPrimaryBlock( GDI_VertexHolder vertex, uint64_t* target_rank, uint64_t* target_displacement ) {
  uint64_t primary_block_dpointer = *(uint64_t*) vertex->blocks->data;
  GDA_GetDPointer( target_displacement, target_rank, primary_block_dpointer );

  *target_displacement = *target_displacement / vertex->transaction->db->block_size + 1 /* list head at system_window[0] */;
}


/**
  Acquires a read lock on a vertex associated with the given VertexHolder.
 */
void GDA_AcquireVertexReadLock( GDI_VertexHolder vertex ) {
  /**
    An attempt to acquire a read lock when some kind of lock is already acquired should never happen, as the calling
    function is obliged to check before making a call.
   */
  assert( vertex->lock_type == GDA_NO_LOCK );

  /**
    Find DPointer to the primary block
   */
  uint64_t target_rank;
  uint64_t offset;
  FindPrimaryBlock( vertex, &target_rank, &offset );

  int64_t value = LOCK_READER_INCREMENT_VALUE;
  int64_t result;

  /**
    Try to acquire a read lock
   */
  RMA_Fetch_and_op( &value, &result, MPI_INT64_T, target_rank, offset, RMA_SUM, vertex->transaction->db->win_system );
  RMA_Win_flush( target_rank, vertex->transaction->db->win_system );

  if( result & LOCK_WRITER_MASK ) {
    /**
      There is an active writer - we can't read, so revert changes
     */
    value = -LOCK_READER_INCREMENT_VALUE;
    RMA_Fetch_and_op( &value, &result, MPI_INT64_T, target_rank, offset, RMA_SUM,
                      vertex->transaction->db->win_system );
    RMA_Win_flush( target_rank, vertex->transaction->db->win_system );
    return;
  }

  /**
    Success - read lock acquired
   */
  vertex->lock_type = GDA_READ_LOCK;
  vertex->incarnation = result >> 32;
}


/**
  Converts a read lock acquired on the vertex associated with the given VertexHolder into a write lock.
 */
void GDA_UpdateToVertexWriteLock( GDI_VertexHolder vertex ) {
  /**
    At the beginning there must be a read lock acquired on the vertex and the calling function is obliged to check this
    condition before making a call. If either there is no lock acquired or write lock is acquired, it is considered as
    an erroneous usage.
   */
  assert( vertex->lock_type == GDA_READ_LOCK );

  /**
    Find DPointer to the primary block
   */
  uint64_t target_rank;
  uint64_t offset;
  FindPrimaryBlock( vertex, &target_rank, &offset );

  int64_t compare_value = vertex->incarnation;
  compare_value = (compare_value << 32);
  int64_t replace_value = compare_value;
  compare_value = compare_value | LOCK_SINGLE_READER;
  replace_value = replace_value | LOCK_SINGLE_WRITER;
  int64_t result;

  /**
    Try to convert a read lock into a write lock. It will only succeed if I am the only reader and there is no writer.

    No need to revert, in case of a failure, as we used a compare and swap operation.
   */
  RMA_Compare_and_swap( &replace_value, &compare_value, &result, MPI_INT64_T, target_rank, offset,
                        vertex->transaction->db->win_system );
  RMA_Win_flush( target_rank, vertex->transaction->db->win_system );

  if( (result & 0x00000000FFFFFFFF /* ignore incarnation */) == LOCK_SINGLE_READER ) {
    /**
      Success - write lock acquired
     */
    vertex->lock_type = GDA_WRITE_LOCK;
  }
}


/**
  Sets the write lock bit for the vertex associated with the given VertexHolder.

  should only be called from GDI_CreateVertex, after the primary block is
  acquired from the free memory management, so we already know, that we are
  the only process with access to that block
 */
void GDA_SetVertexWriteLock( GDI_VertexHolder vertex ) {
  /**
    Find DPointer to the primary block
   */
  uint64_t target_rank;
  uint64_t offset;
  FindPrimaryBlock( vertex, &target_rank, &offset );

  int64_t value = LOCK_WRITER_INCREMENT_VALUE;
  int64_t result; /* result not used */

  /**
    Try to acquire a write lock
   */
  RMA_Fetch_and_op( &value, &result, MPI_INT64_T, target_rank, offset, RMA_SUM, vertex->transaction->db->win_system );
  RMA_Win_flush( target_rank, vertex->transaction->db->win_system );

  /**
    Success - write lock acquired
   */
  vertex->lock_type = GDA_WRITE_LOCK;
  vertex->incarnation = result >> 32;
}


/**
  Releases the lock which is currently acquired on the vertex associated with the given VertexHolder.
 */
void GDA_ReleaseVertexLock( GDI_VertexHolder vertex ) {
  /**
    It is erroneous to call this function if there is no lock acquired on the vertex. The calling function is obliged to
    check this condition before making a call.
   */
  assert( vertex->lock_type != GDA_NO_LOCK );

  /**
    Find DPointer to the primary block
   */
  uint64_t target_rank;
  uint64_t offset;
  FindPrimaryBlock( vertex, &target_rank, &offset );

  int64_t value;
  if( vertex->lock_type == GDA_READ_LOCK ) {
    value = -LOCK_READER_INCREMENT_VALUE;
  } else {
    if( vertex->delete_flag ) {
      value = LOCK_WRITER_INCREMENT_VALUE; /* want to unlock the lock and increase the incarnation field in one step */
    } else {
      value = -(int64_t)LOCK_WRITER_INCREMENT_VALUE;
    }
  }
  int64_t result = 0; /* result not used */
  /**
    Atomically decrement the number of readers or number of writers
   */
  RMA_Fetch_and_op( &value, &result, MPI_INT64_T, target_rank, offset, RMA_SUM, vertex->transaction->db->win_system );
  RMA_Win_flush( target_rank, vertex->transaction->db->win_system );

  vertex->lock_type = GDA_NO_LOCK;
  /**
    no need to update the incarnation field, since the vertex is
    deleted anyway and the GDI_VertexHolder will be freed in the
    calling code as well
   */
}
