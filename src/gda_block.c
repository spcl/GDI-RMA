// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

/**
  update 09.07.2020
  implemented pointer tagging to avoid the ABA problem (idea by Lasse
  Meinen)

  This only concerns the list head in the system window, which consists of
  64 Bits. We will use the upper 32 Bits for pointer tagging.

   ---------------
  |  tag  | index |
   ---------------
  63      32      0

  We can do this, because we can't address more than 2GB in foMPI anyway.
  This will come basically at no additional cost, since it only involves
  additional local updates of the list head value.
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "gdi.h"
#include "gda_block.h"

/**
  collective call

  will initialize the block management on all processes that are part of
  the communicator comm
*/
void GDA_InitBlock( GDI_Database graph_db ) {

  MPI_Info info;
  MPI_Info_create( &info );
  MPI_Info_set( info, "same_size", "true" );

  MPI_Aint num_blocks = graph_db->memsize / graph_db->block_size;
  if( num_blocks >= GDA_BLOCK_INUSE ) {
    /**
      we have two special values at the end of the range of
      possible values of an uint32_t, so need to make sure,
      that the indexes in our list structure don't exceed
      that range

      we could reduce this by one for the NDEBUG case
     */
    fprintf( stderr, "%i: GDA_InitBlock - will return immediately without action performed.\nNumber of blocks (%lu) is too big to handle.\n", graph_db->commrank, num_blocks );
    MPI_Abort( MPI_COMM_WORLD, -1 );
  }

  /**
    create the (main) block window, which contains the vertex and edge blocks
   */

#ifndef NDEBUG
  graph_db->win_blocks_baseptr = NULL;
  graph_db->win_usage_baseptr = NULL;
  graph_db->win_system_baseptr = NULL;
#endif
  /* round down, so that the actual main window size is a multiple of the block size of the database */
  graph_db->win_blocks_size = num_blocks * graph_db->block_size;
  RMA_Win_allocate( graph_db->win_blocks_size, 1 /* displacement unit */, info, graph_db->comm, &(graph_db->win_blocks_baseptr), &(graph_db->win_blocks) );
  assert( graph_db->win_blocks_baseptr != NULL );

  /**
    create the usage window, which keeps track of the blocks in the block window, that are in use
    and also manages a list of the free blocks
   */
  graph_db->win_usage_size = num_blocks * sizeof(uint32_t);
  RMA_Win_allocate( graph_db->win_usage_size, sizeof(uint32_t) /* displacement unit */, info, graph_db->comm, &(graph_db->win_usage_baseptr), &(graph_db->win_usage) );
  assert( graph_db->win_usage_baseptr != NULL );

  /* init the linked list of free blocks */
  for( MPI_Aint i = 0 ; i < num_blocks-1 ; i++ ) {
    graph_db->win_usage_baseptr[i] = i+1;
  }
  graph_db->win_usage_baseptr[num_blocks-1] = GDA_BLOCK_NULL /* indicates end of list */;

  /**
    create the system window
   */
  graph_db->win_system_size = (1 + num_blocks) * sizeof(uint64_t);
  RMA_Win_allocate( graph_db->win_system_size, sizeof(uint64_t) /* displacement unit */, info, graph_db->comm, &(graph_db->win_system_baseptr), &(graph_db->win_system) );
  assert( graph_db->win_system_baseptr != NULL );

  /* init head pointer of the list of free blocks as well as the locks */
  memset( graph_db->win_system_baseptr, 0, graph_db->win_system_size );

  /**
    enable access to all windows on all ranks
   */
  RMA_Win_lock_all( 0, graph_db->win_blocks );
  RMA_Win_lock_all( 0, graph_db->win_usage );
  RMA_Win_lock_all( 0, graph_db->win_system );

  MPI_Info_free( &info );
}

/**
  collective call

  will free the block management on all processes that are part of
  the communicator comm
*/
void GDA_FreeBlock( GDI_Database graph_db ) {
 
  RMA_Win_unlock_all( graph_db->win_blocks );
  RMA_Win_unlock_all( graph_db->win_usage );
  RMA_Win_unlock_all( graph_db->win_system );

  RMA_Win_free( &(graph_db->win_blocks) );
  RMA_Win_free( &(graph_db->win_usage) );
  RMA_Win_free( &(graph_db->win_system) );

  /**
    Don't set the the size(s) to zero and baseptr to NULL,
    since this function is only called from GDI_FreeDatabase,
    so the objects is destroyed afterwards.
   */
}

/**
  will acquire an unused block

  assume that target rank is valid for the graph database

  the algorithm will first try to acquire an unused block on
  target_rank, and if that fails, it will try other processes

  if the algorithm has tried all processes, it will return a
  NULL pointer
 */
GDA_DPointer GDA_AllocateBlock( int target_rank, GDI_Database graph_db ) {

  /**
    first we need the pointer to the current first
    element of the linked list in the usage window

    we need to access the system window for that
   */

  uint64_t list_head;
  uint64_t block_index;
  uint32_t next_index;
  uint64_t origin, result;
  int current_rank = target_rank;

  RMA_Get( &list_head, 1 /* origin count */, MPI_UINT64_T, current_rank, 0 /* target displacement */, 1 /* target count */, MPI_UINT64_T, graph_db->win_system );
  RMA_Win_flush( current_rank, graph_db->win_system );

  while( true ) {
    block_index = list_head & 0x00000000FFFFFFFF; /* mask out the tag */
    if( block_index != GDA_BLOCK_NULL ) {
      /* list is not empty */

      /**
        get index of the next element, that points to
        an unused block in the block window
       */
      RMA_Get( &next_index, 1 /* origin count */, MPI_UINT32_T, current_rank, block_index /* target displacement */, 1 /* target count */, MPI_UINT32_T, graph_db->win_usage );
      RMA_Win_flush( current_rank, graph_db->win_usage );

      /**
        atomic compare and swap operation to the head of the list
        to acquire the empty block
       */
      origin = next_index; /* promote to uint64_t */
      origin = origin | ((list_head & 0xFFFFFFFF00000000) + 4294967296) /* = 2^32 */; /* add the incremented tag to the new value */
      result = GDA_BLOCK_NULL;

      RMA_Compare_and_swap( &origin, &list_head /* compare */, &result, MPI_UINT64_T, current_rank, 0 /* target displacement */, graph_db->win_system );
      RMA_Win_flush( current_rank, graph_db->win_system );

      if( result == list_head ) {
        /* operation was successful */
        GDA_DPointer dp;
        GDA_SetDPointer( block_index * graph_db->block_size, current_rank, &dp );

#ifndef NDEBUG
        /**
          this should never happen, if everyone complies to
          algorithm and uses locking on a higher layer,
          but using these additional communication operations
          makes sure that the list data structure will always
          stay intact

          works together with the atomic compare and swap
          operation at the beginning of GDA_DeallocateBlock

          - set a special value to sequentialize concurrent
            allocate and deallocate operations on the same block
          - additionally that special value will also act as a
            tiebreaker for concurrent deallocate operations on
            the same block
         */
        uint32_t value = GDA_BLOCK_INUSE;
        RMA_Put( &value, 1, MPI_UINT32_T, current_rank, block_index /* target displacement */, 1 /* target count */, MPI_UINT32_T, graph_db->win_usage );
        RMA_Win_flush( current_rank, graph_db->win_usage );
#endif
        return dp;
      } else {
        /**
          otherwise we retry

          use the just retrieved head of the list,
          saves one communication operation
         */
        list_head = result;
      }
    } else {
      /**
        current target rank has no empty blocks,
        so we will try the next one
       */
      current_rank = (current_rank+1) % graph_db->commsize;
      if( current_rank == target_rank ) {
        /* tried every rank, and found no unused blocks */
        return GDA_DPOINTER_NULL;
      }
      RMA_Get( &list_head, 1 /* origin count */, MPI_UINT64_T, current_rank, 0 /* target displacement */, 1 /* target count */, MPI_UINT64_T, graph_db->win_system );
      RMA_Win_flush( current_rank, graph_db->win_system );
    }
  }
}

/**
  will release a block

  assumes that block is valid and its offset a multiple
  of the block size of the database

  algorithm will retry until it succeeds
 */
void GDA_DeallocateBlock( GDA_DPointer block, GDI_Database graph_db ) {

  /**
    first we need the pointer to the current first
    element of the linked list in the usage window

    we need to access the system window for that
   */

  uint32_t next_index;
  uint64_t list_head;
  uint64_t block_offset, block_rank;
  uint64_t result;

  GDA_GetDPointer( &block_offset, &block_rank, block );

  block_offset = block_offset / graph_db->block_size;

#ifndef NDEBUG
  /**
    this should never happen, if everyone complies to
    algorithm and uses locking on a higher layer,
    but using these additional communication operations
    makes sure that the list data structure will always
    stay intact

    works together with the put operation before the
    return of GDA_AllocateBlock

    atomic compare and swap with the special value
    will act as a tiebreaker for concurrent deallocate
    operations on the same block
   */

  uint32_t value = 0; /* doesn't matter as long as it is not GDA_BLOCK_INUSE */
  uint32_t compare = GDA_BLOCK_INUSE;
  uint32_t u32_result;

  /* not hardware-accelerated in foMPI */
  RMA_Compare_and_swap( &value /* origin */, &compare, &u32_result, MPI_UINT32_T, block_rank, block_offset, graph_db->win_usage );
  RMA_Win_flush( block_rank, graph_db->win_usage );
  if( u32_result != GDA_BLOCK_INUSE ) {
    fprintf( stderr, "%i: GDA_DeallocateBlock - will return immediately without action performed.\nConcurrent delete detected for block %" PRIu64 " on rank %" PRIu64 ".\n", graph_db->commrank, block_offset, block_rank );
    return;
  }
#endif

  RMA_Get( &list_head, 1 /* origin count */, MPI_UINT64_T, block_rank, 0 /* target displacement */, 1 /* target count */, MPI_UINT64_T, graph_db->win_system );
  RMA_Win_flush( block_rank, graph_db->win_system );

  while( true ) {

    /**
      update the next pointer, so we can introduce our
      block as the first element in the linked list
     */
    next_index = list_head; // demote to uint32_t
    RMA_Put( &next_index, 1 /* origin count */, MPI_UINT32_T, block_rank, block_offset, 1 /* target count */, MPI_UINT32_T, graph_db->win_usage );
    RMA_Win_flush( block_rank, graph_db->win_usage );

    /**
      atomic compare and swap operation to the head of the list
      to insert our element
     */
    uint64_t origin = block_offset | ((list_head & 0xFFFFFFFF00000000) + 4294967296) /* = 2^32 */; /* add the incremented tag to the new value */
    RMA_Compare_and_swap( &origin, &list_head /* compare */, &result, MPI_UINT64_T, block_rank, 0 /* target displacement */, graph_db->win_system );
    RMA_Win_flush( block_rank, graph_db->win_system );

    if( result == list_head ) {
      /* operation was successful */
      return;
    } else {
      /**
        otherwise we retry

        use the just retrieved head of the list, saves one communication operation
       */
      list_head = result;
    }
  }
}


/**
  non-blocking operation

  assumes that the size of buf is at least the database block size
 */
void GDA_GetBlock( void* buf, GDA_DPointer dpointer, GDI_Database graph_db ) {
  uint64_t offset, target_rank;

  assert( buf != NULL );
  assert( !GDA_DPointerIsNull( dpointer ) );

  GDA_GetDPointer( &offset, &target_rank, dpointer );

  assert( graph_db != GDI_DATABASE_NULL );
  assert( offset % graph_db->block_size == 0 );
  assert( offset < (uint64_t)(graph_db->win_blocks_size) );
  assert( target_rank < graph_db->commsize );

  RMA_Get( buf, graph_db->block_size, MPI_BYTE, target_rank, offset, graph_db->block_size, MPI_BYTE, graph_db->win_blocks );
}


/**
  non-blocking operation

  assumes that the size of buf is at least the database block size
 */
void GDA_PutBlock( const void* buf, GDA_DPointer dpointer, GDI_Database graph_db ) {
  uint64_t offset, target_rank;

  assert( buf != NULL );
  assert( !GDA_DPointerIsNull( dpointer ) );

  GDA_GetDPointer( &offset, &target_rank, dpointer );

  assert( graph_db != GDI_DATABASE_NULL );
  assert( offset % graph_db->block_size == 0 );
  assert( offset < (uint64_t)(graph_db->win_blocks_size) );
  assert( target_rank < graph_db->commsize );

  RMA_Put( buf, graph_db->block_size, MPI_BYTE, target_rank, offset, graph_db->block_size, MPI_BYTE, graph_db->win_blocks );
}
