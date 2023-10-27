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

#include <stdio.h>
#include <string.h>

#include "gda_distributed_hashtable.h"


uint64_t hashfunc( uint64_t key, GDA_RMAHashMap hashmap ) {
  return key % hashmap->table_size_total;
}


/**
  each rank must submit same numbers
 */
void GDA_CreateRMAHashMap( size_t table_size, size_t heap_size, MPI_Comm comm, GDA_RMAHashMap* hashmap ) {
  GDA_RMAHashMap internal_hashmap = malloc( sizeof(GDA_RMAHashMap_desc_t) );

  internal_hashmap->comm = comm;
  int temp;
  MPI_Comm_size( comm, &temp );
  /**
    extra step to avoid compiler warnings comparing unsigned types with a signed type
   */
  internal_hashmap->comm_size = temp;
  MPI_Comm_rank( comm, &(internal_hashmap->comm_rank) );

  internal_hashmap->table_size_local = (table_size + internal_hashmap->comm_size - 1)/internal_hashmap->comm_size;
  internal_hashmap->heap_size_local = (heap_size + internal_hashmap->comm_size - 1)/internal_hashmap->comm_size;

  internal_hashmap->table_size_total = internal_hashmap->table_size_local * internal_hashmap->comm_size;

  RMA_Win_allocate( internal_hashmap->table_size_local * sizeof(uint64_t), sizeof(uint64_t), MPI_INFO_NULL, comm, &(internal_hashmap->table), &(internal_hashmap->win_table) );
  RMA_Win_allocate( internal_hashmap->heap_size_local * 4 * sizeof(uint64_t), sizeof(uint64_t), MPI_INFO_NULL, comm, &(internal_hashmap->heap), &(internal_hashmap->win_heap) );
  RMA_Win_allocate( 2 * sizeof(uint64_t), sizeof(uint64_t), MPI_INFO_NULL, comm, &(internal_hashmap->heap_counter), &(internal_hashmap->win_heap_counter) );

  /**
    number of already used elements of the heap
   */
  internal_hashmap->heap_counter[0] = 0;
  /**
    begin of a linked list of elements that can be reused
   */
  internal_hashmap->heap_counter[1] = GDA_DPOINTER_NULL;

  RMA_Win_lock_all( 0 /* assert */, internal_hashmap->win_table );
  RMA_Win_lock_all( 0 /* assert */, internal_hashmap->win_heap );
  RMA_Win_lock_all( 0 /* assert */, internal_hashmap->win_heap_counter );

  for( size_t i=0 ; i<internal_hashmap->table_size_local ; i++ ) {
    internal_hashmap->table[i] = GDA_DPOINTER_NULL;
  }

  *hashmap = internal_hashmap;
}


void GDA_FreeRMAHashMap( GDA_RMAHashMap* hashmap ) {
  RMA_Win_unlock_all( (*hashmap)->win_table );
  RMA_Win_unlock_all( (*hashmap)->win_heap );
  RMA_Win_unlock_all( (*hashmap)->win_heap_counter );

  RMA_Win_free( &((*hashmap)->win_table) );
  RMA_Win_free( &((*hashmap)->win_heap) );
  RMA_Win_free( &((*hashmap)->win_heap_counter) );

  free( *hashmap );
}

void GDA_DeallocateElementOfRMAHashMap( GDA_DPointer elem, GDA_RMAHashMap hashmap ) {
  uint64_t dp_rank;
  uint64_t dp_offset;

  GDA_GetDPointer( &dp_offset, &dp_rank, elem );

  while (true) {
    uint64_t temp;
    /**
      get pointer to next free element
     */
    // TODO: change to MPI_UINT64_T
    // TODO: should be a fetch_and_op call with MPI_NO_OP
    RMA_Get( &temp, 1, MPI_INT64_T, dp_rank, 1 /* offset */, 1 /* size */, MPI_INT64_T, hashmap->win_heap_counter );
    RMA_Win_flush_local( dp_rank, hashmap->win_heap_counter );

    /**
      update the element, use value for the linked list instead of the
      next pointer

      in case we have a slow find which needs to see that the element
      is invalidated (next pointer pointing to the element itself)
     */
    // TODO: just an assignment if locally
    // TODO: change to MPI_UINT64_T
    // TODO: should be an accumulate call with MPI_OP_REPLACE
    RMA_Put( &temp, 1, MPI_INT64_T, dp_rank, dp_offset*4+1 /* offset */, 1 /* size */, MPI_INT64_T, hashmap->win_heap );
    RMA_Win_flush_local( dp_rank, hashmap->win_heap );

    uint64_t swap_result;
    RMA_Compare_and_swap( &dp_offset, &temp, &swap_result, MPI_UINT64_T, dp_rank, 1 /* offset */, hashmap->win_heap_counter );
    RMA_Win_flush_local( dp_rank, hashmap->win_heap_counter );
    if( swap_result == temp ) {
      return;
    }
  }
}


/**
  may cause double inserts if insert same key again
 */
void GDA_InsertElementIntoRMAHashMap( uint64_t hashed_key, uint64_t key, uint64_t value, uint64_t incarnation, GDA_RMAHashMap hashmap ) {
  uint64_t hash = hashfunc( hashed_key, hashmap );

  uint64_t t_rank = hash / hashmap->table_size_local;
  /**
    offset counted in elements
   */
  // TODO: change this?
  uint64_t t_offset = hash % hashmap->table_size_local;

  /**
    allocate on heap
   */
  GDA_DPointer dp;
  uint64_t h_offset;

  /**
    get a local element
   */
  {
#if 0
    // right now we don't support the allocation of heap space on remote processes
    /**
      add 1
     */
    uint64_t origin = 1;
    RMA_Fetch_and_op( &origin, &h_offset, MPI_INT64_T, hashmap->comm_rank, /* disp */ 0, RMA_SUM, hashmap->win_heap_counter );
    RMA_Win_flush_local( hashmap->comm_rank, hashmap->win_heap_counter);

    /**
      check if result is not out of bounds:
     */
    if( h_offset >= hashmap->heap_size_local ) {
#endif
    if( hashmap->heap_counter[0] < hashmap->heap_size_local ) {
      h_offset = (hashmap->heap_counter[0])++;
    } else {
      /**
        check the linked list for already used elements
       */
      uint64_t swap_result;
      do {
        /**
          for the first part there is no need for RMA operations
         */
        h_offset = hashmap->heap_counter[1];
        if( h_offset == GDA_DPOINTER_NULL ) {
          fprintf( stderr, "RMA_Hashmap: Not enough space on local heap of rank %i. -> will abort\n", hashmap->comm_rank );
          MPI_Abort( MPI_COMM_WORLD, -1 );
        }
        uint64_t temp = hashmap->heap[h_offset].value;

        RMA_Compare_and_swap( &temp, &h_offset, &swap_result, MPI_UINT64_T, hashmap->comm_rank, 1 /* offset */, hashmap->win_heap_counter );
        RMA_Win_flush_local( hashmap->comm_rank, hashmap->win_heap_counter );
      } while( swap_result != h_offset );
    }
    GDA_SetDPointer( h_offset, hashmap->comm_rank, &dp );
  }

  hashmap->heap[h_offset].key = key;
  hashmap->heap[h_offset].value = value;
  hashmap->heap[h_offset].incarnation = incarnation;

  /**
    fill in (try to swap in in table)
   */
  while (true) {
    /**
      read current in table:
     */
    // TODO: change to MPI_UINT64_T
    // TODO: should be a fetch_and_op call with MPI_NO_OP
    RMA_Get( &(hashmap->heap[h_offset].next), 1, MPI_INT64_T, t_rank, t_offset, 1, MPI_INT64_T, hashmap->win_table );
    RMA_Win_flush_local( t_rank, hashmap->win_table );

    /**
      swap element in
     */
    uint64_t swap_result;
    RMA_Compare_and_swap( &dp, &(hashmap->heap[h_offset].next), &swap_result, MPI_UINT64_T, t_rank, t_offset, hashmap->win_table );
    RMA_Win_flush_local( t_rank, hashmap->win_table );

    if( swap_result == hashmap->heap[h_offset].next ) {
      return;
    }
  }
}


void GDA_FindElementInRMAHashMap( uint64_t hashed_key, uint64_t key, uint64_t* value, uint64_t* incarnation, bool* found_flag, GDA_RMAHashMap hashmap ) {
  uint64_t hash = hashfunc( hashed_key, hashmap );

  uint64_t t_rank = hash / hashmap->table_size_local;
  /**
    counted in elements
   */
  // TODO: change this?
  uint64_t t_offset = hash % hashmap->table_size_local;

  /**
    check table
   */
  GDA_DPointer dp;
  // TODO: change to MPI_UINT64_T
  // TODO: should be a fetch_and_op call with MPI_NO_OP
  RMA_Get( &dp, 1, MPI_INT64_T, t_rank, t_offset, 1, MPI_INT64_T, hashmap->win_table );
  RMA_Win_flush_local( t_rank, hashmap->win_table );

  if( dp == GDA_DPOINTER_NULL ) {
    /**
      empty table slot
     */
    *found_flag = false;
    *value = GDA_DPOINTER_NULL;
    return;
  }

  /**
    chase down the list
   */
  while( true ) {
    /**
      fetch element (at dp)
     */
    uint64_t dp_rank;
    uint64_t dp_offset;
    GDA_GetDPointer( &dp_offset, &dp_rank, dp );

    GDA_RMAHashMap_Element element;
    // TODO: change to MPI_UINT64_T
    // TODO: should be a get_accumulate call with MPI_NO_OP
    RMA_Get( &element, 4, MPI_INT64_T, dp_rank, dp_offset * 4, 4, MPI_INT64_T, hashmap->win_heap );
    RMA_Win_flush_local( dp_rank, hashmap->win_heap );

    if( element.next == dp ) {
      /**
        element points to itself -> in process of getting deleted
        restart from the beginning
       */
      GDA_FindElementInRMAHashMap( hashed_key, key, value, incarnation, found_flag, hashmap );
      return;
    }

    if (element.key == key) {
      /**
        found the element
       */
      *found_flag = true;
      *value = element.value;
      *incarnation = element.incarnation;
      return;
    }

    /**
      not the element in question -> iterate
     */
    dp = element.next;
    if( dp == GDA_DPOINTER_NULL ) {
      /**
        element was not found
       */
      *found_flag = false;
      *value = GDA_DPOINTER_NULL;
      return;
    }
  }
}


/**
  same code as GDA_RemoveElementFromRMAHashMap()
  but already marked the element for deletion, so we only need to repair the linked list

  TODO: Can it happen, that two ranks try to remove the same key, and one wins the first CAS and the other one the second?
 */
bool GDA_RemoveElementFromRMAHashMap_internal( uint64_t hashed_key, uint64_t key, uint64_t next, GDA_RMAHashMap hashmap ) {
  uint64_t hash = hashfunc( hashed_key, hashmap );

  uint64_t t_rank = hash / hashmap->table_size_local;
  /**
    counted in elements
   */
  // TODO: change this?
  uint64_t t_offset = hash % hashmap->table_size_local;

  /**
    check table
   */
  GDA_DPointer dp;
  // TODO: change to MPI_UINT64_T
  // TODO: should be a fetch_and_op call with MPI_NO_OP
  RMA_Get( &dp, 1, MPI_INT64_T, t_rank, t_offset, 1, MPI_INT64_T, hashmap->win_table );
  RMA_Win_flush_local( t_rank, hashmap->win_table );

  /**
    table can never be empty
   */
  // TODO: only for debug
  if( dp == GDA_DPOINTER_NULL ) {
    /**
      empty table slot
     */
    return false; // tested
  }

  /**
    have to handle the first element differently, as we will target
    for the second CAS the table and not the heap, which are two
    different windows
   */
  uint64_t dp_rank;
  uint64_t dp_offset;
  GDA_RMAHashMap_Element element;

  /**
    fetch element (at dp)
   */
  GDA_GetDPointer( &dp_offset, &dp_rank, dp );

  // TODO: change to MPI_UINT64_T
  // TODO: should be a get_accumulate call with MPI_NO_OP
  RMA_Get( &element, 4, MPI_INT64_T, dp_rank, dp_offset * 4, 4, MPI_INT64_T, hashmap->win_heap );
  RMA_Win_flush_local( dp_rank, hashmap->win_heap );

  if( element.next == dp ) {
    if( element.key != key ) {
      /**
        element points to itself -> in process of getting deleted
        restart from the beginning
       */
      return GDA_RemoveElementFromRMAHashMap_internal( hashed_key, key, next, hashmap );
    } else {
      /**
        found element
       */
      uint64_t swap_result;

      /**
        element is marked for deletion: use table this time
       */
      RMA_Compare_and_swap( &next, &dp /* compare */, &swap_result, MPI_UINT64_T, t_rank, t_offset, hashmap->win_table );
      RMA_Win_flush_local( t_rank, hashmap->win_table );

      if( swap_result == dp ) {
        GDA_DeallocateElementOfRMAHashMap( dp, hashmap );
        return true; // tested
      } else {
        /**
          failed to update the previous element
          have to restart but still have to remember the pointer to the next element
         */
        return GDA_RemoveElementFromRMAHashMap_internal( hashed_key, key, next, hashmap );
      }
    }
  }

  /**
    pointer to the next pointer of the previous element
   */
  GDA_DPointer previous;
  GDA_SetDPointer( dp_offset * 4 + 3, dp_rank, &previous );
  dp = element.next;
  // TODO: Can this happen?
  if( dp == GDA_DPOINTER_NULL ) {
    return false; // tested
  }

  /**
    chase down the list
   */
  while( true ) {
    /**
      fetch element (at dp):
     */
    GDA_GetDPointer( &dp_offset, &dp_rank, dp );

    // TODO: change to MPI_UINT64_T
    // TODO: should be a get_accumlate call with MPI_NO_OP
    RMA_Get( &element, 4, MPI_INT64_T, dp_rank, dp_offset * 4, 4, MPI_INT64_T, hashmap->win_heap );
    RMA_Win_flush_local( dp_rank, hashmap->win_heap );

    if( element.next == dp ) {
      if( element.key != key ) {
        /**
          element points to itself -> in process of getting deleted
          restart from the beginning
         */
        return GDA_RemoveElementFromRMAHashMap_internal( hashed_key, key, next, hashmap );
      } else {
        // found element
        uint64_t swap_result;

        // element is marked for deletion
        GDA_GetDPointer( &dp_offset, &dp_rank, previous );
        RMA_Compare_and_swap( &next, &dp /* compare */, &swap_result, MPI_UINT64_T, dp_rank, dp_offset, hashmap->win_heap );
        RMA_Win_flush_local( dp_rank, hashmap->win_heap );

        if( swap_result == dp ) {
          GDA_DeallocateElementOfRMAHashMap( dp, hashmap );
          return true; // tested
        } else {
          /**
            failed to update the previous element
            have to restart but still have to remember the pointer to the next element
           */
          return GDA_RemoveElementFromRMAHashMap_internal( hashed_key, key, next, hashmap );
        }
      }
    }

    /**
      iterate
     */
    GDA_SetDPointer( dp_offset * 4 + 3, dp_rank, &previous );
    dp = element.next;
    // TODO: Can this happen?
    if( dp == GDA_DPOINTER_NULL ) {
      return false; // tested
    }
  }
}


bool GDA_RemoveElementFromRMAHashMap( uint64_t hashed_key, uint64_t key, GDA_RMAHashMap hashmap ) {
  uint64_t hash = hashfunc( hashed_key, hashmap );

  uint64_t t_rank = hash / hashmap->table_size_local;
  /**
    offset counted in elements
   */
  // TODO: change this?
  uint64_t t_offset = hash % hashmap->table_size_local;

  /**
    check table
   */
  GDA_DPointer dp;
  // TODO: change to MPI_UINT64_T
  // TODO: should be a fetch_and_op call with MPI_NO_OP
  RMA_Get( &dp, 1, MPI_INT64_T, t_rank, t_offset, 1, MPI_INT64_T, hashmap->win_table );
  RMA_Win_flush_local( t_rank, hashmap->win_table );

  if( dp == GDA_DPOINTER_NULL ) {
    /**
      empty table slot
     */
    return false; // tested
  }

  /**
    have to handle the first element differently, as we will target
    for the second CAS the table and not the heap, which are two
    different windows
   */
  uint64_t dp_rank;
  uint64_t dp_offset;
  GDA_RMAHashMap_Element element;

  /**
    fetch element (at dp)
   */
  GDA_GetDPointer( &dp_offset, &dp_rank, dp );

  // TODO: change to MPI_UINT64_T
  // TODO: should be a get_accumulate call with MPI_NO_OP
  RMA_Get( &element, 4, MPI_INT64_T, dp_rank, dp_offset * 4, 4, MPI_INT64_T, hashmap->win_heap );
  RMA_Win_flush_local( dp_rank, hashmap->win_heap );

  if( element.next == dp ) {
    /**
      element points to itself -> in process of getting deleted
      restart from the beginning
     */
    return GDA_RemoveElementFromRMAHashMap( hashed_key, key, hashmap );
  }

  if (element.key == key) {
    /**
      found element
     */
    uint64_t swap_result;
    RMA_Compare_and_swap( &dp, &(element.next) /* compare */, &swap_result, MPI_UINT64_T, dp_rank, dp_offset * 4 + 3, hashmap->win_heap );
    RMA_Win_flush_local( dp_rank, hashmap->win_heap );

    if( swap_result != element.next ) {
      /**
        CAS failed: either another rank is trying to delete this
        element, or a deletion of the next element is in progress
        -> restart from the beginning
       */
      return GDA_RemoveElementFromRMAHashMap( hashed_key, key, hashmap );
    }

    /**
      element is marked for deletion: use table this time
      */
    RMA_Compare_and_swap( &(element.next), &dp /* compare */, &swap_result, MPI_UINT64_T, t_rank, t_offset, hashmap->win_table );
    RMA_Win_flush_local( t_rank, hashmap->win_table );

    if( swap_result == dp ) {
      GDA_DeallocateElementOfRMAHashMap( dp, hashmap );
      return true; // tested
    } else {
      /**
        failed to update the previous element
        have to restart but still have to remember the pointer to the
        next element
       */
      return GDA_RemoveElementFromRMAHashMap_internal( hashed_key, key, element.next, hashmap );
    }
  }

  /**
    pointer to the next pointer of the previous element
   */
  GDA_DPointer previous;
  GDA_SetDPointer( dp_offset * 4 + 3, dp_rank, &previous );
  dp = element.next;
  if( dp == GDA_DPOINTER_NULL ) {
    return false; // tested
  }

  /**
    chase down the list
   */
  while( true ) {
    /**
      fetch element (at dp)
     */
    GDA_GetDPointer( &dp_offset, &dp_rank, dp );

    // TODO: change to MPI_UINT64_T
    // TODO: should be a get_accumulate call with MPI_NO_OP
    RMA_Get( &element, 4, MPI_INT64_T, dp_rank, dp_offset * 4, 4, MPI_INT64_T, hashmap->win_heap );
    RMA_Win_flush_local( dp_rank, hashmap->win_heap );

    if( element.next == dp ) {
      /**
        element points to itself -> in process of getting deleted
        restart from the beginning
       */
      return GDA_RemoveElementFromRMAHashMap( hashed_key, key, hashmap );
    }

    if( element.key == key ) {
      /**
        found element
       */
      uint64_t swap_result;
      RMA_Compare_and_swap( &dp, &(element.next) /* compare */, &swap_result, MPI_UINT64_T, dp_rank, dp_offset * 4 + 3, hashmap->win_heap );
      RMA_Win_flush_local( dp_rank, hashmap->win_heap );

      if( swap_result != element.next ) {
        /**
          CAS failed: either another rank is trying to delete this
          element, or a deletion of the next element is in progress
          -> restart from the beginning
         */
        return GDA_RemoveElementFromRMAHashMap( hashed_key, key, hashmap );
      }

      /**
        element is marked for deletion
       */
      GDA_GetDPointer( &dp_offset, &dp_rank, previous );
      RMA_Compare_and_swap( &(element.next), &dp /* compare */, &swap_result, MPI_UINT64_T, dp_rank, dp_offset, hashmap->win_heap );
      RMA_Win_flush_local( dp_rank, hashmap->win_heap );

      if( swap_result == dp ) {
        GDA_DeallocateElementOfRMAHashMap( dp, hashmap );
        return true; // tested
      } else {
        /**
          failed to update the previous element
          have to restart but still have to remember the pointer to the next element
         */
        return GDA_RemoveElementFromRMAHashMap_internal( hashed_key, key, element.next, hashmap );
      }
    }

    /**
      iterate
     */
    GDA_SetDPointer( dp_offset * 4 + 3, dp_rank, &previous );
    dp = element.next;
    if( dp == GDA_DPOINTER_NULL ) {
      return false; // tested
    }
  }
}
