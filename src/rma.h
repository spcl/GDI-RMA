/**
  Everything below is direct copy from the rma.h file of the foMPI
  library.

  https://spcl.inf.ethz.ch/Research/Parallel_Programming/foMPI/
*/

// Copyright (c) 2019 ETH-Zurich. All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can
// be found in the LICENSE file.

// this is used to either use MPI or foMPI
// it will translate RMA_... to either MPI_... or foMPI_... as required.

#ifdef RMA_USE_FOMPI
  // foMPI
  #include "fompi.h"

// ##########################################  FUNCTIONS  ###
// #### window creation
  #define RMA_Win_create foMPI_Win_create
  #define RMA_Win_allocate foMPI_Win_allocate
  #define RMA_Win_create_dynamic foMPI_Win_create_dynamic
  #define RMA_Win_free foMPI_Win_free

  #define RMA_Win_attach foMPI_Win_attach
  #define RMA_Win_detach foMPI_Win_detach

// #### synchronization
  #define RMA_Win_fence foMPI_Win_fence

  #define RMA_Win_post foMPI_Win_post
  #define RMA_Win_start foMPI_Win_start
  #define RMA_Win_complete foMPI_Win_complete
  #define RMA_Win_wait foMPI_Win_wait
  #define RMA_Win_test foMPI_Win_test

  #define RMA_Win_lock foMPI_Win_lock
  #define RMA_Win_unlock foMPI_Win_unlock
  #define RMA_Win_lock_all foMPI_Win_lock_all
  #define RMA_Win_unlock_all foMPI_Win_unlock_all

  #define RMA_Win_flush foMPI_Win_flush
  #define RMA_Win_flush_all foMPI_Win_flush_all
  #define RMA_Win_flush_local foMPI_Win_flush_local
  #define RMA_Win_flush_local_all foMPI_Win_flush_local_all
  #define RMA_Win_sync foMPI_Win_sync

// #### communication
  #define RMA_Put foMPI_Put
  #define RMA_Get foMPI_Get
  #define RMA_Accumulate foMPI_Accumulate
  #define RMA_Get_accumulate foMPI_Get_accumulate
  #define RMA_Fetch_and_op foMPI_Fetch_and_op
  #define RMA_Compare_and_swap foMPI_Compare_and_swap

  #define RMA_Rput foMPI_Rput
  #define RMA_Rget foMPI_Rget
  #define RMA_Raccumulate foMPI_Raccumulate
  #define RMA_Rget_accumulate foMPI_Rget_accumulate

// #### miscellaneous
  #define RMA_Win_get_group foMPI_Win_get_group

  #define RMA_Win_set_name foMPI_Win_set_name
  #define RMA_Win_get_name foMPI_Win_get_name

// #### overloaded
  #define RMA_Type_free foMPI_Type_free
  #define RMA_Init foMPI_Init
  #define RMA_Finalize foMPI_Finalize

  #define RMA_Wait foMPI_Wait
  #define RMA_Test foMPI_Test

// ##########################################  TYPEDEF  ###
  #define RMA_Win foMPI_Win
 
// ##########################################  CONSTANTS ###
// #### create flavor
  #define RMA_WIN_FLAVOR_CREATE foMPI_WIN_FLAVOR_CREATE
  #define RMA_WIN_FLAVOR_ALLOCATE foMPI_WIN_FLAVOR_ALLOCATE
  #define RMA_WIN_FLAVOR_DYNAMIC foMPI_WIN_FLAVOR_DYNAMIC

// #### memory model
  #define RMA_WIN_SEPARATE foMPI_WIN_SEPARATE
  #define RMA_WIN_UNIFIED foMPI_WIN_UNIFIED

// #### asserts
  #define RMA_MODE_NOCHECK foMPI_MODE_NOCHECK
  #define RMA_MODE_NOSTORE foMPI_MODE_NOSTORE
  #define RMA_MODE_NOPUT foMPI_MODE_NOPUT
  #define RMA_MODE_NOPRECEDE foMPI_MODE_NOPRECEDE
  #define RMA_MODE_NOSUCCEED foMPI_MODE_NOSUCCEED

// #### accumulate operations
  #define RMA_SUM foMPI_SUM
  #define RMA_PROD foMPI_PROD
  #define RMA_MAX foMPI_MAX
  #define RMA_MIN foMPI_MIN
  #define RMA_LAND foMPI_LAND
  #define RMA_LOR foMPI_LOR
  #define RMA_LXOR foMPI_LXOR
  #define RMA_BAND foMPI_BAND
  #define RMA_BOR foMPI_BOR
  #define RMA_BXOR foMPI_BXOR
  #define RMA_MAXLOC foMPI_MAXLOC
  #define RMA_MINLOC foMPI_MINLOC
  #define RMA_REPLACE foMPI_REPLACE
  #define RMA_NO_OP foMPI_NO_OP
  
// #### lock types
  #define RMA_LOCK_SHARED foMPI_LOCK_SHARED
  #define RMA_LOCK_EXCLUSIVE foMPI_LOCK_EXCLUSIVE

#else
  // standard MPI
  #include <mpi.h>

// ##########################################  FUNCTIONS  ###
// #### window creation
  #define RMA_Win_create MPI_Win_create
  #define RMA_Win_allocate MPI_Win_allocate
  #define RMA_Win_create_dynamic MPI_Win_create_dynamic
  #define RMA_Win_free MPI_Win_free

  #define RMA_Win_attach MPI_Win_attach
  #define RMA_Win_detach MPI_Win_detach

// #### synchronization
  #define RMA_Win_fence MPI_Win_fence

  #define RMA_Win_post MPI_Win_post
  #define RMA_Win_start MPI_Win_start
  #define RMA_Win_complete MPI_Win_complete
  #define RMA_Win_wait MPI_Win_wait
  #define RMA_Win_test MPI_Win_test

  #define RMA_Win_lock MPI_Win_lock
  #define RMA_Win_unlock MPI_Win_unlock
  #define RMA_Win_lock_all MPI_Win_lock_all
  #define RMA_Win_unlock_all MPI_Win_unlock_all

  #define RMA_Win_flush MPI_Win_flush
  #define RMA_Win_flush_all MPI_Win_flush_all
  #define RMA_Win_flush_local MPI_Win_flush_local
  #define RMA_Win_flush_local_all MPI_Win_flush_local_all
  #define RMA_Win_sync MPI_Win_sync

// #### communication
  #define RMA_Put MPI_Put
  #define RMA_Get MPI_Get
  #define RMA_Accumulate MPI_Accumulate
  #define RMA_Get_accumulate MPI_Get_accumulate
  #define RMA_Fetch_and_op MPI_Fetch_and_op
  #define RMA_Compare_and_swap MPI_Compare_and_swap

  #define RMA_Rput MPI_Rput
  #define RMA_Rget MPI_Rget
  #define RMA_Raccumulate MPI_Raccumulate
  #define RMA_Rget_accumulate MPI_Rget_accumulate

// #### miscellaneous
  #define RMA_Win_get_group MPI_Win_get_group

  #define RMA_Win_set_name MPI_Win_set_name
  #define RMA_Win_get_name MPI_Win_get_name

// #### overloaded
  #define RMA_Type_free MPI_Type_free
  #define RMA_Init MPI_Init
  #define RMA_Finalize MPI_Finalize

  #define RMA_Wait MPI_Wait
  #define RMA_Test MPI_Test

// ##########################################  TYPEDEF  ###
  #define RMA_Win MPI_Win

// ##########################################  CONSTANTS ###
// #### create flavor
  #define RMA_WIN_FLAVOR_CREATE MPI_WIN_FLAVOR_CREATE
  #define RMA_WIN_FLAVOR_ALLOCATE MPI_WIN_FLAVOR_ALLOCATE
  #define RMA_WIN_FLAVOR_DYNAMIC MPI_WIN_FLAVOR_DYNAMIC

// #### memory model
  #define RMA_WIN_SEPARATE MPI_WIN_SEPARATE
  #define RMA_WIN_UNIFIED MPI_WIN_UNIFIED

// #### asserts
  #define RMA_MODE_NOCHECK MPI_MODE_NOCHECK
  #define RMA_MODE_NOSTORE MPI_MODE_NOSTORE
  #define RMA_MODE_NOPUT MPI_MODE_NOPUT
  #define RMA_MODE_NOPRECEDE MPI_MODE_NOPRECEDE
  #define RMA_MODE_NOSUCCEED MPI_MODE_NOSUCCEED

// #### accumulate operations
  #define RMA_SUM MPI_SUM
  #define RMA_PROD MPI_PROD
  #define RMA_MAX MPI_MAX
  #define RMA_MIN MPI_MIN
  #define RMA_LAND MPI_LAND
  #define RMA_LOR MPI_LOR
  #define RMA_LXOR MPI_LXOR
  #define RMA_BAND MPI_BAND
  #define RMA_BOR MPI_BOR
  #define RMA_BXOR MPI_BXOR
  #define RMA_MAXLOC MPI_MAXLOC
  #define RMA_MINLOC MPI_MINLOC
  #define RMA_REPLACE MPI_REPLACE
  #define RMA_NO_OP MPI_NO_OP

// #### lock types
  #define RMA_LOCK_SHARED MPI_LOCK_SHARED
  #define RMA_LOCK_EXCLUSIVE MPI_LOCK_EXCLUSIVE

#endif
