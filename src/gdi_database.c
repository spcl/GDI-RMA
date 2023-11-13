// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <assert.h>

#include "gdi.h"
#include "gda_block.h"
#include "gda_constraint.h"
#include "gda_label.h"
#include "gda_property_type.h"
#include "gda_vertex.h"

int GDI_CreateDatabase( void* params, size_t size, GDI_Database* graph_db ) {
  /**
    check the input arguments
   */
  if( (params == NULL) || (graph_db == NULL) ) {
    return GDI_ERROR_BUFFER;
  }

  if( size != sizeof(GDA_Init_params) ) {
    return GDI_ERROR_SIZE;
  }

  /**
    retrieve arguments from the params data structure
   */
  GDA_Init_params* gda_params = (GDA_Init_params*) params;

  /**
    implementation specific parameter check
   */
  if( gda_params->comm == MPI_COMM_NULL ) {
    /**
      NOD-specific error code
     */
    return GDI_ERROR_COMMUNICATOR;
  }

  /**
    test for the bare minimum of the block size, because we need at
    least enough space for the following data in the primary block:
    size of meta data + size of DPointer to next block
   */
  if( gda_params->block_size < (GDA_VERTEX_METADATA_SIZE+sizeof(GDA_DPointer)) ) {
    /**
      NOD-specific error code
     */
    return GDI_ERROR_BLOCK_SIZE;
  }

  if( gda_params->block_size > gda_params->memory_size ) {
    /**
      NOD-specific error code
     */
    return GDI_ERROR_BLOCK_SIZE;
  }

#ifdef RMA_USE_FOMPI
  /**
    DMAPP requires a 4 Byte alignment for local and remote address and
    the length on Gemini systems and on Aries system just for the
    remote address and the length

    https://pubs.cray.com/bundle/XC_Series_GNI_and_DMAPP_API_User_Guide_CLE70UP02_S-2446/page/RDMA__BTE___PostRdma.html#C319673
   */
  if( (gda_params->block_size % 4) != 0 ) {
    return GDI_ERROR_BLOCK_SIZE;
  }
  /**
    XPMEM part of foMPI has a size argument of type int in the version
    installed on our test machine, so it is not possible to use more
    than 2GB of memory
   */
  if( gda_params->memory_size > 2147483648 /* = 2 GB */ ) {
    // TODO: appropriate error class?
    return GDI_ERROR_NO_MEMORY;
  }
#endif // #ifdef RMA_USE_FOMPI
 
  GDI_Database internal_graph_db;
    /* allocate the database data structure */
  internal_graph_db = malloc( sizeof(GDI_Database_desc_t) );
  assert(internal_graph_db != NULL);
  internal_graph_db->memsize = gda_params->memory_size;
  internal_graph_db->block_size = gda_params->block_size;
  /**
    duplicate the MPI communicator because of section 6.9.1 of the
    MPI standard
   */
  MPI_Comm_dup( gda_params->comm, &(internal_graph_db->comm) );

  /**
    set additional members of the object
   */
  {
    int temp;
    MPI_Comm_size( internal_graph_db->comm, &temp );
    internal_graph_db->commsize = temp;
    MPI_Comm_rank( internal_graph_db->comm, &(internal_graph_db->commrank) );
  }

  /**
    additional checks
   */

#ifndef NDEBUG
  /**
    for performance reasons we only perform this check while being in
    debug mode

    check whether every process supplied the same size argument
   */
  {
    MPI_Aint max, local_value, sum;
    MPI_Allreduce( &(internal_graph_db->memsize), &max, 1, MPI_AINT, MPI_MAX, internal_graph_db->comm );
    if( max == internal_graph_db->memsize ) {
      local_value = 1;
    } else {
      local_value = 0;
    }
    MPI_Allreduce( &local_value, &sum, 1, MPI_AINT, MPI_SUM, internal_graph_db->comm );

    if( sum != internal_graph_db->commsize ) {
      /* memsize is different on at least one of the processes */

      /* clean up */
      MPI_Comm_free( &(internal_graph_db->comm) );
      free( internal_graph_db );
      return GDI_ERROR_NOT_SAME;
    }
  }
#endif // #ifndef NDEBUG

  /* passed all input checks, so it is safe to assign the output buffer */
  *graph_db = internal_graph_db;

  /**
    init the layer that handles the blocks
   */
  GDA_InitBlock( *graph_db );

  /**
    create the list that keeps track of all transactions,
    in which the local process takes part
   */
  GDA_list_create( &((*graph_db)->transactions), sizeof(GDI_Transaction) /* element size */ );
  (*graph_db)->collective_flag = false;

  /* Label data structures */
  (*graph_db)->labels = malloc( sizeof(GDI_Label_db_t) );
  assert( (*graph_db)->labels != NULL);
  (*graph_db)->labels->label_max = 1; // 0 is occupied by GDI_LABEL_NONE
  GDA_list_create( &((*graph_db)->labels->labels), sizeof(void*));
  assert((*graph_db)->labels->labels != NULL);
  GDA_hashmap_create( &((*graph_db)->labels->name_to_address), sizeof(uint64_t), 16, sizeof(void*), &GDA_int64_to_int);
  assert( (*graph_db)->labels->name_to_address != NULL);
  GDA_hashmap_create( &((*graph_db)->labels->handle_to_address), sizeof(uint32_t), 16, sizeof(void*), &GDA_int_to_int);
  assert( (*graph_db)->labels->handle_to_address != NULL);

  /* Constraint and subconstraint data structures */
  (*graph_db)->constraints = malloc(sizeof(GDI_Constraint_db_t));
  assert( (*graph_db)->constraints != NULL);
  GDA_list_create(&((*graph_db)->constraints->constraints), sizeof(void*));
  assert( (*graph_db)->constraints->constraints != NULL);
  GDA_list_create(&((*graph_db)->constraints->subconstraints), sizeof(void*));
  assert( (*graph_db)->constraints->subconstraints != NULL);
  GDA_hashmap_create(&((*graph_db)->constraints->property_to_condition), sizeof(void*), 16, sizeof(void*), &GDA_int64_to_int );
  assert((*graph_db)->constraints->property_to_condition != NULL);
  GDA_hashmap_create(&((*graph_db)->constraints->label_to_condition), sizeof(void*), 16, sizeof(void*), &GDA_int64_to_int );
  assert((*graph_db)->constraints->label_to_condition != NULL);

  /* Property type data structures */
  (*graph_db)->ptypes = malloc( sizeof(GDI_PropertyType_db_t) );
  assert( (*graph_db)->ptypes != NULL);
  (*graph_db)->ptypes->ptype_max = 4; // 0 to 3 is occupied by the predefined property types
  GDA_list_create( &((*graph_db)->ptypes->ptypes), sizeof(void*));
  assert((*graph_db)->ptypes->ptypes != NULL);
  GDA_hashmap_create( &((*graph_db)->ptypes->name_to_address), sizeof(uint64_t), 16, sizeof(void*), &GDA_int64_to_int);
  assert( (*graph_db)->ptypes->name_to_address != NULL);
  GDA_hashmap_create( &((*graph_db)->ptypes->handle_to_address), sizeof(uint32_t), 16, sizeof(void*), &GDA_int_to_int);
  assert( (*graph_db)->ptypes->handle_to_address != NULL);

  /**
    set up the distributed hashtable for the internal index

    calculate the total number of blocks in the database first
   */
  size_t num_blocks = internal_graph_db->memsize / internal_graph_db->block_size * internal_graph_db->commsize;
  GDA_CreateRMAHashMap( num_blocks/3 /* table size */, 2 * num_blocks /* heap size */, internal_graph_db->comm, &(internal_graph_db->internal_index) );

  /**
    ensure that all processes have set up and initialized their remote
    accessible data structures
   */
  MPI_Barrier( (*graph_db)->comm );

  return GDI_SUCCESS;
}


int GDI_FreeDatabase( GDI_Database* graph_db ) {
  /**
    check the input argument
   */
  if( graph_db == NULL ) {
    return GDI_ERROR_DATABASE;
  }

  if( *graph_db == GDI_DATABASE_NULL ) {
    return GDI_ERROR_DATABASE;
  }

  GDA_FreeBlock( *graph_db );
  GDA_FreeRMAHashMap( &((*graph_db)->internal_index) );

  /**
    free duplicate of the original communicator
   */
  MPI_Comm_free( &((*graph_db)->comm) ); 

  /**
    free transactions
   */
  GDA_Node* transaction_node = (*graph_db)->transactions->head;
  while( transaction_node != NULL ) {
    // TODO: might be additional work necessary

    GDI_Transaction transaction = *(GDI_Transaction*)(transaction_node->value);
    /**
      free vertex objects from this transaction
     */
    size_t vec_size = transaction->vertices->size;
    for( size_t i=0 ; i<vec_size ; i++ ) {
      // TODO: might be additional work necessary
      GDI_VertexHolder vertex = *(GDI_VertexHolder*)GDA_vector_at( transaction->vertices, i );
      free( vertex->property_data );
      free( vertex->lightweight_edge_data );
      GDA_list_free( &(vertex->edges) );
      GDA_vector_free( &(vertex->blocks) );
      free( vertex );
    }
    GDA_vector_free( &(transaction->vertices) );

    /**
      free edge objects from this transaction
     */
    vec_size = transaction->edges->size;
    for( size_t i=0 ; i<vec_size ; i++ ) {
      // TODO: might be additional work necessary
      free( *(GDI_EdgeHolder*)GDA_vector_at( transaction->edges, i ) );
    }
    GDA_vector_free( &(transaction->edges) );

    /**
      free the vertex translation hash map
     */
    GDA_hashmap_free( &(transaction->v_translate_d2l) );

    /**
      free transaction object
     */
    free( transaction );
    transaction_node = transaction_node->next;
  }
  GDA_list_free( &((*graph_db)->transactions) );

  /* Free all label data structures */
  GDA_FreeAllLabel(*graph_db);
  GDA_hashmap_free( &((*graph_db)->labels->name_to_address) );
  GDA_hashmap_free( &((*graph_db)->labels->handle_to_address ) );
  GDA_list_free( &((*graph_db)->labels->labels));
  free((*graph_db)->labels);

  /* Free all constraint and subconstraint data structures */
  GDA_FreeAllConstraint(*graph_db);
  GDA_FreeAllSubconstraint(*graph_db);
  GDA_list_free(&((*graph_db)->constraints->constraints));
  GDA_list_free(&((*graph_db)->constraints->subconstraints));
  GDA_hashmap_free(&((*graph_db)->constraints->property_to_condition));
  GDA_hashmap_free(&((*graph_db)->constraints->label_to_condition));
  free((*graph_db)->constraints);

  /* Free all property type data structures */
  GDA_FreeAllPropertyType(*graph_db);
  GDA_hashmap_free( &((*graph_db)->ptypes->name_to_address) );
  GDA_hashmap_free( &((*graph_db)->ptypes->handle_to_address ) );
  GDA_list_free( &((*graph_db)->ptypes->ptypes));
  free((*graph_db)->ptypes);

  free(*graph_db);
  *graph_db = GDI_DATABASE_NULL;

  return GDI_SUCCESS;
}
