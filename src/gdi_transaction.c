// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#include <assert.h>
#include <string.h>

#include "gdi.h"
#include "gda_block.h"
#include "gda_dpointer.h"
#include "gda_lightweight_edges.h"
#include "gda_lock.h"
#include "gda_property.h"
#include "gda_vertex.h"

/**
  constant definitions
 */
#define LOCK_SINGLE_WRITER  1

int GDI_StartTransaction( GDI_Database graph_db, GDI_Transaction* transaction ) {
  /**
    check the input arguments
   */
  if( transaction == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  if( graph_db == GDI_DATABASE_NULL ) {
    return GDI_ERROR_DATABASE;
  }

  if( graph_db->collective_flag ) {
    /**
      collective transaction currently active
     */
    return GDI_ERROR_INCOMPATIBLE_TRANSACTIONS;
  }

  /**
    passed all input checks, so it is safe to create the output buffer
   */
  *transaction = malloc( sizeof(GDI_Transaction_desc_t) );
  assert( *transaction != NULL );

  /**
    init data structure
   */
  (*transaction)->db = graph_db;
  (*transaction)->type = GDI_SINGLE_PROCESS_TRANSACTION;
  (*transaction)->write_flag = false /* read only */;
  (*transaction)->critical_flag = false /* no problems */;
  /**
    init vectors that will keep track of all vertex and edge objects
   */
  GDA_vector_create( &((*transaction)->vertices), sizeof(GDI_VertexHolder), 8 /* initial capacity */ );
  GDA_vector_create( &((*transaction)->edges), sizeof(GDI_EdgeHolder), 8 /* initial capacity */ );
  /**
    init hash map that will translate vertex UIDs to vertex handles,
    in case a vertex is already associated with this transaction
   */
  GDA_hashmap_create( &((*transaction)->v_translate_d2l), sizeof(GDA_DPointer) /* key size */, 32 /* capacity */, sizeof(void*) /* value_size */, &GDA_int64_to_int );

  /**
    add current transaction to the list of transactions
    of the graph database
   */
  (*transaction)->db_listptr = GDA_list_push_back( graph_db->transactions, transaction );

  return GDI_SUCCESS;
}


int GDI_CloseTransaction( GDI_Transaction* transaction, int ctype ) {
  /**
    check the input arguments
   */
  if( transaction == NULL ) {
    return GDI_ERROR_TRANSACTION;
  }
  if( *transaction == GDI_TRANSACTION_NULL ) {
    return GDI_ERROR_TRANSACTION;
  }

  if( (ctype != GDI_TRANSACTION_COMMIT) && (ctype != GDI_TRANSACTION_ABORT) ) {
    return GDI_ERROR_STATE;
  }

  if( (*transaction)->type != GDI_SINGLE_PROCESS_TRANSACTION ) {
    return GDI_ERROR_WRONG_TYPE;
  }

  /**
    passed all input checks
   */
  size_t vec_size = (*transaction)->vertices->size;
  bool commit_changes = (ctype == GDI_TRANSACTION_COMMIT) && !((*transaction)->critical_flag) && (*transaction)->write_flag;

  if( commit_changes ) {
    /**
      Only have to do the following steps, if we want to commit and no transaction critical
      error happened during the transaction and there is actually something to write back.
     */
    uint64_t block_size = (*transaction)->db->block_size;

    char** buffers;
    size_t buf_index = 0;
    buffers = malloc( vec_size * 4 * sizeof(char*) );

    for( size_t j=0 ; j<vec_size ; j++ ) {
      GDI_VertexHolder vertex = *(GDI_VertexHolder*)GDA_vector_at( (*transaction)->vertices, j );

      if( vertex->delete_flag ) {
        /**
          vertex is marked for deletion, so release its blocks
         */
        size_t num_blocks = vertex->blocks->size;
        for( size_t i=0 ; i<num_blocks ; i++ ) {
          GDA_DPointer* dp_ptr = GDA_vector_at( vertex->blocks, i );
          GDA_DeallocateBlock( *dp_ptr, (*transaction)->db );
        }
      } else {
        if( vertex->write_flag ) {
          /**
            vertex was changed during the transaction, but is not marked for deletion
           */
          uint64_t remaining_lightweight_edge_data = vertex->lightweight_edge_insert_offset * sizeof(GDA_DPointer);
          if( ((vertex->lightweight_edge_insert_offset - 2) % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE) == 0 ) {
            /**
              omit the lightweight edge meta and label data of the next block
             */
            remaining_lightweight_edge_data -= 2 * sizeof(GDA_DPointer);
          }
          // TODO: property data not calculated correctly
          uint64_t remaining_property_data = vertex->property_size;
          uint64_t total_vertex_size = GDA_VERTEX_METADATA_SIZE + remaining_lightweight_edge_data + remaining_property_data;
          uint32_t total_num_blocks = 1;
          if( total_vertex_size > block_size ) {
            /**
              don't store the address for the primary block
             */
            uint64_t temp_size = total_vertex_size - block_size;
            uint64_t temp_block_size = block_size - sizeof(GDA_DPointer);
            total_num_blocks += (temp_size + temp_block_size - 1 /* round up */)/temp_block_size;
          }

          // TODO: this is wrong, we have to make sure first, that we have enough resources for all vertices, before we start pushing changes
          if( total_num_blocks > vertex->blocks->size ) {
            /**
              not enough blocks associated with this vertex

              we will try to get new blocks from the same rank
              that stores the primary block
             */
            GDA_DPointer* primary_block = GDA_vector_at( vertex->blocks, 0 );
            uint64_t offset, target_rank;
            GDA_GetDPointer( &offset, &target_rank, *primary_block );
            for( uint64_t i=vertex->blocks->size ; i<total_num_blocks ; i++ ) {
              GDA_DPointer dpointer = GDA_AllocateBlock( target_rank, (*transaction)->db );
              if( dpointer == GDA_DPOINTER_NULL ) {
                /**
                  couldn't acquire enough resources
                 */
                // TODO
                assert( 0 );
              }
              GDA_vector_push_back( vertex->blocks, &dpointer );
            }
          } else {
            if( total_num_blocks < vertex->blocks->size ) {
              /**
                more blocks than necessary
               */
              for( uint64_t i=vertex->blocks->size-1 ; i>total_num_blocks ; i-- ) {
                GDA_DPointer* dp_ptr = GDA_vector_at( vertex->blocks, i );
                GDA_DeallocateBlock( *dp_ptr, (*transaction)->db );
              }
            }
          }
          assert( total_num_blocks == vertex->blocks->size );

          char* buf = malloc( block_size );
          assert( buf != NULL );
          buffers[buf_index++] = buf;

          /**
            create the meta data structure

            -----------------
            |#blocks| #edges|
            -----------------
            | property size |
            -----------------
            | unused size   |
            -----------------
           */
          /**
            number of blocks, including the primary block
           */
          *(uint32_t*)(buf+GDA_OFFSET_NUM_BLOCKS) = vertex->blocks->size;
          /**
            need to calculate the actual number of lightweight edges
            assumes that shrink has been called before
           */
          *(uint32_t*)(buf+GDA_OFFSET_NUM_LIGHTWEIGHT_EDGES) = (vertex->lightweight_edge_insert_offset - 2) / GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE * (GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE-2)
                                                               + (vertex->lightweight_edge_insert_offset - 2) % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE;
          *(uint64_t*)(buf+GDA_OFFSET_SIZE_PROPERTY_DATA) = vertex->property_size;
          *(uint64_t*)(buf+GDA_OFFSET_SIZE_UNUSED_SPACE) = vertex->unused_space;

          /**
            now we will prepare the rest of the primary block,
            since it has to be treated differently to the other
            ones
           */
          uint32_t remaining_buf_data = block_size - GDA_VERTEX_METADATA_SIZE;
          uint64_t remaining_block_data = (vertex->blocks->size - 1 /* primary block */) * sizeof(GDA_DPointer);
          /**
            counter for the next block to transfer
           */
          uint32_t block_index = 0;
          GDA_DPointer* dp = vertex->blocks->data;
          char* block_source_address = (char*)(vertex->blocks->data) + sizeof(GDA_DPointer);
          char* lightweight_edge_source_address = (char*)vertex->lightweight_edge_data;
          char* property_source_address = vertex->property_data;
          char* buf_target_address = buf+GDA_VERTEX_METADATA_SIZE;

          /**
            at this point we took care of the vertex meta data
           */

          if( remaining_block_data > 0 ) {
            if( remaining_buf_data < block_size ) {
              /**
                buffer already contains the vertex meta data
               */
              if( remaining_block_data <= remaining_buf_data ) {
                memcpy( buf_target_address, block_source_address, remaining_block_data );

                buf_target_address += remaining_block_data;
                remaining_buf_data -= remaining_block_data;
                remaining_block_data = 0;
              } else {
                /**
                  buffer is already filled with vertex meta data, but
                  there is not enough space for all of the block data
                 */
                memcpy( buf_target_address, block_source_address, remaining_buf_data );

                block_source_address += remaining_buf_data;
                remaining_block_data -= remaining_buf_data;
                remaining_buf_data = 0;
              }
            }

            if( remaining_buf_data == 0 ) {
              /**
                block is completely filled
               */
              GDA_PutBlock( buf, dp[block_index++], (*transaction)->db );

              buf = malloc( block_size );
              assert( buf != NULL );
              buffers[buf_index++] = buf;
              buf_target_address = buf;
              remaining_buf_data = block_size;
            }

            uint32_t num_block_data_blocks = remaining_block_data/block_size;

            for( uint32_t i=0 ; i<num_block_data_blocks ; i++ ) {
              GDA_PutBlock( block_source_address, dp[block_index++], (*transaction)->db );

              block_source_address += block_size;
            }

            remaining_block_data -= num_block_data_blocks * block_size;

            if( remaining_block_data > 0 ) {
              /**
                since we didn't transmit the data with the mechanism
                above, the buffer won't be fully filled
               */
              memcpy( buf_target_address, block_source_address, remaining_block_data );

              buf_target_address += remaining_block_data;
              remaining_buf_data -= remaining_block_data;
            }
          }

          /**
            at this point we took care of the block address data
           */

          if( remaining_lightweight_edge_data > 0 ) {
            if( remaining_buf_data < block_size ) {
              /**
                buffer already contains some data from the blocks
               */
              if( remaining_lightweight_edge_data <= remaining_buf_data ) {
                memcpy( buf_target_address, lightweight_edge_source_address, remaining_lightweight_edge_data );

                buf_target_address += remaining_lightweight_edge_data;
                remaining_buf_data -= remaining_lightweight_edge_data;
                remaining_lightweight_edge_data = 0;
              } else {
                /**
                  some block address data is in the buffer,
                  but there is not enough space for all of
                  the lightweight edge data
                 */
                memcpy( buf_target_address, lightweight_edge_source_address, remaining_buf_data );

                lightweight_edge_source_address += remaining_buf_data;
                remaining_lightweight_edge_data -= remaining_buf_data;
                remaining_buf_data = 0;
              }
            }

            if( remaining_buf_data == 0 ) {
              /**
                block is completely filled
               */
              GDA_PutBlock( buf, dp[block_index++], (*transaction)->db );

              buf = malloc( block_size );
              assert( buf != NULL );
              buffers[buf_index++] = buf;
              buf_target_address = buf;
              remaining_buf_data = block_size;
            }

            /**
              now we can start transmitting the lightweight edge data directly
             */
            uint32_t num_block_lightweight_edges = remaining_lightweight_edge_data/block_size;

            for( uint32_t i=0 ; i<num_block_lightweight_edges ; i++ ) {
              GDA_PutBlock( lightweight_edge_source_address, dp[block_index++], (*transaction)->db );

              lightweight_edge_source_address += block_size;
            }

            remaining_lightweight_edge_data -= num_block_lightweight_edges * block_size;

            if( remaining_lightweight_edge_data > 0 ) {
              /**
                since we didn't transmit the data with the mechanism
                above, the buffer won't be fully filled
               */
              memcpy( buf_target_address, lightweight_edge_source_address, remaining_lightweight_edge_data );

              buf_target_address += remaining_lightweight_edge_data;
              remaining_buf_data -= remaining_lightweight_edge_data;
            }
          }

          /**
            at this point we took care of the lightweight edge data
           */

          if( remaining_property_data > 0 ) {
            if( remaining_buf_data < block_size ) {
              /**
                buffer already contains some data from the other segments
               */
              if( remaining_property_data <= remaining_buf_data ) {
                memcpy( buf_target_address, property_source_address, remaining_property_data );

                /**
                  no need to set the other variables, since we are done
                  especially won't change remaining_buf_data, so that
                  we don't allocate another (unnecessary in this case)
                  buffer
                 */
                remaining_property_data = 0;
              } else {
                /**
                  buffer already contains some data from the other segments,
                  but there is not enough space for all of the property data
                 */
                memcpy( buf_target_address, property_source_address, remaining_buf_data );

                property_source_address += remaining_buf_data;
                remaining_property_data -= remaining_buf_data;
                remaining_buf_data = 0;
              }
            }

            if( remaining_buf_data == 0 ) {
              /**
                block is completely filled
               */
              GDA_PutBlock( buf, dp[block_index++], (*transaction)->db );

              buf = malloc( block_size );
              assert( buf != NULL );
              buffers[buf_index++] = buf;
              remaining_buf_data = block_size;
            }

            /**
              now we can start transmitting the property data directly
             */
            uint32_t num_block_property = remaining_property_data/block_size;

            for( uint32_t i=0 ; i<num_block_property ; i++ ) {
              GDA_PutBlock( property_source_address, dp[block_index++], (*transaction)->db );

              property_source_address += block_size;
            }

            remaining_property_data -= num_block_property * block_size;

            if( remaining_property_data > 0 ) {
              /**
                since we didn't transmit the data with the mechanism
                above, the buffer won't be fully filled
               */
              memcpy( buf, property_source_address, remaining_property_data );

              /**
                no need to set the other variables, since we are done
                remaining_buf_data is only updated, so that we trigger
                the next if condition to get the last block transfered
               */
              remaining_buf_data -= remaining_property_data;
            }
          }

          if( remaining_buf_data < block_size ) {
            /**
              at least partially packed the buffer, so have to transfer
              the buffer
             */
            GDA_PutBlock( buf, dp[block_index++], (*transaction)->db );
          }

          assert( block_index == vertex->blocks->size );
        }
      }
    }

    /**
      TODO: do this later (after the index update)?
     */
    RMA_Win_flush_all( (*transaction)->db->win_blocks );

    /**
      clean up
     */
    for( size_t i=0 ; i<buf_index ; i++ ) {
      free( buffers[i] );
    }
    free( buffers );

    /**
      update the internal index

      TODO: also do this, when the vertex isn't newly created
     */
    for( size_t j=0 ; j<vec_size ; j++ ) {
      GDI_VertexHolder vertex = *(GDI_VertexHolder*)GDA_vector_at( (*transaction)->vertices, j );

      if( vertex->creation_flag || vertex->delete_flag ) {
        /**
          retrieve application-level ID
         */
        size_t id_size;
        size_t offset_resultcount;
        unsigned char* id_buf = malloc( 64 * sizeof(unsigned char) );
        size_t offsets[2];
        int ret;

        ret = GDA_LinearScanningFindAllProperties( id_buf, 64, &id_size, offsets, 2, &offset_resultcount, GDI_PROPERTY_TYPE_ID, vertex );

        if( ret == GDI_ERROR_TRUNCATE ) {
          GDA_LinearScanningNumProperties( vertex, GDI_PROPERTY_TYPE_ID, &offset_resultcount, &id_size );
          /* ignore offset_resultcount afterwards, so no need to fix */
          id_buf = realloc( id_buf, id_size * sizeof(unsigned char) );

          ret = GDA_LinearScanningFindAllProperties( id_buf, id_size, &id_size, offsets, 2, &offset_resultcount, GDI_PROPERTY_TYPE_ID, vertex );
        }

        assert( ret == GDI_SUCCESS );

        if( id_size != 0 ) {
          /**
            retrieve labels
           */
          size_t num_labels;
          GDI_Label* labels = malloc( 10 * sizeof(GDI_Label) );
          ret = GDA_LinearScanningFindAllLabels( vertex, labels, 10, &num_labels );

          if( ret == GDI_ERROR_TRUNCATE ) {
            GDA_LinearScanningNumLabels( vertex, &num_labels );
            labels = realloc( labels, num_labels * sizeof(GDI_Label) );

            ret = GDA_LinearScanningFindAllLabels( vertex, labels, num_labels, &num_labels );
          }

          assert( ret == GDI_SUCCESS );

          /**
            distributed pointer to the primary block
           */
          uint64_t value = *(uint64_t*)(vertex->blocks->data);

          size_t minimum = 7;
          if( id_size < minimum ) {
            minimum = id_size;
          }
          uint64_t key = 0;
          memcpy( &key, id_buf, minimum );

          if( vertex->creation_flag && !(vertex->delete_flag) ) {
            if( num_labels == 0 ) {
              /**
                have an application-level ID, but no labels
               */
              uint64_t hashed_key = GDA_hash_property_id( id_buf, id_size, GDI_LABEL_NONE->int_handle );

              key = (key & 0x00FFFFFFFFFFFFFF) | ((uint64_t)(GDI_LABEL_NONE->int_handle) << 56);

              GDA_InsertElementIntoRMAHashMap( hashed_key, key, value, vertex->incarnation, (*transaction)->db->internal_index );
            } else {
              for( size_t i=0 ; i<num_labels ; i++ ) {
                uint64_t hashed_key = GDA_hash_property_id( id_buf, id_size, (labels[i])->int_handle );

                key = (key & 0x00FFFFFFFFFFFFFF) | ((uint64_t)((labels[i])->int_handle) << 56);

                GDA_InsertElementIntoRMAHashMap( hashed_key, key, value, vertex->incarnation, (*transaction)->db->internal_index );
              }
            }
          } else {
            if( vertex->delete_flag && !(vertex->creation_flag) ) {
              /**
                TODO: this works incorrectly, because it is possible, that the application
                      already removed the ID property and/or labels, before marking the vertex
                      as deleted
               */
              if( num_labels == 0 ) {
                /**
                  have an application-level ID, but no labels
                 */
                uint64_t hashed_key = GDA_hash_property_id( id_buf, id_size, GDI_LABEL_NONE->int_handle );

                key = (key & 0x00FFFFFFFFFFFFFF) | ((uint64_t)(GDI_LABEL_NONE->int_handle) << 56);

                bool del_flag;
                del_flag = GDA_RemoveElementFromRMAHashMap( hashed_key, key, (*transaction)->db->internal_index );
                assert( del_flag );
              } else {
                bool del_flag;
                for( size_t i=0 ; i<num_labels ; i++ ) {
                  uint64_t hashed_key = GDA_hash_property_id( id_buf, id_size, (labels[i])->int_handle );

                  key = (key & 0x00FFFFFFFFFFFFFF) | ((uint64_t)((labels[i])->int_handle) << 56);

                  del_flag = GDA_RemoveElementFromRMAHashMap( hashed_key, key, (*transaction)->db->internal_index );
                  assert( del_flag );
                }
              }
            }
          }

          /**
            clean up
           */
          free( labels );
        }

        /**
          clean up
         */
        free( id_buf );
      }
    }

    // TODO: update indexes

  }

  /**
    release locks
   */
  for( size_t j = 0; j < vec_size; j++ ) {
    GDI_VertexHolder vertex = *(GDI_VertexHolder*)GDA_vector_at( ( *transaction )->vertices, j );

    GDA_ReleaseVertexLock( vertex );
  }

  /**
    remove transaction from the list of currently active transactions
   */
  GDA_list_erase_single( (*transaction)->db->transactions, (*transaction)->db_listptr );

  /**
    free all GDI_VertexHolder objects
   */
  for( size_t i=0 ; i<vec_size ; i++ ) {
    // TODO: might be additional work necessary
    GDI_VertexHolder vertex = *(GDI_VertexHolder*)GDA_vector_at( (*transaction)->vertices, i );
    free( vertex->property_data );
    free( vertex->lightweight_edge_data );
    GDA_list_free( &(vertex->edges) );
    GDA_vector_free( &(vertex->blocks) );
    free( vertex );
  }
  GDA_vector_free( &((*transaction)->vertices) );

  /**
    free all GDI_EdgeHolder objects
   */
  vec_size = (*transaction)->edges->size;
  for( size_t i=0 ; i<vec_size ; i++ ) {
    // TODO: might be additional work necessary
    free( *(GDI_EdgeHolder*)GDA_vector_at( (*transaction)->edges, i ) );
  }
  GDA_vector_free( &((*transaction)->edges) );

  /**
    free the vertex translation hash map
   */
  GDA_hashmap_free( &((*transaction)->v_translate_d2l) );

  bool critical = (*transaction)->critical_flag;

  free( *transaction );
  *transaction = GDI_TRANSACTION_NULL;

  if( (ctype == GDI_TRANSACTION_COMMIT) && critical ) {
    /**
      We wanted to commit, but a transaction critical error occured, and forced an abort.
     */
    return GDI_ERROR_TRANSACTION_COMMIT_FAIL;
  }

  return GDI_SUCCESS;
}


int GDI_StartCollectiveTransaction( GDI_Database graph_db, GDI_Transaction* transaction ) {
  /**
    don't check for GDI_ERROR_NOT_SAME
   */

  /**
    check the input arguments
   */
  if( transaction == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  if( graph_db == GDI_DATABASE_NULL ) {
    return GDI_ERROR_DATABASE;
  }

  /**
    check for other transactions
   */
  if( graph_db->transactions->head != NULL ) {
    /**
      other transactions are still active on this process
     */
    return GDI_ERROR_INCOMPATIBLE_TRANSACTIONS;
  }

  /**
    wait for all other process to reach this point
   */
  MPI_Barrier( graph_db->comm );

  /**
    passed all input checks, so it is safe to create the output buffer
   */
  *transaction = malloc( sizeof(GDI_Transaction_desc_t) );
  assert( *transaction != NULL );

  /**
    init data structure
   */
  (*transaction)->db = graph_db;
  (*transaction)->type = GDI_COLLECTIVE_TRANSACTION;
  (*transaction)->write_flag = false /* read only */;
  (*transaction)->critical_flag = false /* no problems */;
  /**
    init vectors that will keep track of all vertex and edge objects
   */
  GDA_vector_create( &((*transaction)->vertices), sizeof(GDI_VertexHolder), 8 /* initial capacity */ );
  GDA_vector_create( &((*transaction)->edges), sizeof(GDI_EdgeHolder), 8 /* initial capacity */ );
  /**
    init hash map that will translate vertex UIDs to vertex handles,
    in case a vertex is already associated with this transaction
   */
  GDA_hashmap_create( &((*transaction)->v_translate_d2l), sizeof(GDA_DPointer) /* key size */, 32 /* capacity */, sizeof(void*) /* value_size */, &GDA_int64_to_int );

  /**
    add current transaction to the list of transactions
    of the graph database
   */
  (*transaction)->db_listptr = GDA_list_push_back( graph_db->transactions, transaction );
  graph_db->collective_flag = true;

  return GDI_SUCCESS;
}


int GDI_CloseCollectiveTransaction( GDI_Transaction* transaction, int ctype ) {
  /**
    check the input arguments
   */
  if( transaction == NULL ) {
    return GDI_ERROR_TRANSACTION;
  }
  if( *transaction == GDI_TRANSACTION_NULL ) {
    return GDI_ERROR_TRANSACTION;
  }

  if( (ctype != GDI_TRANSACTION_COMMIT) && (ctype != GDI_TRANSACTION_ABORT) ) {
    return GDI_ERROR_STATE;
  }

  if( (*transaction)->type != GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_WRONG_TYPE;
  }

  /**
    passed all input checks
   */

  assert( !((*transaction)->critical_flag) );

  // TODO: invalidate all vertex and edge objects

  /**
    remove transaction from the list of currently active transactions
   */
  GDA_list_erase_single( (*transaction)->db->transactions, (*transaction)->db_listptr );
  (*transaction)->db->collective_flag = false;

  /**
    free all GDI_VertexHolder objects
   */
  size_t vec_size = (*transaction)->vertices->size;
  for( size_t i=0 ; i<vec_size ; i++ ) {
    // TODO: might be additional work necessary
    GDI_VertexHolder vertex = *(GDI_VertexHolder*)GDA_vector_at( (*transaction)->vertices, i );
    free( vertex->property_data );
    free( vertex->lightweight_edge_data );
    GDA_list_free( &(vertex->edges) );
    GDA_vector_free( &(vertex->blocks) );
    free( vertex );
  }
  GDA_vector_free( &((*transaction)->vertices) );

  /**
    free all GDI_EdgeHolder objects
   */
  vec_size = (*transaction)->edges->size;
  for( size_t i=0 ; i<vec_size ; i++ ) {
    // TODO: might be additional work necessary
    free( *(GDI_EdgeHolder*)GDA_vector_at( (*transaction)->edges, i ) );
  }
  GDA_vector_free( &((*transaction)->edges) );

  /**
    free the vertex translation hash map
   */
  GDA_hashmap_free( &((*transaction)->v_translate_d2l) );

  GDI_Database db = (*transaction)->db;

  free( *transaction );
  *transaction = GDI_TRANSACTION_NULL;

  /**
    use an allreduce as a barrier

    only have to do one barrier, since collective transactions are read-only
   */
  uint32_t myelem, sum;
  if( ctype == GDI_TRANSACTION_COMMIT ) {
    myelem = 1;
  } else {
    myelem = 0;
  }
  MPI_Allreduce( &myelem, &sum, 1, MPI_UINT32_T, MPI_SUM, db->comm );

  if( sum != db->commsize ) {
    /**
      not every process wanted to commit
     */
    if( ctype == GDI_TRANSACTION_COMMIT ) {
      return GDI_ERROR_TRANSACTION_COMMIT_FAIL;
    }
  }

  return GDI_SUCCESS;
}


int GDI_GetAllTransactionsOfDatabase( GDI_Transaction array_of_transactions[], size_t count, size_t* resultcount, GDI_Database graph_db ) {
  /**
    check the input arguments
   */
  if( graph_db == GDI_DATABASE_NULL ) {
    return GDI_ERROR_DATABASE;
  }

  if( resultcount == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  /**
    passed all input checks
   */

  if( (array_of_transactions == NULL) || (count == 0) ) {
    /**
      size trick requested: only return the number of currently active transaction in this database in resultcount
     */
    *resultcount = GDA_list_size( graph_db->transactions );
  } else {
    *resultcount = GDA_list_to_array( graph_db->transactions, array_of_transactions, count );

    if( GDA_list_size( graph_db->transactions ) > (*resultcount) ) {
      return GDI_ERROR_TRUNCATE;
    }
  }

  return GDI_SUCCESS;
}


int GDI_GetTypeOfTransaction( int* ttype, GDI_Transaction transaction ) {
  /**
    check the input arguments
   */
  if( transaction == GDI_TRANSACTION_NULL ) {
    return GDI_ERROR_TRANSACTION;
  }

  if( ttype == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  /**
    passed all input checks
   */

  *ttype = transaction->type;

  return GDI_SUCCESS;
}
