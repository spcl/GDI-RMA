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
#include "gda_constraint.h"
#include "gda_dpointer.h"
#include "gda_edge_uid.h"
#include "gda_lock.h"
#include "gda_lightweight_edges.h"
#include "gda_property.h"
#include "gda_vertex.h"

int GDI_CreateVertex( const void* external_id, size_t size, GDI_Transaction transaction, GDI_VertexHolder* vertex ) {
  /**
    check the input arguments
   */
  if( vertex == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  if( transaction == GDI_TRANSACTION_NULL ) {
    return GDI_ERROR_TRANSACTION;
  }

  if( transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  /**
    allocate the primary block

    do this here, so that we can return an error, before the vertex object is created
   */
  GDA_DPointer primary_block = GDA_AllocateBlock( transaction->db->commrank, transaction->db );
  if( primary_block == GDA_DPOINTER_NULL ) {
    return GDI_ERROR_NO_MEMORY;
  }

  /**
    passed all checks, so it is safe to create the output buffer
   */
  *vertex = malloc( sizeof(GDI_VertexHolder_desc_t) );
  assert( *vertex != NULL );

  (*vertex)->transaction = transaction;
  (*vertex)->delete_flag = false;
  (*vertex)->write_flag = true;
  (*vertex)->creation_flag = true;
  transaction->write_flag = true;

  GDA_vector_push_back( transaction->vertices, vertex );

  /**
    create the list that keeps track of all edge objects, that are associated
    with the current vertex
   */
  GDA_list_create( &((*vertex)->edges), sizeof(GDI_EdgeHolder) /* element size */ );

  GDA_LinearScanningInitPropertyList( *vertex );
  GDA_LightweightEdgesInit( *vertex );

  if( (size > 0) && (external_id != NULL ) ) {
    GDA_LinearScanningAddProperty( GDI_PROPERTY_TYPE_ID, external_id, size, *vertex );

    // TODO: index update for external ID
  }

  /**
    initialise the data structure that keeps track of blocks associated
    with this vertex and add the primary block to that data structure
   */
  GDA_vector_create( &((*vertex)->blocks), sizeof(GDA_DPointer), 8 );
  GDA_vector_push_back( (*vertex)->blocks, &primary_block );

  /**
    set write lock bit: only process with access to this block
   */
  GDA_SetVertexWriteLock( *vertex );

  /**
    insert the new vertex into the hash map that translates from the
    vertex UID to the local address of the GDI_VertexHolder object
   */
  GDA_hashmap_insert( transaction->v_translate_d2l, &primary_block /* key */, vertex /* value */ );

  return GDI_SUCCESS;
}


int GDI_AssociateVertex( GDI_Vertex_uid internal_uid, GDI_Transaction transaction, GDI_VertexHolder* vertex ) {
  /**
    check the input arguments
   */
  if( vertex == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  if( transaction == GDI_TRANSACTION_NULL ) {
    return GDI_ERROR_TRANSACTION;
  }

  /**
    do a couple of sanity checks with the vertex uid
    the same checks are performed during GDA_GetBlock,
    however only in DEBUG builds (and with asserts)
   */
  uint64_t offset, target_rank;
  if( internal_uid == 0xFFFFFFFFFFFFFFFF ) {
    return GDI_ERROR_UID;
  }
  GDA_GetDPointer( &offset, &target_rank, internal_uid );

  if( (offset % transaction->db->block_size != 0) || (offset >= (uint64_t)(transaction->db->win_blocks_size)) || (target_rank >= transaction->db->commsize) ) {
    return GDI_ERROR_UID;
  }

  /**
    TODO: can't check for GDI_ERROR_OBJECT_MISMATCH
   */

  /**
    check whether vertex is already associated with this transaction
   */
  GDI_VertexHolder* v_hashmap = GDA_hashmap_get( transaction->v_translate_d2l, &internal_uid );
  if( v_hashmap != NULL ) {
    /**
      vertex is already associated with this transaction
     */
    if( (*v_hashmap)->delete_flag ) {
      return GDI_ERROR_VERTEX;
    }
    *vertex = *v_hashmap;
    return GDI_SUCCESS;
  }

  /**
    passed all checks and the vertex is not associated with this
    transaction, so it is safe to create the output buffer
   */
  *vertex = malloc( sizeof(GDI_VertexHolder_desc_t) );
  assert( *vertex != NULL );

  (*vertex)->transaction = transaction;
  (*vertex)->lock_type = GDA_NO_LOCK;

  /**
    TODO: Workaround: set up (*vertex)->blocks->data before GDA_AcquireVertexReadLock is called. The initialization of
    (*vertex)->blocks and its fields is finished later.
   */
  (*vertex)->blocks = malloc( sizeof( GDA_Vector ) );
  (*vertex)->blocks->data = malloc( sizeof( GDA_DPointer ) );
  *(uint64_t*)( (*vertex)->blocks->data ) = internal_uid;

  /**
    acquire a read lock if we are in a single process transaction
   */
  if( transaction->type == GDI_SINGLE_PROCESS_TRANSACTION ) {
    GDA_AcquireVertexReadLock( *vertex );
    if( (*vertex)->lock_type == GDA_NO_LOCK ) {
      /**
        acquisition of a read lock failed, so free the allocated memory and return an error
       */
      free( (*vertex)->blocks->data );
      free( (*vertex)->blocks );
      free( *vertex );
      transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  GDA_AssociateVertex( internal_uid, transaction, *vertex );

  return GDI_SUCCESS;
}


int GDI_FreeVertex( GDI_VertexHolder* vertex ) {
  /**
    check the input arguments
   */
  if( vertex == NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( *vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( (*vertex)->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( (*vertex)->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  /**
    write lock is required to make any changes in VertexHolder
   */
  if( (*vertex)->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( *vertex );
    if( (*vertex)->lock_type == GDA_READ_LOCK ) {
      /**
        acquiring a write lock failed
       */
      (*vertex)->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    passed all input checks
   */
  (*vertex)->delete_flag = true;
  (*vertex)->write_flag = true;
  (*vertex)->transaction->write_flag = true;

  /**
    mark all edge objects that are associated with the current vertex
    for deletion
   */
  GDA_Node* edge_node = (*vertex)->edges->head;
  while( edge_node != NULL ) {
    GDI_EdgeHolder edge = *(GDI_EdgeHolder*)(edge_node->value);
    edge->delete_flag = true;
    // TODO: does it make sense to remove the edge from the edge object
    // list of the other vertex?
    edge_node = edge_node->next;
  }

  /**
    delete edges:
    iterate over all edges from this vertex and associate the respective vertex,
    and delete the edge there
   */
  uint8_t* metadata_iterator;
  uint32_t offset, max_offset;
  GDI_VertexHolder other_vertex;
  int status;

  offset = 2;
  max_offset = (*vertex)->lightweight_edge_insert_offset;
  metadata_iterator = (uint8_t*) (*vertex)->lightweight_edge_data;

  /**
    main loop over the edges
   */
  while( offset < max_offset ) {
    if( *metadata_iterator != 0 ) {
      /* edge is not deleted */
      if( (*vertex)->lightweight_edge_data[offset] != *(GDA_DPointer*)((*vertex)->blocks->data) ) {
        /* not the same vertex/no self-edge */
        status = GDI_AssociateVertex( (*vertex)->lightweight_edge_data[offset], (*vertex)->transaction, &other_vertex );
        if( status != GDI_SUCCESS ) {
          (*vertex)->transaction->critical_flag = true;
          return GDI_ERROR_TRANSACTION_CRITICAL;
        }

        /**
          acquire write lock if necessary
         */
        if( other_vertex->lock_type == GDA_READ_LOCK ) {
          GDA_UpdateToVertexWriteLock( other_vertex );
          if( other_vertex->lock_type == GDA_READ_LOCK ) {
            (*vertex)->transaction->critical_flag = true;
            return GDI_ERROR_TRANSACTION_CRITICAL;
          }
        }

        uint32_t other_offset = GDA_LightweightEdgesFindEdge( *metadata_iterator + 256, *(GDA_DPointer*)((*vertex)->blocks->data), *(metadata_iterator+8), other_vertex );
        assert( other_offset != 0 );

        bool del_flag;
        GDA_LightweightEdgesRemove( other_offset, other_vertex, &del_flag );
        assert( del_flag );

        other_vertex->write_flag = true;
      }
    }

    offset++;
    metadata_iterator++;
    if( offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE == 0 ) {
      /**
        go to the next block
       */
      offset += 2;
      metadata_iterator += GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES - sizeof(GDA_DPointer);
    }
  }

  /**
    don't delete the vertex object right now, delay this until the
    transaction is closed
   */
  *vertex = GDI_VERTEX_NULL;

  return GDI_SUCCESS;
}


/**
  function written by Roman Haag
 */
int GDI_GetEdgesOfVertex( GDI_Edge_uid array_of_uids[], size_t count, size_t* resultcount, GDI_Constraint constraint, int edge_orientation, GDI_VertexHolder vertex ) {
   /**
     check the input arguments
    */
  if( vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( vertex->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( resultcount == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  if( (constraint != NULL) && (constraint->stale == GDA_CONSTRAINT_STALE) ) {
    return GDI_ERROR_STALE;
  }

  uint8_t orientation = edge_orientation;

  if( (orientation == 0) || (orientation > 7) ) {
    return GDI_ERROR_EDGE_ORIENTATION;
  }

  /**
    constraint evaluation
   */
  uint8_t* list;
  size_t list_size = 0;
  bool list_flag; /* indicates whether the list is a whitelist (true) or a blacklist (false); only significant if list_size > 0 */
  if( constraint != NULL ) {
    int ret;
    ret = GDA_EvalConstraintInLightweightEdgeContext( &list, &list_size, &list_flag, constraint );
    if( ret == GDI_ERROR_CONSTRAINT ) {
      return GDI_ERROR_CONSTRAINT;
    }
  }

  /**
    passed all input checks
   */

  if( (array_of_uids == NULL) || (count == 0) ) {
    if( list_size == 0 ) {
      GDA_LightweightEdgesNumEdges( resultcount, edge_orientation, vertex );
    } else {
      if( list_flag ) {
        GDA_LightweightEdgesNumEdgesWithLabelWhitelist( resultcount, edge_orientation, list, list_size, vertex );
      } else {
        GDA_LightweightEdgesNumEdgesWithLabelBlacklist( resultcount, edge_orientation, list, list_size, vertex );
      }

      free( list );
    }

    return GDI_SUCCESS;
  }

  uint32_t *array_of_offsets, offset;
  GDA_DPointer vertex_uid;
  int status;
  vertex_uid = *(GDA_DPointer*) vertex->blocks->data;
  array_of_offsets = malloc( count * sizeof(uint32_t) );

  if( list_size == 0 ) {
    status = GDA_LightweightEdgesFilterEdges( array_of_offsets, count, resultcount, edge_orientation, vertex );
  } else {
    if( list_flag ) {
      status = GDA_LightweightEdgesFilterEdgesWithLabelWhitelist( array_of_offsets, count, resultcount, edge_orientation, list, list_size, vertex );
    } else {
      status = GDA_LightweightEdgesFilterEdgesWithLabelBlacklist( array_of_offsets, count, resultcount, edge_orientation, list, list_size, vertex );
    }
  }

  for( size_t i=0 ; i<*resultcount ; i++ ) {
    offset = array_of_offsets[i];
    GDA_PackEdgeUid( vertex_uid, offset, array_of_uids + i );
  }

  if( list_size > 0 ) {
    free( list );
  }
  free( array_of_offsets );

  return status;
}


/**
  function written by Roman Haag
 */
int GDI_GetNeighborVerticesOfVertex( GDI_Vertex_uid array_of_uids[], size_t count, size_t* resultcount, GDI_Constraint constraint, int edge_orientation, GDI_VertexHolder vertex ) {
  /**
    check the input arguments
   */
  if( vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( vertex->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( resultcount == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  if( (constraint != NULL) && (constraint->stale == GDA_CONSTRAINT_STALE) ) {
    return GDI_ERROR_STALE;
  }

  uint8_t orientation = edge_orientation;

  if( (orientation == 0) || (orientation > 7) ) {
    return GDI_ERROR_EDGE_ORIENTATION;
  }

  /**
    constraint evalulation
   */
  uint8_t* list;
  size_t list_size = 0;
  bool list_flag; /* indicates whether the list is a whitelist (true) or a blacklist (false); only significant if list_size > 0 */
  if( constraint != NULL ) {
    int ret;
    ret = GDA_EvalConstraintInLightweightEdgeContext( &list, &list_size, &list_flag, constraint );
    if( ret == GDI_ERROR_CONSTRAINT ) {
      return GDI_ERROR_CONSTRAINT;
    }
  }

  /**
    passed all input checks
   */

  if( (array_of_uids == NULL) || (count == 0) ) {
    if( list_size == 0 ) {
      GDA_LightweightEdgesNumEdges( resultcount, edge_orientation, vertex );
    } else {
      if( list_flag ) {
        GDA_LightweightEdgesNumEdgesWithLabelWhitelist( resultcount, edge_orientation, list, list_size, vertex );
      } else {
        GDA_LightweightEdgesNumEdgesWithLabelBlacklist( resultcount, edge_orientation, list, list_size, vertex );
      }

      free( list );
    }

    return GDI_SUCCESS;
  }

  uint32_t *array_of_offsets, offset;
  int status;
  array_of_offsets = malloc( count * sizeof(uint32_t) );

  if( list_size == 0 ) {
    status = GDA_LightweightEdgesFilterEdges( array_of_offsets, count, resultcount, edge_orientation, vertex );
  } else {
    if( list_flag ) {
      status = GDA_LightweightEdgesFilterEdgesWithLabelWhitelist( array_of_offsets, count, resultcount, edge_orientation, list, list_size, vertex );
    } else {
      status = GDA_LightweightEdgesFilterEdgesWithLabelBlacklist( array_of_offsets, count, resultcount, edge_orientation, list, list_size, vertex );
    }
  }

  for( size_t i=0 ; i<*resultcount ; i++ ) {
    offset = array_of_offsets[i];
    array_of_uids[i] = vertex->lightweight_edge_data[offset];
  }

  if( list_size > 0 ) {
    free( list );
  }
  free( array_of_offsets );

  return status;
}


int GDI_AddLabelToVertex( GDI_Label label, GDI_VertexHolder vertex ) {
  /**
    check the input arguments
   */
  if( vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( vertex->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( label == GDI_LABEL_NULL ) {
    return GDI_ERROR_LABEL;
  }

  if( vertex->transaction->db != label->db ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  if( vertex->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  // TODO: check for GDI_ERROR_NON_UNIQUE_ID

  /**
    passed all input checks
   */

  /**
    try to update to a write lock
   */
  if( vertex->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( vertex );
    if( vertex->lock_type == GDA_READ_LOCK ) {
      vertex->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  bool found_flag;

  GDA_LinearScanningInsertLabel( label, vertex, &found_flag );

  if( !found_flag ) {
    /**
      label was not already present
     */
    vertex->write_flag = true;
    vertex->transaction->write_flag = true;
    // update the index (maybe)
  }

  return GDI_SUCCESS;
}


int GDI_RemoveLabelFromVertex( GDI_Label label, GDI_VertexHolder vertex ) {
  /**
    check the input arguments
   */
  if( vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( vertex->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( label == GDI_LABEL_NULL ) {
    return GDI_ERROR_LABEL;
  }

  if( vertex->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  if( vertex->transaction->db != label->db ) {
    /**
      label is from a different graph database, so the vertex can't
      have that label -> we are already done
     */
    return GDI_SUCCESS;
  }

  // TODO: check for GDI_ERROR_NON_UNIQUE_ID
  // maybe have to do this twice: once here and once during the commit

  /**
    passed all input checks
   */

  /**
    try to update to a write lock
   */
  if( vertex->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( vertex );
    if( vertex->lock_type == GDA_READ_LOCK ) {
      vertex->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  bool found_flag;

  GDA_LinearScanningRemoveLabel( label, vertex, &found_flag );
  if( found_flag ) {
    vertex->write_flag = true;
    vertex->transaction->write_flag = true;
    // TODO: update the index (maybe)
  }

  return GDI_SUCCESS;
}


int GDI_GetAllLabelsOfVertex( GDI_Label array_of_labels[], size_t count, size_t* resultcount, GDI_VertexHolder vertex ) {
  /**
    check the input arguments
   */
  if( vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( vertex->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( resultcount == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  /**
    passed all input checks
   */

  if( (array_of_labels == NULL) || (count == 0) ) {
    /**
      size trick requested: only return the number of labels present on this vertex in resultcount
     */
    GDA_LinearScanningNumLabels( vertex, resultcount );
  } else {
    return GDA_LinearScanningFindAllLabels( vertex, array_of_labels, count, resultcount );
  }

  return GDI_SUCCESS;
}


int GDI_AddPropertyToVertex( const void* value, size_t count, GDI_PropertyType ptype, GDI_VertexHolder vertex ) {
  /**
    check the input arguments
   */
  if( vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( vertex->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( ptype == GDI_PROPERTY_TYPE_NULL ) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  /**
    handle the read-only property types differently, since we might
    want to treat them unlike normal property types, like generating
    them on-the-fly
   */
  if( (ptype == GDI_PROPERTY_TYPE_DEGREE) || (ptype == GDI_PROPERTY_TYPE_INDEGREE) || (ptype == GDI_PROPERTY_TYPE_OUTDEGREE) ) {
    return GDI_ERROR_PROPERTY_TYPE_EXISTS;
  }

  if( (count > 0) && (value == NULL) ) {
    return GDI_ERROR_BUFFER;
  }

  if( vertex->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  /**
    already took care of the other predefined property types earlier,
    so that's the only property type left, that has to be handled differently
   */
  if( (ptype != GDI_PROPERTY_TYPE_ID) && (vertex->transaction->db != ptype->db) ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  if( ptype->stype == GDI_FIXED_SIZE ) {
    if( count != ptype->count ) {
      return GDI_ERROR_SIZE_LIMIT;
    }
  } else {
    if( ptype->stype == GDI_MAX_SIZE ) {
      if( count > ptype->count ) {
        return GDI_ERROR_SIZE_LIMIT;
      }
    }
  }

  /**
    try to update to a write lock
   */
  if( vertex->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( vertex );
    if( vertex->lock_type == GDA_READ_LOCK ) {
      vertex->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  int ret = GDA_LinearScanningAddProperty( ptype, value, count, vertex );

  if( ret == GDI_SUCCESS ) {
    /**
      a new property was inserted into the list
     */
    vertex->write_flag = true;
    vertex->transaction->write_flag = true;
    // TODO: probably have to handle GDI_PROPERTY_TYPE_ID differently
    // TODO: index update (maybe)
  } else {
    if( ret == GDI_ERROR_PROPERTY_EXISTS ) {
      /**
        don't treat this as an error on the GDI level

        it is viewed as no action performed, so no index update necessary
       */
      ret = GDI_SUCCESS;
    }
  }

  return ret;
}


int GDI_GetAllPropertyTypesOfVertex( GDI_PropertyType array_of_ptypes[], size_t count, size_t* resultcount, GDI_VertexHolder vertex ) {
  /**
    check the input arguments
   */
  if( vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( vertex->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( resultcount == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  if( (array_of_ptypes == NULL) || (count == 0) ) {
    /**
      size trick requested: only return in resultcount the number of property types that have properties present on this vertex
     */
    GDA_LinearScanningNumPropertyTypes( vertex, resultcount );
  } else {
    return GDA_LinearScanningFindAllPropertyTypes( vertex, array_of_ptypes, count, resultcount );
  }

  return GDI_SUCCESS;
}


int GDI_GetPropertiesOfVertex( void* buf, size_t buf_count, size_t* buf_resultcount, size_t array_of_offsets[], size_t offset_count,
  size_t* offset_resultcount, GDI_PropertyType ptype, GDI_VertexHolder vertex ) {
  /**
    check the input arguments
   */
  if( vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( vertex->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( ptype == GDI_PROPERTY_TYPE_NULL ) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if( (buf_resultcount == NULL) || (offset_resultcount == NULL) ) {
    return GDI_ERROR_BUFFER;
  }

  /**
    handle the read-only property types differently, since we might
    want to treat them unlike normal property types, like generating
    them on-the-fly
   */
  if( (ptype == GDI_PROPERTY_TYPE_DEGREE) || (ptype == GDI_PROPERTY_TYPE_INDEGREE) || (ptype == GDI_PROPERTY_TYPE_OUTDEGREE) ) {
    int ret = GDI_SUCCESS;
    *buf_resultcount = 1;
    *offset_resultcount = 2;
    if( !((buf == NULL) || (array_of_offsets == NULL) || (buf_count == 0) || (offset_count == 0)) ) {
      array_of_offsets[0] = 0;
      if( offset_count < 2 ) {
        *offset_resultcount = 1;
        ret = GDI_ERROR_TRUNCATE;
      } else {
        array_of_offsets[1] = 1;
      }
      size_t num_edges;
      if( ptype == GDI_PROPERTY_TYPE_DEGREE ) {
        GDA_LightweightEdgesNumEdges( &num_edges, (GDI_EDGE_INCOMING | GDI_EDGE_OUTGOING | GDI_EDGE_UNDIRECTED), vertex );
      } else {
        if( ptype == GDI_PROPERTY_TYPE_INDEGREE ) {
          GDA_LightweightEdgesNumEdges( &num_edges, GDI_EDGE_INCOMING, vertex );
        } else {
          /**
            ptype == GDI_PROPERTY_TYPE_OUTDEGREE
           */
          GDA_LightweightEdgesNumEdges( &num_edges, GDI_EDGE_OUTGOING, vertex );
        }
      }
      *(uint64_t*) buf = num_edges;
    }

    return ret;
  }

  /**
    already took care of the other predefined property types earlier,
    so that's the only property type left, that has to be handled differently
   */
  if( (ptype != GDI_PROPERTY_TYPE_ID) && (vertex->transaction->db != ptype->db) ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  if( (buf == NULL) || (array_of_offsets == NULL) || (buf_count == 0) || (offset_count == 0) ) {
    /**
      size trick requested: only return the number of elements in buf_resultcount
      and the number of offsets in offset_resultcount
     */
    GDA_LinearScanningNumProperties( vertex, ptype, offset_resultcount, buf_resultcount );
    if( *offset_resultcount > 0 ) {
      /**
        GDA_LinearScanningNumProperties only returns the number of properties of that property type,
        so the number of offsets is one more
       */
      (*offset_resultcount)++;
    }
  } else {
    return GDA_LinearScanningFindAllProperties( buf, buf_count, buf_resultcount, array_of_offsets, offset_count, offset_resultcount, ptype, vertex );
  }

  return GDI_SUCCESS;
}


int GDI_RemovePropertiesFromVertex( GDI_PropertyType ptype, GDI_VertexHolder vertex ) {
  /**
    check the input arguments
   */
  if( vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( vertex->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( ptype == GDI_PROPERTY_TYPE_NULL ) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if( vertex->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  if( (ptype == GDI_PROPERTY_TYPE_DEGREE) || (ptype == GDI_PROPERTY_TYPE_INDEGREE) || (ptype == GDI_PROPERTY_TYPE_OUTDEGREE) ) {
    return GDI_ERROR_READ_ONLY_PROPERTY_TYPE;
  }

  if( (ptype != GDI_PROPERTY_TYPE_ID) && (vertex->transaction->db != ptype->db) ) {
    /**
      property type is from a different graph database, so the vertex can't
      have a property of that type -> we are already done
     */
    return GDI_SUCCESS;
  }

  /**
    passed all input checks
   */

  /**
    try to update to a write lock
   */
  if( vertex->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( vertex );
    if( vertex->lock_type == GDA_READ_LOCK ) {
      vertex->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  bool found_flag;

  GDA_LinearScanningRemoveProperties( ptype, vertex, &found_flag );
  if( found_flag ) {
    vertex->write_flag = true;
    vertex->transaction->write_flag = true;
    // TODO: update the index (maybe)
    // TODO: probably have to handle GDI_PROPERTY_TYPE_ID differently
  }

  return GDI_SUCCESS;
}


int GDI_RemoveSpecificPropertyFromVertex( const void* value, size_t count, GDI_PropertyType ptype, GDI_VertexHolder vertex ) {
  /**
    check the input arguments
   */
  if( vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( vertex->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( ptype == GDI_PROPERTY_TYPE_NULL ) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if( vertex->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  if( (ptype == GDI_PROPERTY_TYPE_DEGREE) || (ptype == GDI_PROPERTY_TYPE_INDEGREE) || (ptype == GDI_PROPERTY_TYPE_OUTDEGREE) ) {
    return GDI_ERROR_READ_ONLY_PROPERTY_TYPE;
  }

  if( (count > 0) && (value == NULL) ) {
    return GDI_ERROR_BUFFER;
  }

  if( (ptype != GDI_PROPERTY_TYPE_ID) && (vertex->transaction->db != ptype->db) ) {
    /**
      property type is from a different graph database, so the vertex can't
      have a property of that type -> we are already done
     */
    return GDI_SUCCESS;
  }

  if( ptype->stype == GDI_FIXED_SIZE ) {
    if( count != ptype->count ) {
      return GDI_ERROR_SIZE_LIMIT;
    }
  } else {
    if( ptype->stype == GDI_MAX_SIZE ) {
      if( count > ptype->count ) {
        return GDI_ERROR_SIZE_LIMIT;
      }
    }
  }

  /**
    passed all input checks
   */

  /**
    try to update to a write lock
   */
  if( vertex->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( vertex );
    if( vertex->lock_type == GDA_READ_LOCK ) {
      vertex->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  bool found_flag;

  GDA_LinearScanningRemoveSpecificProperty( ptype, value, count, vertex, &found_flag );
  if( found_flag ) {
    vertex->write_flag = true;
    vertex->transaction->write_flag = true;
    // TODO: update the index (maybe)
    // TODO: probably have to handle GDI_PROPERTY_TYPE_ID differently
  }

  return GDI_SUCCESS;
}


int GDI_UpdatePropertyOfVertex( const void* value, size_t count, GDI_PropertyType ptype, GDI_VertexHolder vertex ) {
  /**
    check the input arguments
   */
  if( vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( vertex->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( ptype == GDI_PROPERTY_TYPE_NULL ) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if( vertex->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  if( (ptype == GDI_PROPERTY_TYPE_DEGREE) || (ptype == GDI_PROPERTY_TYPE_INDEGREE) || (ptype == GDI_PROPERTY_TYPE_OUTDEGREE) ) {
    return GDI_ERROR_READ_ONLY_PROPERTY_TYPE;
  }

  if( (count > 0) && (value == NULL) ) {
    return GDI_ERROR_BUFFER;
  }

  if( (ptype != GDI_PROPERTY_TYPE_ID) && (vertex->transaction->db != ptype->db) ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  if( ptype->etype == GDI_MULTIPLE_ENTITY ) {
    return GDI_ERROR_WRONG_TYPE;
  }

  if( ptype->stype == GDI_FIXED_SIZE ) {
    if( count != ptype->count ) {
      return GDI_ERROR_SIZE_LIMIT;
    }
  } else {
    if( ptype->stype == GDI_MAX_SIZE ) {
      if( count > ptype->count ) {
        return GDI_ERROR_SIZE_LIMIT;
      }
    }
  }

  /**
    passed all input checks
   */

  /**
    try to update to a write lock
   */
  if( vertex->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( vertex );
    if( vertex->lock_type == GDA_READ_LOCK ) {
      vertex->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  int ret = GDA_LinearScanningUpdateSingleEntityProperty( ptype, value, count, vertex );

  if( ret == GDI_SUCCESS ) {
    vertex->write_flag = true;
    vertex->transaction->write_flag = true;
    // TODO: update the index (maybe)
    // TODO: probably have to handle GDI_PROPERTY_TYPE_ID differently
  }

  return ret;
}


int GDI_UpdateSpecificPropertyOfVertex( const void* old_value, size_t old_count, const void* new_value, size_t new_count, GDI_PropertyType ptype, GDI_VertexHolder vertex ) {
  /**
    check the input arguments
   */
  if( vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( vertex->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( ptype == GDI_PROPERTY_TYPE_NULL ) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if( vertex->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  if( (ptype == GDI_PROPERTY_TYPE_DEGREE) || (ptype == GDI_PROPERTY_TYPE_INDEGREE) || (ptype == GDI_PROPERTY_TYPE_OUTDEGREE) ) {
    return GDI_ERROR_READ_ONLY_PROPERTY_TYPE;
  }

  if( ((old_count > 0) && (old_value == NULL)) || ((new_count > 0) && (new_value == NULL)) ) {
    return GDI_ERROR_BUFFER;
  }

  if( (ptype != GDI_PROPERTY_TYPE_ID) && (vertex->transaction->db != ptype->db) ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  if( ptype->stype == GDI_FIXED_SIZE ) {
    if( (old_count != ptype->count) || (new_count != ptype->count) ) {
      return GDI_ERROR_SIZE_LIMIT;
    }
  } else {
    if( ptype->stype == GDI_MAX_SIZE ) {
      if( (old_count > ptype->count) || (new_count > ptype->count) ) {
        return GDI_ERROR_SIZE_LIMIT;
      }
    }
  }

  /**
    passed all input checks
   */

  /**
    try to update to a write lock
   */
  if( vertex->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( vertex );
    if( vertex->lock_type == GDA_READ_LOCK ) {
      vertex->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  int ret = GDA_LinearScanningUpdateSpecificProperty( ptype, old_value, old_count, new_value, new_count, vertex );

  if( ret == GDI_SUCCESS ) {
    vertex->write_flag = true;
    vertex->transaction->write_flag = true;
    // TODO: update the index (maybe)
    // TODO: probably have to handle GDI_PROPERTY_TYPE_ID differently
  }

  return ret;
}


int GDI_SetPropertyOfVertex( const void* value, size_t count, GDI_PropertyType ptype, GDI_VertexHolder vertex ) {
  /**
    check the input arguments
   */
  if( vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( vertex->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( ptype == GDI_PROPERTY_TYPE_NULL ) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if( vertex->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  if( (ptype == GDI_PROPERTY_TYPE_DEGREE) || (ptype == GDI_PROPERTY_TYPE_INDEGREE) || (ptype == GDI_PROPERTY_TYPE_OUTDEGREE) ) {
    return GDI_ERROR_READ_ONLY_PROPERTY_TYPE;
  }

  if( (count > 0) && (value == NULL) ) {
    return GDI_ERROR_BUFFER;
  }

  if( (ptype != GDI_PROPERTY_TYPE_ID) && (vertex->transaction->db != ptype->db) ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  if( ptype->etype == GDI_MULTIPLE_ENTITY ) {
    return GDI_ERROR_WRONG_TYPE;
  }

  if( ptype->stype == GDI_FIXED_SIZE ) {
    if( count != ptype->count ) {
      return GDI_ERROR_SIZE_LIMIT;
    }
  } else {
    if( ptype->stype == GDI_MAX_SIZE ) {
      if( count > ptype->count ) {
        return GDI_ERROR_SIZE_LIMIT;
      }
    }
  }

  /**
    passed all input checks
   */

  /**
    try to update to a write lock
   */
  if( vertex->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( vertex );
    if( vertex->lock_type == GDA_READ_LOCK ) {
      vertex->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  bool found_flag;

  GDA_LinearScanningSetSingleEntityProperty( ptype, value, count, vertex, &found_flag );

  vertex->write_flag = true;
  vertex->transaction->write_flag = true;

#if 0
  if( found_flag ) {
    /**
      found an existing property, removed that one and added a new one
     */
    // TODO: update the index (maybe)
    // TODO: probably have to handle GDI_PROPERTY_TYPE_ID differently
  }
#endif

  return GDI_SUCCESS;
}
