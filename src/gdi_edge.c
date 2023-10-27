// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#include <assert.h>

#include "gdi.h"
#include "gda_lightweight_edges.h"
#include "gda_lock.h"

int GDI_CreateEdge( int dtype, GDI_VertexHolder origin, GDI_VertexHolder target, GDI_EdgeHolder* edge ) {
  /**
    check the input arguments
   */
  if( edge == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  if( (origin == GDI_VERTEX_NULL) || (target == GDI_VERTEX_NULL) ) {
    return GDI_ERROR_VERTEX;
  }

  if( (origin->delete_flag) || (target->delete_flag) ) {
    return GDI_ERROR_VERTEX;
  }

  if( origin->transaction != target->transaction ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  if( origin->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  if( (dtype != GDI_EDGE_DIRECTED) && (dtype != GDI_EDGE_UNDIRECTED) ) {
    return GDI_ERROR_STATE;
  }

  /**
    try to update to a write lock (origin)
   */
  if( origin->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( origin );
    if( origin->lock_type == GDA_READ_LOCK ) {
      origin->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    try to update to a write lock (target)
   */
  if( target->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( target );
    if( target->lock_type == GDA_READ_LOCK ) {
      origin->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    passed all input checks and acquired write locks on the vertices, so it is safe to create the output buffer
   */
  *edge = malloc( sizeof(GDI_EdgeHolder_desc_t) );
  assert( edge != NULL );

  (*edge)->origin = origin;
  (*edge)->target = target;
  (*edge)->transaction = origin->transaction;
  (*edge)->delete_flag = false;
  (*edge)->write_flag = true;
  origin->write_flag = true;
  target->write_flag = true;
  origin->transaction->write_flag = true;

  /**
    add the edge as an element to edge lists of the two vertices
   */
  (*edge)->origin_elist_ptr = GDA_list_push_back( origin->edges, edge );
  (*edge)->target_elist_ptr = GDA_list_push_back( target->edges, edge );

  GDA_vector_push_back( origin->transaction->edges, edge );

  /**
    set the correct direction constants
   */
  int origin_direction, target_direction;

  if( dtype == GDI_EDGE_UNDIRECTED ){
    origin_direction = GDI_EDGE_UNDIRECTED;
    target_direction = GDI_EDGE_UNDIRECTED;
  } else {
    origin_direction = GDI_EDGE_OUTGOING;
    target_direction = GDI_EDGE_INCOMING;
  }

  /**
    add the edge in the lightweight edge data structure of both vertices
   */
  GDA_LightweightEdgesAddEdge( origin_direction, *(GDA_DPointer*) target->blocks->data, origin, &((*edge)->origin_lightweight_edge_offset) );
  GDA_LightweightEdgesAddEdge( target_direction, *(GDA_DPointer*) origin->blocks->data, target, &((*edge)->target_lightweight_edge_offset) );

  return GDI_SUCCESS;
}


int GDI_FreeEdge( GDI_EdgeHolder* edge ) {
  /**
    check the input arguments
   */
  if( edge == NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( *edge == GDI_EDGE_NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( (*edge)->delete_flag ) {
    return GDI_ERROR_EDGE;
  }

  if( (*edge)->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  /**
    passed all input checks
   */

  /**
    try to update to a write lock (origin)
   */
  if( (*edge)->origin->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( (*edge)->origin );
    if( (*edge)->origin->lock_type == GDA_READ_LOCK ) {
      (*edge)->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    try to update to a write lock (target)
   */
  if( (*edge)->target->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( (*edge)->target );
    if( (*edge)->target->lock_type == GDA_READ_LOCK ) {
      (*edge)->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  (*edge)->delete_flag = true;
  (*edge)->write_flag = true;
  (*edge)->origin->write_flag = true;
  (*edge)->target->write_flag = true;
  (*edge)->transaction->write_flag = true;

  /**
    don't delete the edge object right now, delay this until the
    transaction is closed
   */

  /**
    delete the edge from the lightweight edge data structure of both vertices
   */
  bool removed_flag;
  GDA_LightweightEdgesRemove( (*edge)->origin_lightweight_edge_offset, (*edge)->origin, &removed_flag );
  assert( removed_flag );
  GDA_LightweightEdgesRemove( (*edge)->target_lightweight_edge_offset, (*edge)->target, &removed_flag );
  assert( removed_flag );

  *edge = GDI_EDGE_NULL;

  return GDI_SUCCESS;
}


/**
  function written by Roman Haag
 */
int GDI_GetVerticesOfEdge( GDI_Vertex_uid* origin_uid, GDI_Vertex_uid* target_uid, GDI_EdgeHolder edge ) {
  /**
    check the input arguments
   */
  if( edge == GDI_EDGE_NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( edge->delete_flag ) {
    return GDI_ERROR_EDGE;
  }

  if( (origin_uid == NULL) || (target_uid == NULL) ) {
    return GDI_ERROR_BUFFER;
  }

  /**
    passed all input checks
   */
  *origin_uid = *(GDI_Vertex_uid*) edge->origin->blocks->data;
  *target_uid = *(GDI_Vertex_uid*) edge->target->blocks->data;

  return GDI_SUCCESS;
}


/**
  function written by Roman Haag
 */
int GDI_GetDirectionTypeOfEdge( int* dtype, GDI_EdgeHolder edge ) {
  /**
    check the input arguments
   */
  if( edge == GDI_EDGE_NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( edge->delete_flag ) {
    return GDI_ERROR_EDGE;
  }

  if( dtype == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  /**
    passed all input checks
   */
  uint8_t* edge_orientation;
  GDA_LightweightEdgesGetMetadataPointerWithOffset( &edge_orientation, edge->origin, edge->origin_lightweight_edge_offset );
  if( *edge_orientation == 4 /* GDI_EDGE_UNDIRECTED - 256 */ ) {
    *dtype = GDI_EDGE_UNDIRECTED;
  } else {
    *dtype = GDI_EDGE_DIRECTED;
  }

  return GDI_SUCCESS;
}


int GDI_SetOriginVertexOfEdge( GDI_VertexHolder origin_vertex, GDI_EdgeHolder edge ) {
  /**
    check the input arguments
   */
  if( edge == GDI_EDGE_NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( edge->delete_flag ) {
    return GDI_ERROR_EDGE;
  }

  if( origin_vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( origin_vertex->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( origin_vertex->transaction != edge->transaction ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  if( edge->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  /**
    passed all input checks
   */

  /**
    try to update to a write lock (new origin)
   */
  if( origin_vertex->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( origin_vertex );
    if( origin_vertex->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    try to update to a write lock (old origin)
   */
  if( edge->origin->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->origin );
    if( edge->origin->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    try to update to a write lock (target)
   */
  if( edge->target->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->target );
    if( edge->target->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    - delete the lightweight edge at the old origin vertex
    - update the incident vertex of the edge on the target vertex
    - add the lightweight edge to the new origin vertex,
   */
  uint8_t* edge_orientation;
  GDA_LightweightEdgesGetMetadataPointerWithOffset( &edge_orientation, edge->origin, edge->origin_lightweight_edge_offset );

  int orientation;
  if( *edge_orientation == 4 /* GDI_EDGE_UNDIRECTED - 256 */ ) {
    orientation = GDI_EDGE_UNDIRECTED;
  } else {
    orientation = GDI_EDGE_OUTGOING;
  }

  bool removed_flag;
  GDA_LightweightEdgesRemove( edge->origin_lightweight_edge_offset, edge->origin, &removed_flag );
  assert( removed_flag );
  GDA_LightweightEdgesAddEdge( orientation, *(GDA_DPointer*) edge->target->blocks->data, origin_vertex, &(edge->origin_lightweight_edge_offset) );
  GDA_LightweightEdgesSetDPointer( *(GDA_DPointer*) origin_vertex->blocks->data, edge->target_lightweight_edge_offset, edge->target );

  /**
    - set write flags of the edge, all three vertices and the transaction
    - update origin vertex of the edge
   */
  edge->write_flag = true;
  edge->origin->write_flag = true;
  edge->target->write_flag = true;
  origin_vertex->write_flag = true;
  origin_vertex->transaction->write_flag = true;
  edge->origin = origin_vertex;

  return GDI_SUCCESS;
}


int GDI_SetTargetVertexOfEdge( GDI_VertexHolder target_vertex, GDI_EdgeHolder edge ) {
  /**
    check the input arguments
   */
  if( edge == GDI_EDGE_NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( edge->delete_flag ) {
    return GDI_ERROR_EDGE;
  }

  if( target_vertex == GDI_VERTEX_NULL ) {
    return GDI_ERROR_VERTEX;
  }

  if( target_vertex->delete_flag ) {
    return GDI_ERROR_VERTEX;
  }

  if( target_vertex->transaction != edge->transaction ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  if( edge->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  /**
    passed all input checks
   */

  /**
    try to update to a write lock (new target)
   */
  if( target_vertex->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( target_vertex );
    if( target_vertex->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    try to update to a write lock (origin)
   */
  if( edge->origin->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->origin );
    if( edge->origin->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    try to update to a write lock (old target)
   */
  if( edge->target->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->target );
    if( edge->target->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    - delete the lightweight edge at the old target vertex
    - update the incident vertex of the edge on the origin vertex
    - add the lightweight edge at the new target vertex
   */
  uint8_t* edge_orientation;
  GDA_LightweightEdgesGetMetadataPointerWithOffset( &edge_orientation, edge->target, edge->target_lightweight_edge_offset );

  int orientation;
  if( *edge_orientation == 4 /* GDI_EDGE_UNDIRECTED - 256 */ ) {
    orientation = GDI_EDGE_UNDIRECTED;
  } else {
    orientation = GDI_EDGE_INCOMING;
  }

  bool removed_flag;
  GDA_LightweightEdgesRemove( edge->target_lightweight_edge_offset, edge->target, &removed_flag );
  assert( removed_flag );
  GDA_LightweightEdgesAddEdge( orientation, *(GDA_DPointer*) edge->origin->blocks->data, target_vertex, &(edge->target_lightweight_edge_offset) );
  GDA_LightweightEdgesSetDPointer( *(GDA_DPointer*) target_vertex->blocks->data, edge->origin_lightweight_edge_offset, edge->origin );

  /**
    - set write flags of the edge, all three vertices and the transaction
    - update target vertex of the edge
   */
  edge->write_flag = true;
  edge->origin->write_flag = true;
  edge->target->write_flag = true;
  target_vertex->write_flag = true;
  target_vertex->transaction->write_flag = true;
  edge->target = target_vertex;

  return GDI_SUCCESS;
}


int GDI_SetDirectionTypeOfEdge( int dtype, GDI_EdgeHolder edge ) {
  /**
    check the input arguments
   */
  if( edge == GDI_EDGE_NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( edge->delete_flag ) {
    return GDI_ERROR_EDGE;
  }

  if( (dtype != GDI_EDGE_DIRECTED) && (dtype != GDI_EDGE_UNDIRECTED) ) {
    return GDI_ERROR_STATE;
  }

  if( edge->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  /**
    passed all input checks
   */

  /**
    try to update to a write lock (origin)
   */
  if( edge->origin->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->origin );
    if( edge->origin->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    try to update to a write lock (target)
   */
  if( edge->target->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->target );
    if( edge->target->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  uint32_t origin_orientation, target_orientation;
  if( dtype == GDI_EDGE_UNDIRECTED ){
    origin_orientation = GDI_EDGE_UNDIRECTED;
    target_orientation = GDI_EDGE_UNDIRECTED;
  } else {
    origin_orientation = GDI_EDGE_OUTGOING;
    target_orientation = GDI_EDGE_INCOMING;
  }

  GDA_LightweightEdgesSetEdgeOrientation( origin_orientation, edge->origin_lightweight_edge_offset, edge->origin );
  GDA_LightweightEdgesSetEdgeOrientation( target_orientation, edge->target_lightweight_edge_offset, edge->target );

  edge->write_flag = true;
  edge->origin->write_flag = true;
  edge->target->write_flag = true;
  edge->transaction->write_flag = true;

  return GDI_SUCCESS;
}


int GDI_AddLabelToEdge( GDI_Label label, GDI_EdgeHolder edge ) {
  /**
    check the input arguments
   */
  if( edge == GDI_EDGE_NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( edge->delete_flag ) {
    return GDI_ERROR_EDGE;
  }

  if( label == GDI_LABEL_NULL ) {
    return GDI_ERROR_LABEL;
  }

  if( edge->transaction->db != label->db ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  if( edge->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  // TODO: check for GDI_ERROR_NON_UNIQUE_ID

  /**
    passed all input checks
   */

  if( label == GDI_LABEL_NONE ) {
    return GDI_SUCCESS;
  }

  /**
    try to update to a write lock (origin)
   */
  if( edge->origin->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->origin );
    if( edge->origin->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    try to update to a write lock (target)
   */
  if( edge->target->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->target );
    if( edge->target->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    since we have only space for edge label right now,
    we will overwrite the existing one
   */
  GDA_LightweightEdgesSetLabel( label->int_handle, edge->origin_lightweight_edge_offset, edge->origin );
  GDA_LightweightEdgesSetLabel( label->int_handle, edge->target_lightweight_edge_offset, edge->target );

  // TODO: potential index updates

  edge->write_flag = true;
  edge->origin->write_flag = true;
  edge->target->write_flag = true;
  edge->transaction->write_flag = true;

  return GDI_SUCCESS;
}


int GDI_RemoveLabelFromEdge( GDI_Label label, GDI_EdgeHolder edge ) {
  /**
    check the input arguments
   */
  if( edge == GDI_EDGE_NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( edge->delete_flag ) {
    return GDI_ERROR_EDGE;
  }

  if( label == GDI_LABEL_NULL ) {
    return GDI_ERROR_LABEL;
  }

  if( edge->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  if( edge->transaction->db != label->db ) {
    /**
      label is from a different graph database, so the edge can't
      have that label -> we are already done
     */
    return GDI_SUCCESS;
  }

  // TODO: check for GDI_ERROR_NON_UNIQUE_ID
  // maybe have to do this twice: once here and once during the commit

  /**
    passed all input checks
   */

  uint8_t label_int_handle;
  bool found_flag;
  GDA_LightweightEdgesGetLabel( &label_int_handle, edge->origin_lightweight_edge_offset, edge->origin, &found_flag );
  assert( found_flag );

  if( (label == GDI_LABEL_NONE) || (label->int_handle != label_int_handle) ) {
    return GDI_SUCCESS;
  }

  /**
    try to update to a write lock (origin)
   */
  if( edge->origin->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->origin );
    if( edge->origin->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    try to update to a write lock (target)
   */
  if( edge->target->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->target );
    if( edge->target->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  GDA_LightweightEdgesSetLabel( GDI_LABEL_NONE->int_handle, edge->origin_lightweight_edge_offset, edge->origin );
  GDA_LightweightEdgesSetLabel( GDI_LABEL_NONE->int_handle, edge->target_lightweight_edge_offset, edge->target );

  // TODO: update the index (maybe)

  edge->write_flag = true;
  edge->origin->write_flag = true;
  edge->target->write_flag = true;
  edge->transaction->write_flag = true;

  return GDI_SUCCESS;
}


int GDI_GetAllLabelsOfEdge( GDI_Label array_of_labels[], size_t count, size_t* resultcount, GDI_EdgeHolder edge ) {
  /**
    check the input arguments
   */
  if( edge == GDI_EDGE_NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( edge->delete_flag ) {
    return GDI_ERROR_EDGE;
  }

  if( resultcount == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  /**
    passed all input checks
   */

  uint8_t label_int_handle;
  bool found_flag;

  GDA_LightweightEdgesGetLabel( &label_int_handle, edge->origin_lightweight_edge_offset, edge->origin, &found_flag );
  assert( found_flag );

  if( label_int_handle == GDI_LABEL_NONE->int_handle ) {
    *resultcount = 0;
  } else {
    *resultcount = 1;
    if( (array_of_labels != NULL) && (count > 0) ) {
      /**
        didn't request the size trick, so update array_of_labels as well
       */

      /**
        simplification of GDA_IntHandleToLabel to avoid the additional input validation
       */
      uint32_t lookup_int_handle = label_int_handle;
      GDA_Node** node = GDA_hashmap_get( edge->transaction->db->labels->handle_to_address, &lookup_int_handle );
      assert( node != NULL );
      array_of_labels[0] = *(GDI_Label*)((*node)->value);
    }
  }

  return GDI_SUCCESS;
}


int GDI_AddPropertyToEdge( const void* value, size_t count, GDI_PropertyType ptype, GDI_EdgeHolder edge ) {
  /**
    check the input arguments
   */
  if( edge == GDI_EDGE_NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( edge->delete_flag ) {
    return GDI_ERROR_EDGE;
  }

  if( ptype == GDI_PROPERTY_TYPE_NULL ) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if( (ptype == GDI_PROPERTY_TYPE_DEGREE) || (ptype == GDI_PROPERTY_TYPE_INDEGREE) || (ptype == GDI_PROPERTY_TYPE_OUTDEGREE) ) {
    return GDI_ERROR_READ_ONLY_PROPERTY_TYPE;
  }

  if( edge->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  /**
    already took care of the other predefined property types earlier,
    so that's the only property type left, that has to be handled differently
   */
  if( (ptype != GDI_PROPERTY_TYPE_ID) && (edge->transaction->db != ptype->db) ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  if( (count > 0) && (value == NULL) ) {
    return GDI_ERROR_BUFFER;
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

  // TODO: check for GDI_ERROR_PROPERTY_TYPE_EXISTS, if ptype->etype == GDI_SINGLE_ENTITY

  // TODO: check whether property already exists on edge (GDI_ERROR_PROPERTY_EXISTS)

  /**
    passed all input checks
   */

  /**
    try to update to a write lock (origin)
   */
  if( edge->origin->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->origin );
    if( edge->origin->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    try to update to a write lock (target)
   */
  if( edge->target->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->target );
    if( edge->target->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  fprintf( stderr, "GDI_AddPropertyToEdge implements only input parsing and should not be used.\n" );

  // TODO: probably have to handle GDI_PROPERTY_TYPE_ID differently

  edge->write_flag = true; // TODO: only if the property is actually inserted
  edge->origin->write_flag = true;
  edge->target->write_flag = true;
  edge->transaction->write_flag = true;

  return GDI_SUCCESS;
}


int GDI_RemovePropertiesFromEdge( GDI_PropertyType ptype, GDI_EdgeHolder edge ) {
  /**
    check the input arguments
   */
  if( edge == GDI_EDGE_NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( edge->delete_flag ) {
    return GDI_ERROR_EDGE;
  }

  if( ptype == GDI_PROPERTY_TYPE_NULL ) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if( edge->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  if( (ptype == GDI_PROPERTY_TYPE_DEGREE) || (ptype == GDI_PROPERTY_TYPE_INDEGREE) || (ptype == GDI_PROPERTY_TYPE_OUTDEGREE) ) {
    return GDI_ERROR_READ_ONLY_PROPERTY_TYPE;
  }

  if( (ptype != GDI_PROPERTY_TYPE_ID) && (edge->transaction->db != ptype->db) ) {
    /**
      property type is from a different graph database, so the edge can't
      have a property of that type -> we are already done
     */
    return GDI_SUCCESS;
  }

  /**
    passed all input checks
   */

  /**
    try to update to a write lock (origin)
   */
  if( edge->origin->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->origin );
    if( edge->origin->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    try to update to a write lock (target)
   */
  if( edge->target->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->target );
    if( edge->target->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  fprintf( stderr, "GDI_RemovePropertiesFromEdge implements only input parsing and should not be used.\n" );

  // TODO: remove properties from local edge object and update the index (maybe)
  // TODO: probably have to handle GDI_PROPERTY_TYPE_ID differently

  edge->write_flag = true; // TODO: if there are any properties to be removed
  edge->origin->write_flag = true;
  edge->target->write_flag = true;
  edge->transaction->write_flag = true;

  return GDI_SUCCESS;
}


int GDI_RemoveSpecificPropertyFromEdge( const void* value, size_t count, GDI_PropertyType ptype, GDI_EdgeHolder edge ) {
  /**
    check the input arguments
   */
  if( edge == GDI_EDGE_NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( edge->delete_flag ) {
    return GDI_ERROR_EDGE;
  }

  if( ptype == GDI_PROPERTY_TYPE_NULL ) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if( edge->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  if( (ptype == GDI_PROPERTY_TYPE_DEGREE) || (ptype == GDI_PROPERTY_TYPE_INDEGREE) || (ptype == GDI_PROPERTY_TYPE_OUTDEGREE) ) {
    return GDI_ERROR_READ_ONLY_PROPERTY_TYPE;
  }

  if( (ptype != GDI_PROPERTY_TYPE_ID) && (edge->transaction->db != ptype->db) ) {
    /**
      property type is from a different graph database, so the edge can't
      have a property of that type -> we are already done
     */
    return GDI_SUCCESS;
  }

  if( (count > 0) && (value == NULL) ) {
    return GDI_ERROR_BUFFER;
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
    try to update to a write lock (origin)
   */
  if( edge->origin->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->origin );
    if( edge->origin->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    try to update to a write lock (target)
   */
  if( edge->target->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->target );
    if( edge->target->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  fprintf( stderr, "GDI_RemoveSpecificPropertyFromEdge implements only input parsing and should not be used.\n" );

  // TODO: remove property from local edge object and update the index (maybe)
  // TODO: probably have to handle GDI_PROPERTY_TYPE_ID differently

  edge->write_flag = true; // TODO: if any property is removed
  edge->origin->write_flag = true;
  edge->target->write_flag = true;
  edge->transaction->write_flag = true;

  return GDI_SUCCESS;
}


int GDI_UpdatePropertyOfEdge( const void* value, size_t count, GDI_PropertyType ptype, GDI_EdgeHolder edge ) {
  /**
    check the input arguments
   */
  if( edge == GDI_EDGE_NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( edge->delete_flag ) {
    return GDI_ERROR_EDGE;
  }

  if( ptype == GDI_PROPERTY_TYPE_NULL ) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if( edge->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  if( (ptype == GDI_PROPERTY_TYPE_DEGREE) || (ptype == GDI_PROPERTY_TYPE_INDEGREE) || (ptype == GDI_PROPERTY_TYPE_OUTDEGREE) ) {
    return GDI_ERROR_READ_ONLY_PROPERTY_TYPE;
  }

  if( (ptype != GDI_PROPERTY_TYPE_ID) && (edge->transaction->db != ptype->db) ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  if( ptype->etype == GDI_MULTIPLE_ENTITY ) {
    return GDI_ERROR_WRONG_TYPE;
  }

  if( (count > 0) && (value == NULL) ) {
    return GDI_ERROR_BUFFER;
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

  // TODO: return GDI_ERROR_NO_PROPERTY, if no property is found

  /**
    passed all input checks
   */

  /**
    try to update to a write lock (origin)
   */
  if( edge->origin->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->origin );
    if( edge->origin->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    try to update to a write lock (target)
   */
  if( edge->target->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->target );
    if( edge->target->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  fprintf( stderr, "GDI_UpdatePropertyOfEdge implements only input parsing and should not be used.\n" );

  // TODO: update property from local edge object and update the index (maybe)
  // TODO: probably have to handle GDI_PROPERTY_TYPE_ID differently

  edge->write_flag = true; // TODO: only if a property is updated
  edge->origin->write_flag = true;
  edge->target->write_flag = true;
  edge->transaction->write_flag = true;

  return GDI_SUCCESS;
}


int GDI_UpdateSpecificPropertyOfEdge( const void* old_value, size_t old_count, const void* new_value, size_t new_count, GDI_PropertyType ptype, GDI_EdgeHolder edge ) {
  /**
    check the input arguments
   */
  if( edge == GDI_EDGE_NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( edge->delete_flag ) {
    return GDI_ERROR_EDGE;
  }

  if( ptype == GDI_PROPERTY_TYPE_NULL ) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if( edge->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  if( (ptype == GDI_PROPERTY_TYPE_DEGREE) || (ptype == GDI_PROPERTY_TYPE_INDEGREE) || (ptype == GDI_PROPERTY_TYPE_OUTDEGREE) ) {
    return GDI_ERROR_READ_ONLY_PROPERTY_TYPE;
  }

  if( (ptype != GDI_PROPERTY_TYPE_ID) && (edge->transaction->db != ptype->db) ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  if( ((old_count > 0) && (old_value == NULL)) || ((new_count > 0) && (new_value == NULL)) ) {
    return GDI_ERROR_BUFFER;
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

  // TODO: return GDI_ERROR_NO_PROPERTY, if no property is found
  // TODO: return GDI_ERROR_PROPERTY_EXISTS, if another property with the same value exists

  /**
    passed all input checks
   */

  /**
    try to update to a write lock (origin)
   */
  if( edge->origin->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->origin );
    if( edge->origin->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    try to update to a write lock (target)
   */
  if( edge->target->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->target );
    if( edge->target->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  fprintf( stderr, "GDI_UpdateSpecificPropertyOfEdge implements only input parsing and should not be used.\n" );

  // TODO: update property from local edge object and update the index (maybe)
  // TODO: probably have to handle GDI_PROPERTY_TYPE_ID differently

  edge->write_flag = true; // TODO: only if a property is updated
  edge->origin->write_flag = true;
  edge->target->write_flag = true;
  edge->transaction->write_flag = true;

  return GDI_SUCCESS;
}


int GDI_SetPropertyOfEdge( const void* value, size_t count, GDI_PropertyType ptype, GDI_EdgeHolder edge ) {
  /**
    check the input arguments
   */
  if( edge == GDI_EDGE_NULL ) {
    return GDI_ERROR_EDGE;
  }

  if( edge->delete_flag ) {
    return GDI_ERROR_EDGE;
  }

  if( ptype == GDI_PROPERTY_TYPE_NULL ) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if( edge->transaction->type == GDI_COLLECTIVE_TRANSACTION ) {
    return GDI_ERROR_READ_ONLY_TRANSACTION;
  }

  if( (ptype == GDI_PROPERTY_TYPE_DEGREE) || (ptype == GDI_PROPERTY_TYPE_INDEGREE) || (ptype == GDI_PROPERTY_TYPE_OUTDEGREE) ) {
    return GDI_ERROR_READ_ONLY_PROPERTY_TYPE;
  }

  if( (ptype != GDI_PROPERTY_TYPE_ID) && (edge->transaction->db != ptype->db) ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  if( ptype->etype == GDI_MULTIPLE_ENTITY ) {
    return GDI_ERROR_WRONG_TYPE;
  }

  if( (count > 0) && (value == NULL) ) {
    return GDI_ERROR_BUFFER;
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
    try to update to a write lock (origin)
   */
  if( edge->origin->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->origin );
    if( edge->origin->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  /**
    try to update to a write lock (target)
   */
  if( edge->target->lock_type == GDA_READ_LOCK ) {
    GDA_UpdateToVertexWriteLock( edge->target );
    if( edge->target->lock_type == GDA_READ_LOCK ) {
      edge->transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }
  }

  fprintf( stderr, "GDI_SetPropertyOfEdge implements only input parsing and should not be used.\n" );

  // TODO: update property from local edge object and update the index (maybe)
  // TODO: probably have to handle GDI_PROPERTY_TYPE_ID differently

  edge->write_flag = true; // TODO: only if there is a change
  edge->origin->write_flag = true;
  edge->target->write_flag = true;
  edge->transaction->write_flag = true;

  return GDI_SUCCESS;
}
