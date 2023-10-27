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
#include "gda_lock.h"
#include "gda_vertex.h"

int GDI_TranslateVertexID( bool* found_flag, GDI_Vertex_uid* internal_uid, GDI_Label label, const void* external_id, size_t size, GDI_Transaction transaction ) {
  /**
    check the input arguments
   */
  if( (found_flag == NULL) || (internal_uid == NULL) || (external_id == NULL) ) {
    return GDI_ERROR_BUFFER;
  }

  if( transaction == GDI_TRANSACTION_NULL ) {
    return GDI_ERROR_TRANSACTION;
  }

  if( label == GDI_LABEL_NULL ) {
    return GDI_ERROR_LABEL;
  }

  if( (label != GDI_LABEL_NONE) && (label->db != transaction->db) ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  if( size == 0 ) {
    return GDI_ERROR_SIZE;
  }

  /**
    passed all checks
   */

  /**
    will also work with GDI_LABEL_NONE, since its integer handle is zero
   */
  uint64_t hashed_key = GDA_hash_property_id( external_id, size, label->int_handle );

  size_t minimum = 7;
  if( size < minimum ) {
    minimum = size;
  }
  uint64_t key = 0;
  memcpy( &key, external_id, minimum );

  key = (key & 0x00FFFFFFFFFFFFFF) | ((uint64_t)(label->int_handle) << 56);

  uint64_t incarnation;

  GDA_FindElementInRMAHashMap( hashed_key, key, internal_uid, &incarnation, found_flag, transaction->db->internal_index );

  // TODO: GDI_WARNING_NON_UNIQUE_ID

  if( (*found_flag) && (transaction->type == GDI_SINGLE_PROCESS_TRANSACTION) ) {
    /**
      check whether vertex is already associated with this transaction
     */
    GDI_VertexHolder* v_hashmap = GDA_hashmap_get( transaction->v_translate_d2l, &internal_uid );
    if( v_hashmap != NULL ) {
      /**
        vertex is already associated with this transaction
       */
      return GDI_SUCCESS;
    }

    /**
      vertex is not associated with this transaction:
      1) acquire a read lock
      2) check that there were no concurrent delete operations to that vertex
      3) associate the vertex
     */
    GDI_VertexHolder vertex = malloc( sizeof(GDI_VertexHolder_desc_t) );
    assert( vertex != NULL );

    vertex->transaction = transaction;
    vertex->lock_type = GDA_NO_LOCK;

    /**
      TODO: Workaround: set up (*vertex)->blocks->data before
      GDA_AcquireVertexReadLock is called. The initialization of
      (*vertex)->blocks and its fields is finished later.
     */
    vertex->blocks = malloc( sizeof( GDA_Vector ) );
    vertex->blocks->data = malloc( sizeof( GDA_DPointer ) );
    *(uint64_t*)(vertex->blocks->data) = *internal_uid;

    /**
      try to acquire a read lock
     */
    GDA_AcquireVertexReadLock( vertex );
    if( vertex->lock_type == GDA_NO_LOCK ) {
      /**
        acquisition of a read lock failed, so free the allocated memory
        and return an error
       */
      free( vertex->blocks->data );
      free( vertex->blocks );
      free( vertex );
      transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }

    if( (incarnation & 0x00000000FFFFFFFF) != vertex->incarnation ) {
      /**
        the vertex in question was removed from the database, but the
        internal index wasn't aware yet
       */
      free( vertex->blocks->data );
      free( vertex->blocks );
      free( vertex );
      transaction->critical_flag = true;
      return GDI_ERROR_TRANSACTION_CRITICAL;
    }

    GDA_AssociateVertex( *internal_uid, transaction, vertex );
  }

  return GDI_SUCCESS;
}
