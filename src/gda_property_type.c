// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#include <assert.h>

#include "gdi.h"

/**
  This function removes all property types associated
  to the graph database. It is a convenience function that can be
  called when the full database gets destroyed.
  It does NOT:
    - remove the property type from associated objects (e.g. edges, vertices, constraints, subconstraints)
    - remove the reference to the property types from all data structures,
      meaning, that lists and hash maps still contain references to the property types.
 */
int GDA_FreeAllPropertyType(GDI_Database graph_db) {
  if(graph_db == GDI_DATABASE_NULL) {
    return GDI_ERROR_DATABASE;
  }

  size_t resultcount = 0;
  GDI_GetAllPropertyTypesOfDatabase( NULL, 0, &resultcount, graph_db );
  if(resultcount == 0) {
    return GDI_SUCCESS;
  }

  GDI_PropertyType* ptype_array = (GDI_PropertyType*)malloc(sizeof(GDI_PropertyType) * resultcount);
  assert(ptype_array != NULL);

  size_t resultcount2 = 0;
  GDI_GetAllPropertyTypesOfDatabase(ptype_array, resultcount, &resultcount2, graph_db);
  assert(resultcount == resultcount2);

  for(size_t i = 0; i < resultcount; i++) {
    free(ptype_array[i]->name);
    free(ptype_array[i]);
  }

  free(ptype_array);
  return GDI_SUCCESS;
}


/**
  Convencience function to get the integer handle from a given property type.
 */
int GDA_PropertyTypeToIntHandle(GDI_PropertyType ptype, uint32_t* handle) {
  if(ptype == GDI_PROPERTY_TYPE_NULL) {
    return GDI_ERROR_PROPERTY_TYPE;
  }
  if(handle == NULL) {
    return GDI_ERROR_BUFFER;
  }
  *handle = ptype->int_handle;
  return GDI_SUCCESS;
}


/**
  Convenience function that returns the property type handle to some given integer handle.
  This function can be used for example when you read a property type form some object (e.g. a vertex)
  and want to get the actual handle of the property type.
 */
int GDA_IntHandleToPropertyType(GDI_Database graph_db, uint32_t int_handle, GDI_PropertyType *ptype) {
  if(graph_db == GDI_DATABASE_NULL) {
    return GDI_ERROR_DATABASE;
  }

  if( ptype == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  if( (ptype == GDI_PROPERTY_TYPE_NULL) || (*ptype == GDI_PROPERTY_TYPE_ID) || (*ptype == GDI_PROPERTY_TYPE_DEGREE) ||
    (*ptype == GDI_PROPERTY_TYPE_INDEGREE) || (*ptype == GDI_PROPERTY_TYPE_OUTDEGREE) ) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if(int_handle == GDI_PROPERTY_TYPE_ID->int_handle) {
    *ptype = GDI_PROPERTY_TYPE_ID;
    return GDI_SUCCESS;
  }

  if(int_handle == GDI_PROPERTY_TYPE_DEGREE->int_handle) {
    *ptype = GDI_PROPERTY_TYPE_DEGREE;
    return GDI_SUCCESS;
  }

  if(int_handle == GDI_PROPERTY_TYPE_INDEGREE->int_handle) {
    *ptype = GDI_PROPERTY_TYPE_INDEGREE;
    return GDI_SUCCESS;
  }

  if(int_handle == GDI_PROPERTY_TYPE_OUTDEGREE->int_handle) {
    *ptype = GDI_PROPERTY_TYPE_OUTDEGREE;
    return GDI_SUCCESS;
  }

  // use the hash map to lookup the given handle
  void** ptr = GDA_hashmap_get(graph_db->ptypes->handle_to_address, &int_handle);
  if(ptr == NULL) {
    *ptype = GDI_PROPERTY_TYPE_NULL;
  }
  else {
    GDA_Node* node = *ptr;
    ptr = node->value;
    *ptype = (GDI_PropertyType)*ptr;
  }

  return GDI_SUCCESS;
}
