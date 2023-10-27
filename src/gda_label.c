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
  This function removes all labels associated
  to the graph database. It is a convenience function that can be
  called when the full database gets destroyed.
  It does NOT:
    - remove the labels from associated objects (e.g. edges, vertices, constraints, subconstraints)
    - remove the reference to the labels from all data structures,
      meaning, that lists and hash maps still contain references to the labels.
 */
int GDA_FreeAllLabel(GDI_Database graph_db) {
  if(graph_db == GDI_DATABASE_NULL) {
    return GDI_ERROR_DATABASE;
  }

  size_t resultcount = 0;
  GDI_GetAllLabelsOfDatabase( NULL, 0, &resultcount, graph_db );
  if(resultcount == 0) {
    return GDI_SUCCESS;
  }

  GDI_Label* label_array = (GDI_Label*)malloc(sizeof(GDI_Label) * resultcount);
  assert(label_array != NULL);

  size_t resultcount2 = 0;
  GDI_GetAllLabelsOfDatabase(label_array, resultcount, &resultcount2, graph_db);
  assert(resultcount == resultcount2);

  for(size_t i = 0; i < resultcount; i++) {
    free(label_array[i]->name);
    free(label_array[i]);
  }

  free(label_array);
  return GDI_SUCCESS;
}


/**
  Convencience function to get the integer handle from a given label.
 */
int GDA_LabelToIntHandle(GDI_Label label, uint32_t* handle) {
  if(label == GDI_LABEL_NULL) {
    return GDI_ERROR_LABEL;
  }
  if(handle == NULL) {
    return GDI_ERROR_BUFFER;
  }
  *handle = label->int_handle;
  return GDI_SUCCESS;
}


/**
  Convenience function that returns the label handle to some given integer handle.
  This function can be used for example when you read a label from some object (e.g. a vertex)
  and want to get the actual handle of the label.
 */
int GDA_IntHandleToLabel(GDI_Database graph_db, uint32_t int_handle, GDI_Label *label) {
  if(graph_db == GDI_DATABASE_NULL){
    return GDI_ERROR_DATABASE;
  }

  if( label == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  if( (label == GDI_LABEL_NULL) || (*label == GDI_LABEL_NONE) ) {
    return GDI_ERROR_LABEL;
  }

  if(int_handle == GDI_LABEL_NONE->int_handle) {
    *label = GDI_LABEL_NONE;
    return GDI_SUCCESS;
  }

  void** ptr = GDA_hashmap_get(graph_db->labels->handle_to_address, &int_handle);
  if(ptr == NULL) {
    *label = GDI_LABEL_NULL;
  }
  else {
    GDA_Node* node = *ptr;
    ptr = node->value;
    *label = (GDI_Label)*ptr;
  }

  return GDI_SUCCESS;
}
