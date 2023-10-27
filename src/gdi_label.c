// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#include <assert.h>
#include <string.h>

#include "gdi.h"
#include "gda_constraint.h"
#include "gda_utf8.h"

#define MIN(a,b) (((a)<(b))?(a):(b))

inline uint64_t label_key(const char* str) {
  return GDA_djb2_hash((const unsigned char*)str);
}


int GDI_CreateLabel( const char* name, GDI_Database graph_db, GDI_Label* label ) {
  // check for GDI_ERROR_NOT_SAME is omitted

  if(label == NULL || *label == GDI_LABEL_NONE) {
    return GDI_ERROR_BUFFER;
  }

  if(graph_db == GDI_DATABASE_NULL) {
    return GDI_ERROR_DATABASE;
  }

  if(name == NULL || name[0] == '\0' ) {
    return GDI_ERROR_EMPTY_NAME;
  }

  // copy and truncate the given label name
  char* name_cut = GDA_copy_truncate_string(name, GDI_MAX_OBJECT_NAME-1);

  // test if the string is empty when we removed the trailing spaces
  if(name_cut[0] == '\0' ) {
    free(name_cut);
    return GDI_ERROR_EMPTY_NAME;
  }

  // check if the name is unique
  uint64_t name_key = label_key(name_cut);
  if( strcmp(name_cut, GDI_LABEL_NONE->name) == 0 ||
      GDA_hashmap_find(graph_db->labels->name_to_address, &name_key) != GDA_HASHMAP_NOT_FOUND ) {
    free(name_cut);
    return GDI_ERROR_NAME_EXISTS;
  }

  // create the struct
  *label = (GDI_Label)malloc(sizeof( GDI_Label_desc_t  ));
  assert(*label != NULL);
  (*label)->db = graph_db;
  (*label)->int_handle = (graph_db->labels->label_max)++;
  (*label)->name = name_cut;

  // add to the data structures: store the reference to the struct in the list
  // store a reference to the node in the hashmaps
  GDA_Node* node = GDA_list_push_back(graph_db->labels->labels, label);
  GDA_hashmap_insert(graph_db->labels->handle_to_address, &((*label)->int_handle), &node);
  GDA_hashmap_insert(graph_db->labels->name_to_address, &name_key, &node);

  return GDI_SUCCESS;
}


int GDI_FreeLabel( GDI_Label* label ) {
  // check for GDI_ERROR_NOT_SAME is omitted

  if(label == NULL || *label == GDI_LABEL_NULL || *label == GDI_LABEL_NONE) {
    return GDI_ERROR_LABEL;
  }

  GDA_HashMap* handle_to_address = ((GDI_Database)(*label)->db)->labels->handle_to_address;
  GDA_HashMap* name_to_address = ((GDI_Database)(*label)->db)->labels->name_to_address;
  GDA_List* labels = ((GDI_Database)(*label)->db)->labels->labels;

  int32_t pos = GDA_hashmap_find(handle_to_address, &((*label)->int_handle));
  if(pos == GDA_HASHMAP_NOT_FOUND) {
    return GDI_ERROR_LABEL;
  }


  fprintf( stderr, "GDI_FreeLabel is not fully implemented. Removal of label from indexes, vertices and edges is still missing.\n" );

  // !!!
  // TODO: REMOVE FROM ALL INDEXES, VERTICES, AND EDGES THE LABEL
  // return GDI\_WARNING\_NON\_UNIQUE\_ID if needed
  // !!!



  // mark the subconstraints and constraints as stale that have an according label condition
  // it also removes the according label conditions from the subconstraints and constraints
  GDA_MarkStaleByLabel(*label);

  // erase from the data structures
  GDA_Node** node = GDA_hashmap_get_at(handle_to_address, (size_t)pos);
  GDA_list_erase_single(labels, *node);
  GDA_hashmap_erase_at(handle_to_address, (size_t)pos);
  uint64_t name_key = label_key( (*label)->name );
  GDA_hashmap_erase( name_to_address,  &name_key);

  free((*label)->name);
  free(*label);
  *label = GDI_LABEL_NULL;

  return GDI_SUCCESS;
}


int GDI_UpdateLabel( const char* name, GDI_Label label ) {
  // TODO: check for GDI\_ERROR\_NOT\_SAME

  if(name == NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(name[0] == '\0') {
    return GDI_ERROR_EMPTY_NAME;
  }

  if(label == GDI_LABEL_NULL || label == GDI_LABEL_NONE) {
    return GDI_ERROR_LABEL;
  }

  // copy and truncate the given label name
  char* name_cut = GDA_copy_truncate_string(name, GDI_MAX_OBJECT_NAME-1);

  // test if the string is empty when we removed the trailing spaces
  if(name_cut[0] == '\0' ) {
    free(name_cut);
    return GDI_ERROR_EMPTY_NAME;
  }

  // do nothing if the name remains the same
  if(strcmp(name_cut, label->name) == 0) {
    free(name_cut);
    return GDI_SUCCESS;
  }

  // check if the name is unique
  GDA_HashMap* name_to_address = ((GDI_Database)label->db)->labels->name_to_address;
  uint64_t name_key_new = label_key(name_cut);
  if( strcmp( name_cut, GDI_LABEL_NONE->name) == 0 ||
      GDA_hashmap_find(name_to_address, &name_key_new ) != GDA_HASHMAP_NOT_FOUND ) {
    free(name_cut);
    return GDI_ERROR_NAME_EXISTS;
  }

  // lookup the node element in the list
  uint64_t name_key = label_key(label->name);
  int32_t pos = GDA_hashmap_find(name_to_address, &name_key);
  if(pos == GDA_HASHMAP_NOT_FOUND) {
    free(name_cut);
    return GDI_ERROR_INTERN;
  }
  GDA_Node** node_ptr = GDA_hashmap_get_at(name_to_address, (size_t)pos);
  GDA_Node* node = *node_ptr;

  // remove the old name and free the old name buffer
  GDA_hashmap_erase_at(name_to_address, (size_t)pos);
  free(label->name);

  // assign the name and insert it into the hash map
  label->name = name_cut;
  GDA_hashmap_insert(((GDI_Database)label->db)->labels->name_to_address, &name_key_new, &node);

  return GDI_SUCCESS;
}


int GDI_GetLabelFromName( GDI_Label* label, const char* name, GDI_Database graph_db ) {
  if(graph_db == GDI_DATABASE_NULL) {
    return GDI_ERROR_DATABASE;
  }

  if(label == GDI_LABEL_NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(name == NULL) {
    return GDI_ERROR_ARGUMENT;
  }

  if(name[0] == '\0') {
    *label = GDI_LABEL_NULL;
    return GDI_SUCCESS;
  }

  char* name_cut = GDA_copy_truncate_string(name, GDI_MAX_OBJECT_NAME-1);

  // compare to GDI_LABEL_NONE
  if(strcmp(name_cut, GDI_LABEL_NONE->name) == 0) {
    *label = GDI_LABEL_NONE;
    free(name_cut);
    return GDI_SUCCESS;
  }

  // lookup in the hash map
  uint64_t name_key = label_key(name_cut);
  GDA_HashMap* name_to_address = graph_db->labels->name_to_address;
  int32_t pos = GDA_hashmap_find(name_to_address, &name_key);
  if(pos == GDA_HASHMAP_NOT_FOUND) {
    *label = GDI_LABEL_NULL;
    free(name_cut);
    return GDI_SUCCESS;
  }

  // the hash map points to a node element, so we need to determine the label
  GDA_Node** node_ptr = GDA_hashmap_get_at(name_to_address, (size_t)pos);
  GDA_Node* node = *node_ptr;
  void** ptr = node->value;
  *label = *ptr;

  free(name_cut);
  return GDI_SUCCESS;
}


int GDI_GetNameOfLabel( char* name, size_t length, size_t* resultlength, GDI_Label label) {

  if(resultlength == NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(label == GDI_LABEL_NULL) {
    return GDI_ERROR_LABEL;
  }

  if(name == NULL || length == 0) {
    *resultlength = strlen(label->name);
    return GDI_SUCCESS;
  }

  size_t name_len = strlen(label->name);
  size_t len = MIN(length - 1, name_len );
  strncpy(name, label->name, len);

  *resultlength = GDA_truncate_string(name, len);

  if(len < name_len) {
    return GDI_ERROR_TRUNCATE;
  }

  return GDI_SUCCESS;
}


int GDI_GetAllLabelsOfDatabase( GDI_Label array_of_labels[], size_t count, size_t* resultcount, GDI_Database graph_db ) {

  if(graph_db == GDI_DATABASE_NULL) {
    return GDI_ERROR_DATABASE;
  }

  if(resultcount == NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(array_of_labels == NULL || count == 0) {
    *resultcount = GDA_list_size(graph_db->labels->labels);
    return GDI_SUCCESS;
  }

  *resultcount = GDA_list_to_array(graph_db->labels->labels, array_of_labels, count);

  if(*resultcount < GDA_list_size(graph_db->labels->labels)) {
    return GDI_ERROR_TRUNCATE;
  }

  return GDI_SUCCESS;
}
