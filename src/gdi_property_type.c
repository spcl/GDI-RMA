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
#include "gda_datatype.h"
#include "gda_utf8.h"

#define MIN(a,b) (((a)<(b))?(a):(b))

/**
  Convenience function that converts a string into a 64bit hash.
  For a few strings (ca. 1000), the probability for a hash collision is
  approx. 1/10^12 (see https://preshing.com/20110504/hash-collision-probabilities/)
 */
inline uint64_t property_type_key(const char* str) {
  return GDA_djb2_hash((const unsigned char*)str);
}


/**
  Convenience function to test if the given property type is predefined or not.
 */
inline int is_predefined_property_type(GDI_PropertyType ptype) {
  if(ptype == GDI_PROPERTY_TYPE_ID || ptype == GDI_PROPERTY_TYPE_DEGREE ||
         ptype == GDI_PROPERTY_TYPE_INDEGREE || ptype == GDI_PROPERTY_TYPE_OUTDEGREE) {
    return GDI_TRUE;
  } else {
    return GDI_FALSE;
  }
}


/**
  Convenience function to test if the given string is the same as the one
  from a predefined property type.
 */
inline int is_name_of_predefined_property_type(const char* name) {
  if( strcmp(name, GDI_PROPERTY_TYPE_ID->name) == 0 || strcmp(name, GDI_PROPERTY_TYPE_DEGREE->name) == 0 ||
         strcmp(name, GDI_PROPERTY_TYPE_INDEGREE->name) == 0 || strcmp(name, GDI_PROPERTY_TYPE_OUTDEGREE->name) == 0) {
    return GDI_TRUE;
  } else{
    return GDI_FALSE;
  }
}


/**
  Convenience function to test if the given size type is invalid.
 */
inline int is_invalid_stype(int stype) {
  if(!(stype == GDI_FIXED_SIZE || stype == GDI_MAX_SIZE || stype == GDI_NO_SIZE_LIMIT )) {
    return GDI_TRUE;
  } else {
    return GDI_FALSE;
  }
}


/**
  Convenience function to test if the given entity type is invalid.
 */
inline int is_invalid_etype(int etype) {
  if(!(etype == GDI_MULTIPLE_ENTITY || etype == GDI_SINGLE_ENTITY)) {
    return GDI_TRUE;
  } else {
    return GDI_FALSE;
  }
}


int GDI_CreatePropertyType( const char* name, int etype, GDI_Datatype dtype,
                            int stype, size_t count, GDI_Database graph_db,
                            GDI_PropertyType* ptype ) {
  // note: GDI\_ERROR\_NOT\_SAME omitted

  if(!GDA_IsDatatypeValid(dtype)) {
    return GDI_ERROR_DATATYPE;
  }

  if(is_invalid_etype(etype) == GDI_TRUE) {
    return GDI_ERROR_STATE;
  }

  if(is_invalid_stype(stype) == GDI_TRUE) {
    return GDI_ERROR_STATE;
  }

  if(ptype == NULL || is_predefined_property_type(*ptype) == GDI_TRUE) {
    return GDI_ERROR_BUFFER;
  }

  if(graph_db == GDI_DATABASE_NULL) {
    return GDI_ERROR_DATABASE;
  }

  if(name == NULL || name[0] == '\0' ) {
    return GDI_ERROR_EMPTY_NAME;
  }

  // copy and truncate the given property type name
  char* name_cut = GDA_copy_truncate_string(name, GDI_MAX_OBJECT_NAME-1);

  // test if the string is empty when we removed the trailing spaces
  if(name_cut[0] == '\0' ) {
    free(name_cut);
    return GDI_ERROR_EMPTY_NAME;
  }

  // check if the name is unique
  uint64_t name_key = property_type_key(name_cut);
  if( is_name_of_predefined_property_type(name_cut) == GDI_TRUE ||
      GDA_hashmap_find(graph_db->ptypes->name_to_address, &name_key) != GDA_HASHMAP_NOT_FOUND ) {
    free(name_cut);
    return GDI_ERROR_NAME_EXISTS;
  }

  // create the struct
  *ptype = (GDI_PropertyType)malloc(sizeof(GDI_PropertyType_desc_t));
  assert(*ptype != NULL);
  (*ptype)->db = graph_db;
  (*ptype)->int_handle = (graph_db->ptypes->ptype_max)++;
  (*ptype)->name = name_cut;
  (*ptype)->etype = etype;
  (*ptype)->dtype = dtype;
  (*ptype)->stype = stype;
  (*ptype)->count = count;

  // add to the data structures: store the reference to the struct in the list
  // store a reference to the node in the hashmaps
  GDA_Node* node = GDA_list_push_back(graph_db->ptypes->ptypes, ptype);
  GDA_hashmap_insert(graph_db->ptypes->handle_to_address, &((*ptype)->int_handle), &node);
  GDA_hashmap_insert(graph_db->ptypes->name_to_address, &name_key, &node);

  return GDI_SUCCESS;
}


int GDI_FreePropertyType( GDI_PropertyType* ptype ) {
  // note: GDI\_ERROR\_NOT\_SAME omitted

  if(ptype == NULL || *ptype == GDI_PROPERTY_TYPE_NULL || is_predefined_property_type(*ptype)) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  GDA_HashMap* handle_to_address = ((GDI_Database)(*ptype)->db)->ptypes->handle_to_address;
  GDA_HashMap* name_to_address = ((GDI_Database)(*ptype)->db)->ptypes->name_to_address;
  GDA_List* ptypes = ((GDI_Database)(*ptype)->db)->ptypes->ptypes;


  fprintf( stderr, "GDI_FreePropertyType is not fully implemented. Removal of properties from indexes, vertices and edges is still missing.\n" );

  // !!!
  // TODO: REMOVE FROM ALL INDEXES, VERTICES, AND EDGES THE PROPERTY TYPE
  // !!!


  // mark the subconstraints and constraints as stale that have an according property condition
  // it also removes the according property conditions from the subconstraints and constraints
  GDA_MarkStaleByPropertyType(*ptype);

  // erase from the data structures
  int32_t pos = GDA_hashmap_find(handle_to_address, &((*ptype)->int_handle));
  assert(pos != GDA_HASHMAP_NOT_FOUND);

  GDA_Node** node = GDA_hashmap_get_at(handle_to_address, (size_t)pos);
  GDA_list_erase_single(ptypes, *node);
  GDA_hashmap_erase_at(handle_to_address, (size_t)pos);
  uint64_t name_key = property_type_key( (*ptype)->name );
  GDA_hashmap_erase( name_to_address,  &name_key);

  free((*ptype)->name);
  free(*ptype);

  *ptype = GDI_PROPERTY_TYPE_NULL;
  return GDI_SUCCESS;
}


int GDI_UpdatePropertyType( const char* name, int etype, GDI_Datatype dtype,
                            int stype, size_t count, const void* default_value,
                            GDI_PropertyType ptype ) {
  // GDI\_ERROR\_NOT\_SAME omitted

  if(!GDA_IsDatatypeValid(dtype)) {
    return GDI_ERROR_DATATYPE;
  }

  if(is_invalid_stype(stype) == GDI_TRUE) {
    return GDI_ERROR_STATE;
  }

  if(is_invalid_etype(etype) == GDI_TRUE) {
    return GDI_ERROR_STATE;
  }

  if(name == NULL || default_value == NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(name[0] == '\0') {
    return GDI_ERROR_EMPTY_NAME;
  }

  if(ptype == GDI_PROPERTY_TYPE_NULL || is_predefined_property_type(ptype) == GDI_TRUE) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if(!GDA_CanConvertDatatypes(ptype->dtype, dtype)) {
    return GDI_ERROR_CONVERSION;
  }

  // copy and truncate the given property name name
  char* name_cut = GDA_copy_truncate_string(name, GDI_MAX_OBJECT_NAME-1);

  // test if the string is empty when we removed the trailing spaces
  if(name_cut[0] == '\0' ) {
    free(name_cut);
    return GDI_ERROR_EMPTY_NAME;
  }

  // TODO:
  // Robert: I am currently not sure, whether it would be possible to move the two
  // code blocks, that depend on the strcmp test, together in one bracket, which would
  // fix the maybe-uninitialized error
  uint64_t name_key_new;
  GDA_HashMap* name_to_address;
  if( strcmp(name_cut, ptype->name) != 0 ) {
    /**
      name is not the same as it was before

      so have to check if the new name is unique
     */
    name_to_address = ((GDI_Database)ptype->db)->ptypes->name_to_address;
    name_key_new = property_type_key(name_cut);
    if( is_name_of_predefined_property_type(name_cut) ||
        GDA_hashmap_find(name_to_address, &name_key_new ) != GDA_HASHMAP_NOT_FOUND ) {
      free(name_cut);
      return GDI_ERROR_NAME_EXISTS;
    }
  }


  fprintf( stderr, "GDI_UpdatePropertyType is not fully implemented. Update of properties on vertices and edges is still missing.\n" );

  // !!!
  // TODO: UPDATE THE PROPERTY TYPE ON ALL EDGES, VERTICES
  // !!!


  // TODO:
  // Robert: Additionally we will need to decide, when constraints get marked as stale. Because
  // a name change doesn't necessarily warrant that.

  // mark the subconstraints and constraints as stale that have an according property condition
  // it also removes the according property conditions from the subconstraints and constraints
  GDA_MarkStaleByPropertyType(ptype);

  if( strcmp(name_cut, ptype->name) != 0 ) {
    /**
      only have to update the management structures if we have a new name

      first lookup the node element in the list
     */
    uint64_t name_key = property_type_key(ptype->name);
    int32_t pos = GDA_hashmap_find(name_to_address, &name_key);
    if(pos == GDA_HASHMAP_NOT_FOUND) {
      free(name_cut);
      return GDI_ERROR_INTERN;
    }
    GDA_Node** node_ptr = GDA_hashmap_get_at(name_to_address, (size_t)pos);
    GDA_Node* node = *node_ptr;

    // remove the old name and free the old name buffer
    GDA_hashmap_erase_at(name_to_address, (size_t)pos);
    free(ptype->name);

    // assign the name and insert it into the hash map
    ptype->name = name_cut;
    GDA_hashmap_insert(((GDI_Database)ptype->db)->ptypes->name_to_address, &name_key_new, &node);
  } else {
    free(name_cut);
  }

  ptype->etype = etype;
  ptype->stype = stype;
  ptype->count = count;
  ptype->dtype = dtype;

  return GDI_SUCCESS;
}


int GDI_GetPropertyTypeFromName( GDI_PropertyType* ptype, const char* name,
                                 GDI_Database graph_db ) {
  if(graph_db == GDI_DATABASE_NULL) {
    return GDI_ERROR_DATABASE;
  }

  if(ptype == GDI_PROPERTY_TYPE_NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(name == NULL) {
    return GDI_ERROR_ARGUMENT;
  }

  if(name[0] == '\0') {
    *ptype = GDI_PROPERTY_TYPE_NULL;
    return GDI_SUCCESS;
  }

  char* name_cut = GDA_copy_truncate_string(name, GDI_MAX_OBJECT_NAME-1);

  // compare to GDI_PROPERTY_TYPE_ID
  if(strcmp(name_cut, GDI_PROPERTY_TYPE_ID->name) == 0) {
    *ptype = GDI_PROPERTY_TYPE_ID;
    free(name_cut);
    return GDI_SUCCESS;
  }

  // compare to GDI_PROPERTY_TYPE_DEGREE
  if(strcmp(name_cut, GDI_PROPERTY_TYPE_DEGREE->name) == 0) {
    *ptype = GDI_PROPERTY_TYPE_DEGREE;
    free(name_cut);
    return GDI_SUCCESS;
  }

  // compare to GDI_PROPERTY_TYPE_INDEGREE
  if(strcmp(name_cut, GDI_PROPERTY_TYPE_INDEGREE->name) == 0) {
    *ptype = GDI_PROPERTY_TYPE_INDEGREE;
    free(name_cut);
    return GDI_SUCCESS;
  }

  // compare to GDI_PROPERTY_TYPE_OUTDEGREE
  if(strcmp(name_cut, GDI_PROPERTY_TYPE_OUTDEGREE->name) == 0) {
    *ptype = GDI_PROPERTY_TYPE_OUTDEGREE;
    free(name_cut);
    return GDI_SUCCESS;
  }

  // lookup in the hash map
  uint64_t name_key = property_type_key(name_cut);
  GDA_HashMap* name_to_address = graph_db->ptypes->name_to_address;
  int32_t pos = GDA_hashmap_find(name_to_address, &name_key);
  if(pos == GDA_HASHMAP_NOT_FOUND) {
    *ptype = GDI_PROPERTY_TYPE_NULL;
    free(name_cut);
    return GDI_SUCCESS;
  }

  // the hash map points to a node element, so we need to determine the property type
  GDA_Node** node_ptr = GDA_hashmap_get_at(name_to_address, (size_t)pos);
  GDA_Node* node = *node_ptr;
  void** ptr = node->value;
  *ptype = *ptr;

  free(name_cut);

  return GDI_SUCCESS;
}


int GDI_GetNameOfPropertyType( char* name, size_t length, size_t* resultlength,
                               GDI_PropertyType ptype ) {

  if(resultlength == NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(ptype == GDI_PROPERTY_TYPE_NULL) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if(name == NULL || length == 0) {
    *resultlength = strlen(ptype->name);
    return GDI_SUCCESS;
  }

  size_t name_len = strlen(ptype->name);
  size_t len = MIN(length - 1, name_len );
  strncpy(name, ptype->name, len);

  *resultlength = GDA_truncate_string(name, len);

  if(len < name_len) {
    return GDI_ERROR_TRUNCATE;
  }

  return GDI_SUCCESS;
}


int GDI_GetAllPropertyTypesOfDatabase( GDI_PropertyType array_of_ptypes[],
                                       size_t count, size_t* resultcount, GDI_Database graph_db ) {

  if(graph_db == GDI_DATABASE_NULL) {
    return GDI_ERROR_DATABASE;
  }

  if(resultcount == NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(array_of_ptypes == NULL || count == 0) {
    *resultcount = GDA_list_size(graph_db->ptypes->ptypes);
    return GDI_SUCCESS;
  }

  *resultcount = GDA_list_to_array(graph_db->ptypes->ptypes, array_of_ptypes, count);

  if(*resultcount < GDA_list_size(graph_db->ptypes->ptypes)) {
    return GDI_ERROR_TRUNCATE;
  }

  return GDI_SUCCESS;
}


int GDI_GetEntityTypeOfPropertyType( int* etype, GDI_PropertyType ptype ) {
  if(etype == NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(ptype == GDI_PROPERTY_TYPE_NULL) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  *etype = ptype->etype;

  return GDI_SUCCESS;
}


int GDI_GetDatatypeOfPropertyType( GDI_Datatype* dtype, GDI_PropertyType ptype ) {
  if(dtype == NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(ptype == NULL) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  *dtype = ptype->dtype;
  return GDI_SUCCESS;
}


int GDI_GetSizeLimitOfPropertyType( int* stype, size_t* count, GDI_PropertyType ptype ) {
  if(stype == NULL || count == NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(ptype == GDI_PROPERTY_TYPE_NULL) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  *stype = ptype->stype;
  *count = ptype->count;

  return GDI_SUCCESS;
}
