// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#ifndef __GDI_PROPERTY_H_
#define __GDI_PROPERTY_H_

#include "gdi_datatype.h"
#include "gda_hashmap.h"
#include "gda_list.h"

/**
  This header provides functions for GDI_PropertyType.

  A GDI_PropertyType is a pointer to a struct (GDI_PropertyType_desc_t)
  consisting of the property type name, an integer handle (since we will
  store only an integer on objects such as vertices to represent the
  label), a reference to the associated database, the entity type, the
  datatype, the type of the size limitation, and the number of elements.

  The struct, GDI_PropertyType_db_t, is created for each database object
  and manages all associated property types. It consists of a list that
  holds the property types (to be precise, each node holds a pointer to
  a pointer that points to the property type), a hash map that allows to
  translate the integer handle to the node of said list, and a further
  hash map that allows to translate the name of a property type to the
  node of said list. Further, a counter is used to keep track of the
  integer (the integer handle) that is associated with a property type.
*/


/**
  data type definitions
 */

/**
  struct that holds all required data structures for a single GDI
  database instance
 */
typedef struct GDI_PropertyType_db {
  /**
    each list elements contains a pointer that points to the property
    type struct
   */
  GDA_List* ptypes;
  /**
    translates an integer property type handle to pointers that point
    to the elements in the list "ptypes"
   */
  GDA_HashMap* handle_to_address;
  /**
    translates the property type name to pointers that point to
    elements in the list "ptypes"
   */
  GDA_HashMap* name_to_address;
  /**
    code of GDA_LinearScanningNumPropertyTypes relies on the information,
    that this member is uint32_t; in case of any changes, please update
    that code as well
   */
  uint32_t ptype_max;
} GDI_PropertyType_db_t;


/**
  struct that represents a single property type
 */
typedef struct GDI_PropertyType_desc {
  /**
    reference to the graph database struct
   */
  void* db;
  /**
    the character string which is remembered as the name (string)
   */
  char* name;
  /**
    entity type (state)
   */
  uint8_t etype;
  /**
    datatype object (handle)
   */
  GDI_Datatype dtype;
  /**
    type of the size limitation (state)
   */
  uint8_t stype;
  /**
    number of elements (positive integer)
   */
  size_t count;
  /**
    the property type handle as integer
   */
  uint32_t int_handle;
} GDI_PropertyType_desc_t;


/**
  the GDI property type handle
 */
typedef GDI_PropertyType_desc_t* GDI_PropertyType;

/**
  predefined property types
 */
GDI_PropertyType GDI_PROPERTY_TYPE_ID;
GDI_PropertyType GDI_PROPERTY_TYPE_DEGREE;
GDI_PropertyType GDI_PROPERTY_TYPE_INDEGREE;
GDI_PropertyType GDI_PROPERTY_TYPE_OUTDEGREE;

#endif // __GDI_PROPERTY_H_
