// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#ifndef __GDI_LABEL_H
#define __GDI_LABEL_H

#include "gda_hashmap.h"
#include "gda_list.h"

/**
  This header provides data structures for GDI_Labels. A GDI_Label is a
  pointer to a struct (GDI_Label_desc_t) consisting of the label name,
  an integer handle (as we will store only an integer on objects to
  represent the label), and a reference to the associated database. The
  struct, GDI_Label_db_t, is created for each database object and
  manages all associated labels. It consists of a list that holds the
  labels (to be precise, each node holds a pointer to a pointer that
  points to the label), a hash map that allows to translate the integer
  handle to the node of said list, and a further hash map that allows to
  translate the name of a label to the node of said list. Further, a
  counter is used to keep track of the integer (the integer handle) that
  is associated with a label.
*/


/**
  data type definitions
 */

/**
  struct that holds all required data structures for a single GDI
  database instance
 */
typedef struct GDI_Label_db {
  /**
    each list elements contains a pointer that points to the label
    struct
   */
  GDA_List* labels;
  /**
    translates an integer label handle to pointers that point to the
    elements in the list "labels"
   */
  GDA_HashMap* handle_to_address;
  /**
    translates the label name to pointers that point to elements in
    the list "labels"
   */
  GDA_HashMap* name_to_address;
  uint32_t label_max;
} GDI_Label_db_t;

/**
  struct that represents a single label
 */
typedef struct GDI_Label_desc {
  /**
    the label name (null terminated)
   */
  char* name;
  /**
    the label handle as integer
   */
  uint32_t int_handle;
  /**
    reference to the struct that manages all labels
   */
  void* db;
} GDI_Label_desc_t;

/**
  the GDI label handle
 */
typedef GDI_Label_desc_t* GDI_Label;


/**
  predefined labels
 */
GDI_Label GDI_LABEL_NONE;


/**
  function prototypes

  included in gdi.h to avoid circular dependencies between the two header files
 */

#endif // __GDI_LABEL_H
