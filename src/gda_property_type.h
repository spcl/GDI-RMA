// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#ifndef __GDA_PROPERTY_TYPE_H
#define __GDA_PROPERTY_TYPE_H

/**
  function prototypes
 */

/**
  This function removes all property types associated to the graph
  database. It is a convenience function that can be called when
  the full database gets destroyed.
  It does NOT:
  - remove the property type from associated objects (e.g. edges,
    vertices, constraints, subconstraints)
  - remove the reference to the property types from all data
    structures, meaning, that lists and hash maps still contain
    references to the property types.
 */
int GDA_FreeAllPropertyType(GDI_Database graph_db);

/**
  Convencience function to get the integer handle from a given property
  type.
 */
int GDA_PropertyTypeToIntHandle(GDI_PropertyType ptype, uint32_t* handle);

/**
  Convenience function that returns the property type handle to some
  given integer handle. This function can be used for example when you
  read a property type form some object (e.g. a vertex) and want to get
  the actual handle of the property type.
 */
int GDA_IntHandleToPropertyType(GDI_Database graph_db, uint32_t int_handle, GDI_PropertyType *ptype);

#endif // __GDA_PROPERTY_TYPE_H
