// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Roman Haag

#ifndef __GDA_EDGE_UID_H
#define __GDA_EDGE_UID_H

#include "gda_dpointer.h"

/**
  This header provides functions to pack and unpack edge UIDs. An edge
  UID is a data structure that identifies an edge uniquely in the
  database. The data structure consists of two parts:
  - a vertex UID
  - an offset for the edge data structure of the vertex identified by
    the vertex UID
  An edge UID takes up 12 Bytes of memory, where the lower 8 Bytes
  contain the vertex UID and the upper 4 Bytes the offset to the edge.

   -------------------------------------------------
  |     offset    |           vertex UID            |
   -------------------------------------------------
 */

/**
  data type definitions
 */
typedef uint8_t GDA_Edge_uid[12];


/**
  function defintions
 */

/**
  Assembles a 12 Byte GDA_Edge_uid from a 8 Byte DPointer and a 4 Byte
  offset.
 */
static inline void GDA_PackEdgeUid( GDA_DPointer rank, uint32_t offset, GDA_Edge_uid* edge_uid ) {
  GDA_DPointer* rankptr;
  uint32_t* offsetptr;

  offsetptr = (uint32_t*)((*edge_uid)+8);
  rankptr = (GDA_DPointer*)(*edge_uid);
  *offsetptr = offset;
  *rankptr = rank;
}

/**
  Disassembles an edge UID into a 8 Byte DPointer and a 4 Byte offset.
 */
static inline void GDA_UnpackEdgeUid( GDA_DPointer* rank, uint32_t* offset, GDA_Edge_uid edge_uid ) {
  *offset = *(uint32_t*)(edge_uid+8);
  *rank = *(GDA_DPointer*)(edge_uid);
}

#endif // #ifndef __GDA_EDGE_UID_H
