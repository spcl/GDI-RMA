// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#ifndef __GDA_LABEL_H
#define __GDA_LABEL_H

/**
  function prototypes
 */

/**
  This function removes all labels associated to the graph database. It
  is a convenience function that can be called when the full database
  gets destroyed.
  It does NOT:
  - remove the labels from associated objects (e.g. edges, vertices,
    constraints, subconstraints)
  - remove the reference to the labels from all data structures,
    meaning, that lists and hash maps still contain references to the
    labels.
 */
int GDA_FreeAllLabel(GDI_Database graph_db);

/**
  Convencience function to get the integer handle from a given label.
 */
int GDA_LabelToIntHandle(GDI_Label label, uint32_t* handle);

/**
  Convenience function that returns the label handle to some given
  integer handle. This function can be used for example when you read
  a label from some object (e.g. a vertex) and want to get the actual
  handle of the label.
 */
int GDA_IntHandleToLabel(GDI_Database graph_db, uint32_t int_handle, GDI_Label *label);

#endif // __GDA_LABEL_H
