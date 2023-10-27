// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#ifndef __GDA_CONDITION_H_
#define __GDA_CONDITION_H_


/**
  constant definitions
 */

// TODO: Maybe use just two flags: GDA_STALE and GDA_VALID
// (or named differently: GDA_OBJECT_STALE and GDA_OBJECT_VALID)
#define GDA_CONSTRAINT_VALID 0
#define GDA_CONSTRAINT_STALE 1
#define GDA_SUBCONSTRAINT_VALID 0
#define GDA_SUBCONSTRAINT_STALE 1


/**
  function prototypes
 */

/**
  Convenience function to free a label condition: it erases itself form
  the list that the subconstraint holds, erases itself from the list to
  which the hash map points to, and frees itself up.
 */
void GDA_free_label_condition( GDI_Database graph_db, GDI_LabelCondition_desc_t** lcond);

/**
  Convenience function to free a property type condition: it erases
  itself from the list that the subconstraint holds, erases itself
  from the list to which the hash map points to, and frees itself up
 */
void GDA_free_property_condition( GDI_Database graph_db, GDI_PropertyCondition_desc_t** pcond );

/**
   This NOD function can be called to mark all constraints and
   subconstraints as stale that have the given lable in one of their
   conditions (or transitively for the constraints). Further, the
   function removes all conditions that are associated with said label
   (including the according allocated resources).
 */
int GDA_MarkStaleByLabel( GDI_Label label );

/**
  This NOD function can be called to mark all constraints and
  subconstraints as stale that have the given property type in one of
  their conditions (or transitively for the constraints). Further, the
  function removes all conditions that are associated with said property
  type (including the according allocated resources).
 */
int GDA_MarkStaleByPropertyType( GDI_PropertyType ptype );

/**
  This NOD function frees all constraints and the according attached
  resources (such as its subconstraints and conditions) that are
  associated with the given database.
 */
int GDA_FreeAllConstraint( GDI_Database graph_db );

/**
  This NOD function frees all subconstraints and the according attached
  resources (such as its conditions) that are associated with the given
  database.
 */
int GDA_FreeAllSubconstraint( GDI_Database graph_db );

/**
  This NOD function can be used to evaluate constraints in the labeled
  lightweight edge context.

  - assumes that constraint is a valid object
  - list_flag and list are only significant, if list_size > 0
  - list has to be freed by the caller, if list_size > 0

  - returns GDI_ERROR_CONSTRAINT, if the constraint object contains any
    property conditions
  - returns GDI_SUCCESS otherwise
 */
int GDA_EvalConstraintInLightweightEdgeContext( uint8_t** list, size_t* list_size, bool* list_flag, GDI_Constraint constraint );

#endif // __GDA_CONDITION_H_
