// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#ifndef __GDI_CONDITION_H_
#define __GDI_CONDITION_H_

#include "gdi_label.h"
#include "gdi_operation.h"
#include "gdi_property_type.h"

/**
  This header provides functions for GDI_Constraint and
  GDI_Subconstraint. A single graph database holds data structures to
  manage these objects. Every graph database holds the struct
  GDI_Constraint_desc_t, it is used to manage all constraints,
  subconstraints, label and property type conditions.
  The struct consists of:
    - A hashmap that maps a label handle to a label condition. This is
      used when one wants to remove a label from the graph database and
      wants to mark the GDI_Constraint and GDI_Subconstraint as stale
      (and remove the according conditions)
    - A hash map that maps a property type handle to a property
      condition. It is used when one wants to remove a property type
      from the graph database and wants to mark the GDI_Constraint and
      GDI_Subconstraint as stale (and remove the according conditions).
    - A list that holds all GDI_Constraint that are registered at the
      graph database. This is needed to keep track of the registered
      GDI_Constraint objects.
    - A list that holds all subconstraints that are registered at the
      graph database. This is needed to keep track of the registered
      GDI_Subconstraint objects.

  A GDI_Constraint is a pointer to a struct (GDI_Constraint_desc_t)
  consisting of:
    - A list of subconstraints that are assigned to the constraint. The
      list is used to know what subconstraints are assigned.
    - A node reference (acting similar as an iterator) that points to
      the list that holds all constraints that are registered at the
      database. This reference allows to efficiently remove the
      according entry in the list.
    - A reference to the graph database. That way one can efficiently
      check if objects belong to the same database.
    - A flag indicating if the constraint is stale or not.

  A GDI_Subconstraint is a pointer to a struct (GDI_Subconstraint_desc_t) consisting of:
    - A list of label conditions (see below). It is needed to track the
      associated label conditions.
    - A list of property type conditions (see below). It is needed to
      track the associated property type conditions.
    - The handle of the constraint (to which the subconstraint might
      belong). Needed to mark the constraint as stale, when the
      subconstraint becomes stale.
    - A pointer to the GDI graph database, to which the subconstraint
      belongs. That way one can determine if two objects belong to the
      same graph database efficiently.
    - A flag indicating if the subconstraint is stale or not
    - A node reference (acting similar as an iterator) that points to
      the list that holds all subconstraints that are registered at the
      database. It is needed to remove the according entry in the list
      efficently.

  A label condition and property type condition share some concepts.
  These are the following:
    - A pointer to a list - this is the list to which the hash map maps
      the associated property type or label. It is used in conjunction
      with a node pointer (see below) to remove the constraint from the
      list to which the hash map translates. This information is used
      when the condition is removed by freeing the associated
      subconstraint.
    - A node pointer that points to the precise location in the list to
      which the said hash map translates to. The pointer is used to
      remove the condition from the list.
    - A further node that points to the list of constraints that the
      subconstraint holds. Similarly, it is used to efficiently remove
      the condition.
    - A reference to the subconstraint, to which the condition is
      associated. It is removed for example when we want to mark the
      subconstraint as stale, since the condition is also stale.

  A label condition consists of the struct GDI_LabelCondition_desc and
  holds the following fields:
    - The label handle that is associated with the condition.
    - The operation that is associated with the condition.

  A property type condition consists of the struct
  GDI_PropertyCondition_desc and holds the following fields:
    - The property type handle that is associated with the condition.
    - The operation that is associated with the condition.
    - The buffer, a pointer to the data that is associated with the
      condition.
    - The number of elements that are stored in the buffer.
*/


/**
  data type definitions
 */

/**
  struct that holds all required data structures for a single GDI
  database instance
 */
typedef struct GDI_Constraint_db {
  /**
    maps a label to a list of label conditions
   */
  GDA_HashMap* label_to_condition;
  /**
    maps a property type to a list of property type conditions
   */
  GDA_HashMap* property_to_condition;
  /**
    contains all constraints
   */
  GDA_List* constraints;
  /**
    contains all subconstraints
   */
  GDA_List* subconstraints;
} GDI_Constraint_db_t;


typedef struct GDI_Constraint_desc {
  GDA_List* subconstraints;
  /**
    the node in db->constraints->constraints
   */
  GDA_Node* node;
  void* db;
  uint8_t stale;
} GDI_Constraint_desc_t;

/**
  constraint handle
 */
typedef GDI_Constraint_desc_t* GDI_Constraint;

typedef struct GDI_Subconstraint_desc {
  GDA_List* label_conditions;
  GDA_List* property_conditions;
  GDI_Constraint constraint;
  /**
    the node in db->constraint->constraints->subconstraints
   */
  GDA_Node* node;
  void* db;
  uint8_t stale;
} GDI_Subconstraint_desc_t;

/**
  subconstraint handle
 */
typedef GDI_Subconstraint_desc_t* GDI_Subconstraint;

/**
  the common struct for label and property conditions
 */
typedef struct GDI_Condition_desc {
  /**
    the list that holds the condition in the
    label_property_to_condition hash map
   */
  GDA_List* hm_list;
  /**
    the node in the list of the hash map
   */
  GDA_Node* hm_node;
  /**
    the node in the list of the subconstraint
   */
  GDA_Node* subc_node;
  /**
    the subconstraint that holds this property condition
   */
  GDI_Subconstraint subconstraint;
} GDI_Condition_desc_t;

/**
  the struct for the property condition
 */
typedef struct GDI_PropertyCondition_desc {
  GDI_Condition_desc_t cond;
  GDI_PropertyType ptype;
  GDI_Op op;
  void* data;
  size_t nelems;
} GDI_PropertyCondition_desc_t;

/**
  the struct for the label condition
 */
typedef struct GDI_LabelCondition_desc {
  GDI_Condition_desc_t cond;
  GDI_Label label;
  GDI_Op op;
} GDI_LabelCondition_desc_t;

#endif // __GDI_CONDITION_H_
