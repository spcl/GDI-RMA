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
#include "gda_operation.h"

/**
  Convenience function to free a property type condition: it erases itself from the list that the subconstraint holds,
  erases itself from the list to which the hash map points to, and frees itself up.
 */
void GDA_free_property_condition( GDI_Database graph_db, GDI_PropertyCondition_desc_t** pcond ) {
  if(pcond != NULL && *pcond != NULL) {
    // erase the node from the list of the subconstraint
    GDA_list_erase_single( (*pcond)->cond.subconstraint->property_conditions, (*pcond)->cond.subc_node);

    // erase the node from the list, to which the hashmap points
    GDA_list_erase_single( (*pcond)->cond.hm_list, (*pcond)->cond.hm_node);

    // remove the list from the hashmap, if the list is empty
    if(GDA_list_size( ((*pcond)->cond.hm_list)) == 0) {
      GDA_list_free( &( ((*pcond)->cond.hm_list) ) );
      GDA_hashmap_erase(graph_db->constraints->property_to_condition, &((*pcond)->ptype));
    }

    if((*pcond)->data) {
      free((*pcond)->data);
    }
    free(*pcond);
    *pcond = NULL;
  }
}


/**
  Convenience function to free a label condition: it erases itself form the list that the subconstraint holds,
  erases itself from the list to which the hash map points to, and frees itself up.
 */
void GDA_free_label_condition( GDI_Database graph_db, GDI_LabelCondition_desc_t** lcond) {
  if(lcond != NULL && *lcond != NULL) {
    // erase the node from the list of the subconstraint
    GDA_list_erase_single( (*lcond)->cond.subconstraint->label_conditions, (*lcond)->cond.subc_node);

    // erase the node from the list, to which the hashmap points
    GDA_list_erase_single( (*lcond)->cond.hm_list, (*lcond)->cond.hm_node);

    // remove the list from the hashmap, if the list is empty
    if(GDA_list_size( (*lcond)->cond.hm_list) == 0) {
      GDA_list_free( &( (*lcond)->cond.hm_list));
      GDA_hashmap_erase(graph_db->constraints->label_to_condition, &( (*lcond)->label));
    }

    free(*lcond);
    *lcond = NULL;
  }
}


/**
  This NOD function frees all constraints and the according attached resources
  (such as its subconstraints and conditions) that are associated with the given database.
 */
int GDA_FreeAllConstraint(GDI_Database graph_db) {
  if(graph_db == GDI_DATABASE_NULL) {
    return GDI_ERROR_DATABASE;
  }
  GDA_List* cons = graph_db->constraints->constraints;
  GDA_Node* con_node = GDA_list_front(cons);
  while( con_node  != NULL) {
    void** ptr = GDA_list_value(cons, con_node);
    con_node = GDA_list_next(cons, con_node);
    GDI_Constraint con = *ptr;
    GDI_FreeConstraint(&con);
  }

  return GDI_SUCCESS;
}


/**
  This NOD function frees all subconstraints and the according attached resources
  (such as its conditions) that are associated with the given database.
 */
int GDA_FreeAllSubconstraint(GDI_Database graph_db) {
  if(graph_db == GDI_DATABASE_NULL) {
    return GDI_ERROR_DATABASE;
  }
  GDA_List* scons = graph_db->constraints->subconstraints;
  GDA_Node* scon_node = GDA_list_front(scons);
  while( scon_node != NULL) {
    void** ptr = GDA_list_value(scons, scon_node);
    scon_node = GDA_list_next(scons, scon_node);
    GDI_Subconstraint con = *ptr;
    GDI_FreeSubconstraint(&con);
  }

  return GDI_SUCCESS;
}


/**
  This NOD function can be called to mark all constraints and subconstraints as stale
  that have the given lable in one of their conditions (or transitively for the constraints).
  Further, the function removes all conditions that are associated with said label
  (including the according allocated resources).
 */
int GDA_MarkStaleByLabel( GDI_Label label ) {
  if(label == GDI_LABEL_NULL || label == GDI_LABEL_NONE) {
    return GDI_ERROR_LABEL;
  }

  void** ptr_label_list = GDA_hashmap_get( ((GDI_Database) label->db)->constraints->label_to_condition, &label);
  if(ptr_label_list == NULL) {
    return GDI_SUCCESS;
  }

  GDA_List* label_list = *ptr_label_list;
  if(label_list == NULL) {
    return GDI_ERROR_INTERN;
  }

  GDA_Node* curr = GDA_list_front(label_list);
  while(curr != NULL) {
    void** ptr_node = GDA_list_value(label_list, curr);
    GDI_LabelCondition_desc_t* desc = *ptr_node;

    // mark subconstraint and constraint as stale
    desc->cond.subconstraint->stale = GDA_SUBCONSTRAINT_STALE;
    if(desc->cond.subconstraint->constraint != NULL) {
      desc->cond.subconstraint->constraint->stale = GDA_CONSTRAINT_STALE;
    }

    curr = GDA_list_next(label_list, curr);

    // free the label and all according relations
    GDA_free_label_condition( (GDI_Database)label->db, &desc );
  }

  return GDI_SUCCESS;
}


/**
  This NOD function can be called to mark all constraints and subconstraints as stale
  that have the given property type in one of their conditions (or transitively for the constraints).
  Further, the function removes all conditions that are associated with said property type
  (including the according allocated resources).
 */
int GDA_MarkStaleByPropertyType( GDI_PropertyType ptype ) {
  if(ptype == GDI_PROPERTY_TYPE_NULL || ptype == GDI_PROPERTY_TYPE_ID ||
     ptype == GDI_PROPERTY_TYPE_DEGREE || ptype == GDI_PROPERTY_TYPE_INDEGREE || ptype == GDI_PROPERTY_TYPE_OUTDEGREE) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  void** ptr_label_list = GDA_hashmap_get( ((GDI_Database) ptype->db)->constraints->property_to_condition, &ptype);
  if(ptr_label_list == NULL) {
    return GDI_SUCCESS;
  }

  GDA_List* label_list = *ptr_label_list;
  if(label_list == NULL) {
    return GDI_ERROR_INTERN;
  }

  GDA_Node* curr = GDA_list_front(label_list);
  while(curr != NULL) {
    void** ptr_node = GDA_list_value(label_list, curr);
    GDI_PropertyCondition_desc_t* desc = *ptr_node;

    // mark subconstraint and constraint as stale
    desc->cond.subconstraint->stale = GDA_SUBCONSTRAINT_STALE;
    if(desc->cond.subconstraint->constraint != NULL) {
      desc->cond.subconstraint->constraint->stale = GDA_CONSTRAINT_STALE;
    }

    curr = GDA_list_next(label_list, curr);

    // free the label and all according relations
    GDA_free_property_condition( (GDI_Database)ptype->db, &desc );
  }

  return GDI_SUCCESS;
}


/**
  helper function for GDA_EvalConstraintInLightweightEdgeContext

  list_ptr can contain either the blacklist or the whitelist
  list_flag:
  - true:  whitelist
  - false: blacklist

  function written by Robert Gerstenberger
 */
static inline void GDA_ParseLabelSubconstraintInLightweightEdgeContext( uint8_t** list_ptr, size_t* list_maxcount, size_t* list_resultcount, bool* list_flag, bool* satisfiable, GDI_Subconstraint sc ) {

  assert( *list_maxcount > 0 );

  int ret;
  size_t resultcount;
  size_t num_conditions = 10;
  GDI_Label* array_of_labels = malloc( num_conditions * sizeof(GDI_Label) );
  GDI_Op* array_of_ops = malloc( num_conditions * sizeof(GDI_Op) );

  uint8_t* list = *list_ptr;

  /**
    extract the label conditions
    resize the two arrays if necessary
   */
  ret = GDI_GetAllLabelConditionsFromSubconstraint( array_of_labels, array_of_ops, num_conditions, &resultcount, sc );
  if( ret == GDI_ERROR_TRUNCATE ) {
    GDI_GetAllLabelConditionsFromSubconstraint( NULL, NULL, 0, &num_conditions, sc );
    array_of_labels = realloc( array_of_labels, num_conditions * sizeof(GDI_Label) );
    array_of_ops = realloc( array_of_ops, num_conditions * sizeof(GDI_Op) );

    ret = GDI_GetAllLabelConditionsFromSubconstraint( array_of_labels, array_of_ops, num_conditions, &resultcount, sc );
  }
  assert( ret == GDI_SUCCESS );

  *satisfiable = true;
  *list_resultcount = 0;

  /**
    parse the label conditions
   */
  if( resultcount > 0 ) {
    /* init list */
    list[0] = array_of_labels[0]->int_handle;
    *list_resultcount = 1;
    if( array_of_ops[0] == GDI_EQUAL ) {
      *list_flag = true; /* mark as whitelist */
    } else {
      *list_flag = false; /* mark as blacklist */
    }
  }

  for( size_t i=1 ; i<resultcount ; i++ ) {
    if( array_of_ops[i] == GDI_EQUAL ) {
      if( !(*list_flag) ) {
        /**
          merge existing blacklist with an equal condition: only have to
          check whether the current label is already present in the blacklist:
          if it is, then is not possible to satisfy the subconstraint
          (edge has to have that label AND edge has to have not that label)
         */
        for( size_t j=0 ; j<(*list_resultcount) ; j++ ) {
          if( list[j] == array_of_labels[i]->int_handle ) {
            *satisfiable = false;

            /* clean up */
            free( array_of_labels );
            free( array_of_ops );
            return;
          }
        }
      } else {
        /**
          merge existing whitelist with an equal condition: only one element allowed
          in a whitelist, since NOD currently only supports one label per edge
         */
        if( array_of_labels[i]->int_handle != list[0] ) {
          /**
            edge can't have two labels in NOD right now: not possible to satisfy
            this subconstraint
           */
          *satisfiable = false;

          /* clean up */
          free( array_of_labels );
          free( array_of_ops );
          return;
        }
      }
    } else {
      /**
        equal to GDI_NOTEQUAL, as labels only support those two GDI_Ops
       */
      if( !(*list_flag) ) {
        /**
          merge blacklist with a non-equal condition: just have to make sure
          that label is not already a part of the blacklist before adding it
          to the blacklist
         */
        bool add_flag = true;
        for( size_t j=0 ; j<(*list_resultcount) ; j++ ) {
          if( list[j] == array_of_labels[i]->int_handle ) {
            add_flag = false;
            break;
          }
        }

        if( add_flag ) {
          if( *list_resultcount == *list_maxcount ) {
            /**
              list (which is an array) is too small for one more element
             */
            *list_maxcount = *list_maxcount << 1;
            *list_ptr = realloc( *list_ptr, *list_maxcount * sizeof(uint8_t) );
            list = *list_ptr;
          }
          list[(*list_resultcount)++] = array_of_labels[i]->int_handle;
        }
      } else {
        /**
          merge whitelist with a non-equal condition: either the whitelist contains
          the same label as the current condition, in which case it is not possible
          to satisfy the subconstraint, or the list will be a blacklist afterwards
         */
        if( array_of_labels[i]->int_handle == list[0] ) {
          *satisfiable = false;

          /* clean up */
          free( array_of_labels );
          free( array_of_ops );
          return;
        } else {
          *list_flag = false; /* mark as blacklist */
          list[0] = array_of_labels[i]->int_handle;
        }
      }
    }
  }

  /* clean up */
  free( array_of_labels );
  free( array_of_ops );
}

/**
  helper function for GDA_EvalConstraintInLightweightEdgeContext

  list_flags decide whether the respective list is either a blacklist or a whitelist
  - true:  whitelist
  - false: blacklist

  function written by Robert Gerstenberger
 */
void GDA_MergeLists( uint8_t* sc_list, size_t num_sc_list, bool sc_list_flag, GDA_List** c_list_ptr, bool* c_list_flag ) {

  if( sc_list_flag ) {
    if( *c_list_flag ) {
      /**
        merge two whitelists
        - subconstraint whitelist is exactly one label
        - check whether that label is already present in the constraint whitelist:
          if not, add that label as well to the constraint whitelist
       */
      assert( num_sc_list == 1 );
      bool add_flag = true;
      GDA_Node* node = (*c_list_ptr)->head;
      while( node != NULL ) {
        if( *(uint8_t*)(node->value) == sc_list[0] ) {
          add_flag = false;
          break;
        }
        node = node->next;
      }
      if( add_flag ) {
        GDA_list_push_back( *c_list_ptr, &(sc_list[0]) );
      }
    } else {
      /**
        merge a subconstraint whitelist with a constraint blacklist
        - subconstraint whitelist is exactly one label
        - check whether that label is already present in the constraint blacklist:
          if so, remove that label from the constraint blacklist
       */
      GDA_Node* node = (*c_list_ptr)->head;
      while( node != NULL ) {
        if( *(uint8_t*)(node->value) == sc_list[0] ) {
          GDA_list_erase_single( *c_list_ptr, node );
          break;
        }
        node = node->next;
      }
    }
  } else {
    if( *c_list_flag ) {
      /**
        merge a subconstraint blacklist with a constraint whitelist
        - will change the constraint whitelist into a blacklist
        - check for each label in the constraint whitelist, whether it is present
          in the subconstraint blacklist: if so, that label is not added to new
          constraint blacklist
       */
      GDA_List* new_c_list;
      GDA_list_create( &new_c_list, sizeof(uint8_t) );

      for( size_t i=0 ; i<num_sc_list ; i++ ) {
        bool add_flag = true;
        GDA_Node* node = (*c_list_ptr)->head;
        while( node != NULL ) {
          if( *(uint8_t*)(node->value) == sc_list[i] ) {
            add_flag = false;
            break;
          }
          node = node->next;
        }

        if( add_flag ) {
          GDA_list_push_back( new_c_list, &(sc_list[i]) );
        }
      }

      /* switch the two lists */
      GDA_list_free( c_list_ptr );
      *c_list_ptr = new_c_list;

      *c_list_flag = false; /* mark as blacklist */
    } else {
      /**
        merge two blacklists
        - check for each label in the the subconstraint blacklist, whether it is
          present in the constraint blacklist: if so, that label is added to a new
          constraint blacklist
       */
      GDA_List* new_c_list;
      GDA_list_create( &new_c_list, sizeof(uint8_t) );

      for( size_t i=0 ; i<num_sc_list ; i++ ) {
        bool add_flag = false;
        GDA_Node* node = (*c_list_ptr)->head;
        while( node != NULL ) {
          if( *(uint8_t*)(node->value) == sc_list[i] ) {
            add_flag = true;
            break;
          }
          node = node->next;
        }

        if( add_flag ) {
          GDA_list_push_back( new_c_list, &(sc_list[i]) );
        }
      }

      /* switch the two lists */
      GDA_list_free( c_list_ptr );
      *c_list_ptr = new_c_list;
    }
  }
}


/**
  This NOD function can be used to evaluate constraints in the labeled lightweight edge context.

  - assumes that constraint is a valid object
  - list_flag and list are only significant, if list_size > 0
  - list has to be freed by the caller, if list_size > 0

  - returns GDI_ERROR_CONSTRAINT, if the constraint object contains any property conditions
  - returns GDI_SUCCESS otherwise

  function written by Robert Gerstenberger
 */
int GDA_EvalConstraintInLightweightEdgeContext( uint8_t** list, size_t* list_size, bool* list_flag, GDI_Constraint constraint ) {

  int ret;
  size_t num_subconstraints;
  GDI_Subconstraint* array_of_subconstraints = malloc( 10 * sizeof(GDI_Subconstraint) );

  /**
    retrieve all subconstraints of the constraint object
   */
  ret = GDI_GetAllSubconstraintsOfConstraint( array_of_subconstraints, 10, &num_subconstraints, constraint );
  if( ret == GDI_ERROR_TRUNCATE ) {
    GDI_GetAllSubconstraintsOfConstraint( NULL, 0, &num_subconstraints, constraint );
    array_of_subconstraints = realloc( array_of_subconstraints, num_subconstraints * sizeof(GDI_Subconstraint) );

    ret = GDI_GetAllSubconstraintsOfConstraint( array_of_subconstraints, num_subconstraints, &num_subconstraints, constraint );
  }
  assert( ret == GDI_SUCCESS );

  /**
    check for any property conditions, which is an error case
   */
  for( size_t i=0 ; i<num_subconstraints ; i++ ) {
    size_t resultcount;

    GDI_GetAllPropertyTypesOfSubconstraint( NULL, 0, &resultcount, array_of_subconstraints[i] );
    if( resultcount > 0 ) {
      free( array_of_subconstraints );
      return GDI_ERROR_CONSTRAINT;
    }
  }

  /**
    prepare a couple of data structures
   */
  /* blacklist of the subconstraint */
  size_t max_num_sc_list = 10;
  uint8_t* sc_list = malloc( max_num_sc_list * sizeof(uint8_t) );

  /* global list of the constraint */
  bool c_list_flag = true; /* indicates, whether c_list is a whitelist or a blacklist: true = whitelist, false = blacklist */
  GDA_List* c_list;
  GDA_list_create( &c_list, sizeof(uint8_t) );

  bool evaluate_something = false;

  /**
    iterate over the subconstraint objects and assemble the global constraint list
   */
  for( size_t i=0 ; i<num_subconstraints ; i++ ) {
    bool sc_list_flag;
    bool satisfiable;
    size_t num_sc_list;
    GDA_ParseLabelSubconstraintInLightweightEdgeContext( &sc_list, &max_num_sc_list, &num_sc_list, &sc_list_flag, &satisfiable, array_of_subconstraints[i] );
    if( satisfiable && (num_sc_list > 0) ) {
      GDA_MergeLists( sc_list, num_sc_list, sc_list_flag, &c_list, &c_list_flag );
    }

    if( GDA_list_size( c_list ) == 0 ) {
      if( evaluate_something ) {
        /* constraint can always be satisfied */
        break;
      }
    } else {
      evaluate_something = true;
    }
  }

  /**
    prepare the output buffers
   */
  *list_size = GDA_list_size( c_list );
  if( *list_size != 0 ) {
    *list = malloc( *list_size * sizeof(uint8_t) );
    GDA_list_to_array( c_list, *list, *list_size );
    *list_flag = c_list_flag;
  }

  /**
    clean up
   */
  free( array_of_subconstraints );
  free( sc_list );
  GDA_list_free( &c_list );

  return GDI_SUCCESS;
}
