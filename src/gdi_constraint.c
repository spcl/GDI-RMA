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
  Convenience function to create a subconstraint. A subconstraint might be created and associated with a database,
  or just be created without association. This function includes both and takes a flag (register_at_db) to
  distinguish these two cases.
 */
inline void create_subconstraint( GDI_Database graph_db, GDI_Subconstraint* subconstraint, int register_at_db ) {
  // allocate memory and copy the values
  (*subconstraint) = malloc(sizeof(GDI_Subconstraint_desc_t));
  assert(*subconstraint != NULL);
  (*subconstraint)->stale = GDA_SUBCONSTRAINT_VALID;
  (*subconstraint)->constraint = NULL;
  (*subconstraint)->db = graph_db;

  GDA_list_create(&((*subconstraint)->label_conditions), sizeof(void*));
  GDA_list_create(&((*subconstraint)->property_conditions), sizeof(void*));

  // register the subconstraint at the database (if needed)
  if(register_at_db == GDI_TRUE) {
    GDA_Node* node = GDA_list_push_back(graph_db->constraints->subconstraints, subconstraint);
    assert(node != NULL);
    (*subconstraint)->node = node;
  }
  else {
    (*subconstraint)->node = NULL;
  }
}


/**
  Convenience function to copy a subconstraint. This might be needed when one wants to know what
  subconstraints are attached to a constraint (then it also requires registering them at the database),
  further it is required when one adds a subconstraint to a constraint (then there is no need
  to register the new subconstraint at the database).
 */
static inline GDI_Subconstraint subconstraint_copy(GDI_Subconstraint subconstraint, int register_at_db) {
  // create a copy of the subconstraint
  GDI_Subconstraint res;
  create_subconstraint(subconstraint->db, &res, register_at_db);
  res->stale = subconstraint->stale;

  // copy all label conditions
  {
    GDA_List* lcond = subconstraint->label_conditions;
    GDA_Node* lcond_node = GDA_list_front(lcond);
    while( lcond_node != NULL) {
      void** ptr = GDA_list_value(lcond, lcond_node);
      GDI_LabelCondition_desc_t* cond = *ptr;
      GDI_AddLabelConditionToSubconstraint(cond->label, cond->op, res);
      lcond_node = GDA_list_next(lcond, lcond_node);
    }
  }

  // copy all property conditions
  {
    GDA_List* pcond = subconstraint->property_conditions;
    GDA_Node* pcond_node = GDA_list_front(pcond);
    while ( pcond_node != NULL) {
      void** ptr = GDA_list_value(pcond, pcond_node);
      GDI_PropertyCondition_desc_t* cond = *ptr;
      GDI_AddPropertyConditionToSubconstraint(cond->ptype, cond->op, cond->data, cond->nelems, res);
      pcond_node = GDA_list_next(pcond, pcond_node);
    }
  }

  return res;
}


int GDI_CreateConstraint( GDI_Database graph_db, GDI_Constraint* constraint ) {
  if(graph_db == GDI_DATABASE_NULL) {
    return GDI_ERROR_DATABASE;
  }

  if(constraint == GDI_CONSTRAINT_NULL) {
    return GDI_ERROR_BUFFER;
  }

  // create the data structure and assign the values
  (*constraint) = malloc(sizeof(GDI_Constraint_desc_t));
  assert(constraint != NULL);
  (*constraint)->stale = GDA_CONSTRAINT_VALID;
  (*constraint)->db = graph_db;

  // register the constraint at the database
  GDA_list_create( &((*constraint)->subconstraints), sizeof(void*));
  GDA_Node* node = GDA_list_push_back(graph_db->constraints->constraints, constraint);
  assert(node != NULL);
  (*constraint)->node = node;

  return GDI_SUCCESS;
}


int GDI_FreeConstraint( GDI_Constraint* constraint ) {
  if(constraint == NULL || *constraint == GDI_CONSTRAINT_NULL) {
    return GDI_ERROR_CONSTRAINT;
  }

  GDI_Database db = (*constraint)->db;

  // remove the association to the database
  GDA_list_erase_single( db->constraints->constraints, (*constraint)->node );

  // remove all subconstraints
  GDA_List* subconstraints = (*constraint)->subconstraints;
  GDA_Node* cond_node = GDA_list_front(subconstraints);
  while( cond_node  != NULL) {
    void** ptr = GDA_list_value(subconstraints, cond_node);
    cond_node = GDA_list_next(subconstraints, cond_node);
    GDI_Subconstraint cond = *ptr;
    GDI_FreeSubconstraint(&cond);
  }

  GDA_list_free(&((*constraint)->subconstraints));
  free(*constraint);
  (*constraint) = GDI_CONSTRAINT_NULL;
  return GDI_SUCCESS;
}


int GDI_GetAllConstraintsOfDatabase( GDI_Constraint array_of_constraints[],
                                      size_t count, size_t* resultcount, GDI_Database graph_db ) {
  if(graph_db == GDI_DATABASE_NULL) {
    return GDI_ERROR_DATABASE;
  }

  if(resultcount == NULL) {
    return GDI_ERROR_BUFFER;
  }

  // case for the size trick
  if(array_of_constraints == NULL || count == 0) {
    *resultcount = GDA_list_size( graph_db->constraints->constraints );
    return GDI_SUCCESS;
  }

  *resultcount = GDA_list_to_array(graph_db->constraints->constraints, array_of_constraints, count);
  if(*resultcount < GDA_list_size(graph_db->constraints->constraints)) {
    return GDI_ERROR_TRUNCATE;
  }

  return GDI_SUCCESS;
}


int GDI_IsConstraintStale( int* staleness, GDI_Constraint constraint ) {
  if(constraint == GDI_CONSTRAINT_NULL) {
    return GDI_ERROR_CONSTRAINT;
  }

  if(staleness == NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(constraint->stale == GDA_CONSTRAINT_STALE ) {
    *staleness = GDI_TRUE;
  }
  else {
    *staleness = GDI_FALSE;
  }

  return GDI_SUCCESS;
}


int GDI_CreateSubconstraint( GDI_Database graph_db, GDI_Subconstraint* subconstraint ) {
  if(subconstraint == GDI_CONSTRAINT_NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(graph_db == GDI_DATABASE_NULL) {
    return GDI_ERROR_DATABASE;
  }

  // call the common function that creates the subconstraint and associates
  // it with the graph database
  create_subconstraint(graph_db, subconstraint, GDI_TRUE);

  return GDI_SUCCESS;
}


int GDI_FreeSubconstraint( GDI_Subconstraint* subconstraint ) {
  if(subconstraint == NULL || *subconstraint == GDI_SUBCONSTRAINT_NULL) {
    return GDI_ERROR_SUBCONSTRAINT;
  }

  {
    // remove all property conditions
    GDA_List* pconds = (*subconstraint)->property_conditions;
    GDA_Node* pcond_node = GDA_list_front( pconds ) ;
    while( pcond_node != NULL) {
      void** ptr = GDA_list_value(pconds, pcond_node);
      GDI_PropertyCondition_desc_t* pcond = *ptr;

      // first get the next node, since free_property_condition removes the element from the list
      pcond_node = GDA_list_next(pconds, pcond_node);
      GDA_free_property_condition((*subconstraint)->db, &pcond);
    }
  }

  {
    // remove all label conditions
    GDA_List* lconds = (*subconstraint)->label_conditions;
    GDA_Node* lcond_node = GDA_list_front(lconds);
    while( lcond_node != NULL) {
      void** ptr = GDA_list_value(lconds, lcond_node);
      GDI_LabelCondition_desc_t* lcond = *ptr;

      // first get the next node, since free_label_condition removes the element from the list
      lcond_node = GDA_list_next(lconds, lcond_node);
      GDA_free_label_condition((*subconstraint)->db, &lcond);
    }
  }

  // remove the association with the db
  if((*subconstraint)->node != NULL) {
    GDA_list_erase_single(((GDI_Database)(*subconstraint)->db)->constraints->subconstraints, (*subconstraint)->node);
  }

  // note: no need to remove the assocation with the constraint:
  // if the subconstraint is associated and the subconstraint gets freed
  // then the constraint gets freed too, and accordingly the entry is removed as well

  GDA_list_free(&( (*subconstraint)->label_conditions));
  GDA_list_free( &( (*subconstraint)->property_conditions));
  free(*subconstraint);
  *subconstraint = GDI_SUBCONSTRAINT_NULL;

  return GDI_SUCCESS;
}


int GDI_GetAllSubconstraintsOfDatabase(GDI_Subconstraint array_of_subconstraints[], size_t count,
                                       size_t* resultcount, GDI_Database graph_db ) {
  if(graph_db == GDI_DATABASE_NULL) {
    return GDI_ERROR_DATABASE;
  }

  if(resultcount == NULL) {
    return GDI_ERROR_BUFFER;
  }

  // case for the size trick
  if(array_of_subconstraints == NULL || count == 0) {
    *resultcount = GDA_list_size( graph_db->constraints->subconstraints );
    return GDI_SUCCESS;
  }

  *resultcount = GDA_list_to_array(graph_db->constraints->subconstraints, array_of_subconstraints, count);
  if(*resultcount < GDA_list_size(graph_db->constraints->subconstraints) ) {
    return GDI_ERROR_TRUNCATE;
  }

  return GDI_SUCCESS;
}


int GDI_IsSubconstraintStale( int* staleness, GDI_Subconstraint subconstraint ) {
  if(subconstraint == GDI_SUBCONSTRAINT_NULL) {
    return GDI_ERROR_SUBCONSTRAINT;
  }

  if(staleness == NULL) {
    return GDI_ERROR_BUFFER;
  }

  if( subconstraint->stale == GDA_SUBCONSTRAINT_STALE ) {
    *staleness = GDI_TRUE;
  }
  else {
    *staleness = GDI_FALSE;
  }

  return GDI_SUCCESS;
}


int GDI_AddSubconstraintToConstraint( GDI_Subconstraint subconstraint,
                                      GDI_Constraint constraint ) {
  if(subconstraint == GDI_SUBCONSTRAINT_NULL) {
    return GDI_ERROR_SUBCONSTRAINT;
  }

  if(constraint == GDI_CONSTRAINT_NULL) {
    return GDI_ERROR_CONSTRAINT;
  }

  if(subconstraint->db != constraint->db) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  int staleness = 0;
  int status = GDI_IsConstraintStale(&staleness, constraint);
  // TODO: maybe we don't have to check this (also below)
  if( status != GDI_SUCCESS) {
    return GDI_ERROR_ARGUMENT;
  }
  if(staleness == GDI_TRUE) {
    return GDI_ERROR_STALE;
  }

  status = GDI_IsSubconstraintStale(&staleness, subconstraint);
  if(status != GDI_SUCCESS) {
    return GDI_ERROR_ARGUMENT;
  }
  if(staleness == GDI_TRUE) {
    return GDI_ERROR_STALE;
  }

  // create a copy of the subconstraint, including all conditions
  // the subconstraint will not be registered at the graph database
  GDI_Subconstraint copy = subconstraint_copy(subconstraint, GDI_FALSE);
  copy->constraint = constraint;

  // add the subconstraint to the list
  GDA_list_push_back(constraint->subconstraints, &copy);

  // note: we do not need to store on the subconstraint a reference to the
  // list of subconstraints that the associated constraint holds,
  // since the subconstraint will only be removed when
  // the constraint will also be removed - so the according entry will be
  // removed anyway

  return GDI_SUCCESS;
}


int GDI_GetAllSubconstraintsOfConstraint( GDI_Subconstraint array_of_subconstraints[],  size_t count,
                                          size_t* resultcount, GDI_Constraint constraint ) {
  if(resultcount == NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(constraint == GDI_CONSTRAINT_NULL) {
    return GDI_ERROR_CONSTRAINT;
  }

  // case for the size trick
  size_t size = GDA_list_size(constraint->subconstraints);
  if(count == 0 || array_of_subconstraints == NULL) {
    *resultcount = size;
    return GDI_SUCCESS;
  }

  // create copies of the subconstraints
  // and also register them at the graph database
  GDA_List* subconstraints = constraint->subconstraints;
  GDA_Node* cond_node;
  for(cond_node  = GDA_list_front(subconstraints), *resultcount = 0;
      *resultcount < count && cond_node != NULL; (*resultcount)++, cond_node = GDA_list_next(subconstraints, cond_node)) {
    void** ptr = GDA_list_value(subconstraints, cond_node);
    GDI_Subconstraint cond = *ptr;
    array_of_subconstraints[*resultcount] = subconstraint_copy(cond, GDI_TRUE);
  }

  if(*resultcount < size) {
    return GDI_ERROR_TRUNCATE;
  }

  return GDI_SUCCESS;
}


int GDI_AddLabelConditionToSubconstraint( GDI_Label label, GDI_Op op, GDI_Subconstraint subconstraint ) {
  if(label == GDI_LABEL_NULL) {
    return GDI_ERROR_LABEL;
  }

  if(subconstraint == GDI_SUBCONSTRAINT_NULL) {
    return GDI_ERROR_SUBCONSTRAINT;
  }

  int staleness = 0;
  int status = GDI_IsSubconstraintStale(&staleness, subconstraint);
  if(status != GDI_SUCCESS) {
    return GDI_ERROR_ARGUMENT;
  }
  if(staleness == GDI_TRUE) {
    return GDI_ERROR_STALE;
  }

  if(!GDA_IsOpValid(op)) {
    return GDI_ERROR_OP;
  }

  if( !(op == GDI_EQUAL || op == GDI_NOTEQUAL)) {
    return GDI_ERROR_OP_DATATYPE_MISMATCH;
  }

  if(label->db != subconstraint->db && label != GDI_LABEL_NONE) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  // create a new struct and copy the values
  GDI_LabelCondition_desc_t* lcond = malloc(sizeof( GDI_LabelCondition_desc_t) );
  assert(lcond != NULL);
  lcond->op = op;
  lcond->label = label;
  lcond->cond.subconstraint = subconstraint;

  // add the label condition to the hash map that is used to remove
  // the label conditions/mark the constraints+subconstraints as stale.
  // the hashmap maps to a list of conditions - so we need to check first
  // if there is one, if not, we create it
  GDA_HashMap* hashmap = ( (GDI_Database)subconstraint->db)->constraints->label_to_condition;
  void** ptr_list = GDA_hashmap_get(hashmap, &label);
  GDA_List* list;
  if(ptr_list == NULL) {
    GDA_list_create(&list, sizeof(void*));
    GDA_hashmap_insert(hashmap, &label, &list);
  }
  else {
    list = *ptr_list;
  }

  // add the reference to the list and node to the condition
  GDA_Node* node = GDA_list_push_back(list, &lcond);
  lcond->cond.hm_node = node;
  lcond->cond.hm_list = list;

  // add the reference to the node to the condition
  node = GDA_list_push_back(subconstraint->label_conditions, &lcond);
  lcond->cond.subc_node = node;

  return GDI_SUCCESS;
}


int GDI_GetAllLabelConditionsFromSubconstraint( GDI_Label array_of_labels[],
                                                GDI_Op array_of_ops[], size_t count, size_t* resultcount,
                                                GDI_Subconstraint subconstraint ) {
  if(resultcount == NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(subconstraint == GDI_SUBCONSTRAINT_NULL) {
    return GDI_ERROR_SUBCONSTRAINT;
  }

  // case for the size trick
  size_t size = GDA_list_size(subconstraint->label_conditions);
  if(array_of_ops == NULL || array_of_labels == NULL || count == 0) {
    *resultcount = size;
    return GDI_SUCCESS;
  }

  // copy all label conditions to the user given array
  GDA_List* lconds = subconstraint->label_conditions;
  GDA_Node* lcond_node;
  for(lcond_node = GDA_list_front(lconds), *resultcount = 0;
      *resultcount < count && lcond_node != NULL; (*resultcount)++, lcond_node = GDA_list_next(lconds, lcond_node)) {
    void** ptr = GDA_list_value(lconds, lcond_node);
    GDI_LabelCondition_desc_t* lcond = *ptr;
    array_of_ops[*resultcount] = lcond->op;
    array_of_labels[*resultcount] = lcond->label;
  }

  if(*resultcount < size) {
    return GDI_ERROR_TRUNCATE;
  }

  return GDI_SUCCESS;
}


int GDI_AddPropertyConditionToSubconstraint( GDI_PropertyType ptype, GDI_Op op, void* value,
                                             size_t count, GDI_Subconstraint subconstraint ) {
  if(subconstraint == GDI_SUBCONSTRAINT_NULL) {
    return GDI_ERROR_SUBCONSTRAINT;
  }

  if(ptype == GDI_PROPERTY_TYPE_NULL) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if(!GDA_IsOpValid(op)) {
    return GDI_ERROR_OP;
  }

  if( (ptype != GDI_PROPERTY_TYPE_ID) && (ptype != GDI_PROPERTY_TYPE_DEGREE) && (ptype != GDI_PROPERTY_TYPE_INDEGREE)
    && (ptype != GDI_PROPERTY_TYPE_OUTDEGREE) && (ptype->db != subconstraint->db) ) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  int staleness;
  int state = GDI_IsSubconstraintStale(&staleness, subconstraint);
  if(state != GDI_SUCCESS) {
    return GDI_ERROR_ARGUMENT;
  }
  if(staleness == GDI_TRUE) {
    return GDI_ERROR_STALE;
  }

  // only return a buffer error, if we should read from the buffer
  if(value == NULL && count > 0) {
    return GDI_ERROR_BUFFER;
  }

  if(!GDA_IsOpAllowedOnDatatype(op, ptype->dtype)) {
    return GDI_ERROR_OP_DATATYPE_MISMATCH;
  }

  if((ptype->stype == GDI_FIXED_SIZE && count != ptype->count ) ||
     (ptype->stype == GDI_MAX_SIZE && count > ptype->count)) {
    return GDI_ERROR_SIZE_LIMIT;
  }

  // allocate memory and copy the values
  GDI_PropertyCondition_desc_t* pcond = malloc(sizeof(GDI_PropertyCondition_desc_t));
  assert(pcond != NULL);
  pcond->op = op;
  pcond->ptype = ptype;
  pcond->nelems = count;
  pcond->cond.subconstraint = subconstraint;

  size_t elem_size;
  int status = GDI_GetSizeOfDatatype(&elem_size, ptype->dtype);
  if(status != GDI_SUCCESS) {
    return GDI_ERROR_ARGUMENT;
  }

  // copy the value
  if(count > 0) {
    pcond->data = malloc(elem_size * count);
    assert(pcond->data != NULL);
    memcpy(pcond->data, value, elem_size * count);
  } else {
    pcond->data = NULL;
  }

  // add an according entry at the hashmap that maps the property type to the list of condition
  // create first the list, if it does not exist
  GDA_HashMap* hashmap = ( (GDI_Database)subconstraint->db)->constraints->property_to_condition;
  void** ptr_list = GDA_hashmap_get(hashmap, &ptype);
  GDA_List* list;
  if(ptr_list == NULL) {
    GDA_list_create(&list, sizeof(void*));
    GDA_hashmap_insert(hashmap, &ptype, &list);
  } else {
    list = *ptr_list;
  }

  // add the data
  GDA_Node* node = GDA_list_push_back(list, &pcond);
  pcond->cond.hm_node = node;
  pcond->cond.hm_list = list;

  // add the property constraint to the subconstraint
  node = GDA_list_push_back(subconstraint->property_conditions, &pcond);
  pcond->cond.subc_node = node;

  return GDI_SUCCESS;
}


int GDI_GetAllPropertyTypesOfSubconstraint( GDI_PropertyType array_of_ptypes[],
                                            size_t count, size_t* resultcount, GDI_Subconstraint subconstraint ) {

  if(subconstraint == GDI_SUBCONSTRAINT_NULL) {
    return GDI_ERROR_SUBCONSTRAINT;
  }

  if(resultcount == NULL) {
    return GDI_ERROR_BUFFER;
  }

  // create a temporary hash map to track the property types we return, since
  // we do not want to return duplicate property types
  GDA_HashMap* hashmap;
  GDA_hashmap_create(&hashmap, sizeof(GDI_PropertyType), 16, sizeof(char), &GDA_int64_to_int);

  // initiate all required data
  GDA_List* pconds = subconstraint->property_conditions;

  *resultcount = 0;
  char c = 1;

  // variable that is 1, when we just need to know the amount of elements stored
  int is_count = array_of_ptypes == NULL || count == 0;

  // iterate over the list of property type conditions
  for(GDA_Node* pcond_node = GDA_list_front(pconds); pcond_node != NULL; pcond_node = GDA_list_next(pconds, pcond_node)) {
    void** ptr = GDA_list_value(pconds, pcond_node);
    GDI_PropertyCondition_desc_t* pcond = *ptr;

    // for each property condition, check if it already is in the hash map
    int32_t pos = GDA_hashmap_find(hashmap, &(pcond->ptype));
    if(pos == GDA_HASHMAP_NOT_FOUND) {

      // if not there, add it not
      pos = GDA_hashmap_insert(hashmap, &(pcond->ptype), &c);
      assert(pos != GDA_HASHMAP_NOT_FOUND);

      // case distinction, if we just want to know the number or really must
      // return the property types
      if(!is_count) {
        // check if we exceed the size of the user provided array
        if(*resultcount < count) {
          array_of_ptypes[*resultcount] = pcond->ptype;
        } else {
          GDA_hashmap_free(&hashmap);
          return GDI_ERROR_TRUNCATE;
        }
      }
      (*resultcount)++;
    }
  }

  GDA_hashmap_free(&hashmap);
  return GDI_SUCCESS;
}


int GDI_GetPropertyConditionsOfSubconstraint( void* buf, size_t buf_count,
                            size_t* buf_resultcount, size_t array_of_offsets[], GDI_Op array_of_ops[],
                            size_t offset_count, size_t* offset_resultcount, GDI_PropertyType ptype,
                            GDI_Subconstraint subconstraint ) {

  if(offset_resultcount == NULL || buf_resultcount == NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(ptype == GDI_PROPERTY_TYPE_NULL) {
    return GDI_ERROR_PROPERTY_TYPE;
  }

  if(subconstraint == GDI_SUBCONSTRAINT_NULL) {
    return GDI_ERROR_SUBCONSTRAINT;
  }

  if(ptype->db != subconstraint->db) {
    return GDI_ERROR_OBJECT_MISMATCH;
  }

  size_t size;
  if(GDI_GetSizeOfDatatype(&size, ptype->dtype) != GDI_SUCCESS) {
    return GDI_ERROR_ARGUMENT;
  }

  // initialize all required values
  GDA_List* pconds = subconstraint->property_conditions;
  *offset_resultcount = 0;
  *buf_resultcount = 0;
  bool buffer_overflow = false;
  bool first_time = true;

  // variable that is 1, when we just need to know the amount of elements/buffer size
  int is_count = buf == NULL || buf_count == 0 || array_of_offsets == NULL || array_of_ops == NULL || offset_count == 0;

  // iterate over the list of property type conditions
  for( GDA_Node* pcond_node = GDA_list_front(pconds) ; pcond_node != NULL; pcond_node = GDA_list_next(pconds, pcond_node)) {
    void** ptr = GDA_list_value(pconds, pcond_node);
    GDI_PropertyCondition_desc_t* pcond = *ptr;

    // filter those that we actually want
    if( pcond->ptype == ptype ) {

      // case distinction if we need to add data to the buffers
      if(!is_count) {
        if( first_time ) {
          array_of_offsets[0] = 0;
          /**
            handle the special case of offset_count = 1

            if offset_count > 1, then we will write to array_of_ops[0] twice
           */
          array_of_ops[0] = pcond->op;

          first_time = false;
        }

        /**
          check the overflow state separately for each buffer,
          so that we can still to fill the other
         */
        bool overflow = false;
        if( buf_count >= (*buf_resultcount + pcond->nelems) ) {
          if(pcond->data) {
            memcpy((char*)buf + size * (*buf_resultcount), pcond->data, pcond->nelems * size);
          }
          *buf_resultcount += pcond->nelems;
        } else {
          overflow = true;
          buffer_overflow = true;
        }

        if( *offset_resultcount+1 < offset_count ) {
          array_of_ops[*offset_resultcount] = pcond->op;
          array_of_offsets[*offset_resultcount + 1] = array_of_offsets[*offset_resultcount] + pcond->nelems;
          (*offset_resultcount)++;
        } else {
          if( overflow ) {
            /**
              both buffers are not able to fit everything in,
              so we can stop traversing the list
             */
            (*offset_resultcount)++;
            return GDI_ERROR_TRUNCATE;
          }
          buffer_overflow = true;
        }
      } else {
        /**
          size trick path
         */
        *buf_resultcount = *buf_resultcount + pcond->nelems;
        (*offset_resultcount)++;
      }
    }
  }

  if( buffer_overflow ) {
    /**
      overflow in one of the buffers, but not both of them
     */
    (*offset_resultcount)++;
    return GDI_ERROR_TRUNCATE;
  }

  if( *offset_resultcount > 0 ) {
    (*offset_resultcount)++;
  }

  return GDI_SUCCESS;
}
