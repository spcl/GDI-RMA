// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#include <inttypes.h>
#include <string.h>

#include "gda_list.h"

// TODO: remove this
#ifndef UNUSED
  #define UNUSED(x) (void)(x)
#endif


/**
  Private function to remove elements from a given range.
*/
inline int list_p_free_range(GDA_Node* start, GDA_Node* end) {
  if(start == NULL || end == NULL) {
    return 0;
  }

  int i = 0;
  while(start != end) {
    GDA_Node* tmp_node = start;
    start = start->next;
    free(tmp_node->value);
    free(tmp_node);
    i++;
  }
  free(end->value);
  free(end);
  i++;

  return i;
}


/**
  Private function to create a new node with the given value and element size.
 */
inline GDA_Node* list_p_create_node(void* value, size_t element_size) {
  GDA_Node* node = (GDA_Node*) malloc(sizeof(GDA_Node));
  node->value = (void*)malloc(sizeof(element_size));
  memcpy(node->value, value, element_size);
  return node;
}


/**
  Private function to free a node.
 */
inline void list_p_free_node(GDA_Node* node) {
  if(node) {
    free(node->value);
    free(node);
  }
}


void GDA_list_create(GDA_List** list, int element_size) {
  *list = (GDA_List*) malloc(sizeof(GDA_List));
  (*list)->head = NULL;
  (*list)->tail = NULL;
  (*list)->size = 0;
  (*list)->element_size = element_size;
}


void GDA_list_free(GDA_List** list) {
  GDA_Node* head = (*list)->head;
  GDA_Node* tail = (*list)->tail;

  list_p_free_range(head, tail);

  free(*list);
  *list = NULL;
}


size_t GDA_list_size(GDA_List* list) {
  return list->size;
}


GDA_Node* GDA_list_push_back(GDA_List* list, void* value) {
  GDA_Node* node = list_p_create_node(value, list->element_size);

  if(list->size == 0) {
    list->head = node;
    list->tail = node;
    node->next = NULL;
    node->prev = NULL;
  }
  else {
    node->prev = list->tail;
    list->tail->next = node;
    node->next = NULL;
    list->tail = node;
  }

  list->size++;
  return node;
}


GDA_Node* GDA_list_push_front(GDA_List* list, void* value) {
  GDA_Node* node = list_p_create_node(value, list->element_size);

  if(list->size == 0) {
    list->head = node;
    list->tail = node;
    node->next = NULL;
    node->prev = NULL;
  }
  else {
    node->next = list->head;
    list->head->prev = node;
    node->prev = NULL;
    list->head = node;
  }

  list->size++;

  return node;
}


GDA_Node* GDA_list_insert_before(GDA_List* list, GDA_Node* position, void* value) {
  // we insert at the head
  if(position == list->head) {
    return GDA_list_push_front(list, value);
  }
  else {
    GDA_Node* node = list_p_create_node(value, list->element_size);

    GDA_Node* right = position;
    GDA_Node* left = position->prev;

    left->next = node;
    node->next = right;
    right->prev = node;
    node->prev = left;
    list->size++;

    return node;
  }
}


GDA_Node* GDA_list_insert_after(GDA_List* list, GDA_Node* position, void* value) {
  // we insert at the tail
  if(position == list->tail) {
    return GDA_list_push_back(list, value);
  }
  else {
    GDA_Node* node = list_p_create_node(value, list->element_size);

    GDA_Node* right = position->next;
    GDA_Node* left = position;

    left->next = node;
    node->next = right;
    right->prev = node;
    node->prev = left;
    list->size++;
    return node;
  }
}


void GDA_list_pop_back(GDA_List* list) {
  GDA_Node* tail = list->tail;
  if(list->size == 0) {
    return;
  }
  else if(list->size == 1) {
    list->head = NULL;
    list->tail = NULL;
    list->size = 0;
  }
  else {
    list->tail = list->tail->prev;
    list->tail->next = NULL;
    list->size--;
  }

  list_p_free_node(tail);
}


void GDA_list_pop_front(GDA_List* list) {
  GDA_Node* head = list->head;
  if(list->size == 0) {
    return;
  }
  else if(list->size == 1) {
    list->head = NULL;
    list->tail = NULL;
    list->size = 0;
  }
  else {
    list->head = list->head->next;
    list->head->prev = NULL;
    list->size--;
  }

  list_p_free_node(head);
}


GDA_Node* GDA_list_next(GDA_List* list, GDA_Node* node) {
  UNUSED(list);
  return node->next;
}


GDA_Node* GDA_list_prev(GDA_List* list, GDA_Node* node) {
  UNUSED(list);
  return node->prev;
}


void* GDA_list_value(GDA_List* list, GDA_Node* node) {
  UNUSED(list);
  return node->value;
}


size_t GDA_list_to_array(GDA_List* list, void* array, size_t size_in_elements) {
  GDA_Node* head = list->head;
  size_t element_size = list->element_size;

  size_t i = 0;
  char* char_arr = (char*) array;
  for( ; head != NULL && i < size_in_elements; i++) {
    memcpy(char_arr + i*element_size, head->value, element_size);
    head = head->next;
  }
  return i;
}


GDA_Node* GDA_list_erase_single(GDA_List* list, GDA_Node* node) {
  if(list->head == node) {
    GDA_list_pop_front(list);
    return list->head;
  }
  else if(list->tail == node) {
    GDA_list_pop_back(list);
    return list->tail;
  }
  else {
    GDA_Node* left = node->prev;
    GDA_Node* right = node->next;
    left->next = right;
    right->prev = left;
    list->size--;

    list_p_free_node(node);
    return right;
  }
}


GDA_Node* GDA_list_erase_range(GDA_List* list, GDA_Node* start, GDA_Node* end) {
  if(start == end) {
    return GDA_list_erase_single(list, start);
  }
  else if( list->head == start && list->tail == end ) {
    list_p_free_range(start, end);

    list->head = NULL;
    list->tail = NULL;
    list->size = 0;
    return NULL;
  }
  else if( list->head == start) {
    GDA_Node* end_next = end->next;
    end_next->prev = NULL;
    list->head = end_next;

    int i = list_p_free_range(start, end);
    list->size = list->size - i;
    return list->head;
  }
  else if(list->tail == end) {
    GDA_Node* start_prev = start->prev;
    start_prev->next = NULL;
    list->tail = start_prev;

    int i = list_p_free_range(start, end);
    list->size = list->size - i;
    return list->tail;
  }
  else {
    GDA_Node* start_prev = start->prev;
    GDA_Node* end_next = end->next;
    start_prev->next = end_next;
    end_next->prev = start_prev;

    int i = list_p_free_range(start, end);
    list->size = list->size - i;
    return end_next;
  }
}


void GDA_list_map(GDA_List* list, void (*fun)(void*) ) {
  GDA_Node* head = list->head;
  while(head != NULL) {
    fun( (void*)head);
    head = head->next;
  }
}


GDA_Node* GDA_list_find_node(GDA_List* list, GDA_Node* node) {
  GDA_Node* head = list->head;
  while(head != NULL) {
    if(head == node) {
      return head;
    }
    head = head->next;
  }
  return NULL;
}


GDA_Node* GDA_list_find_value(GDA_List* list, void* value) {
  GDA_Node* head = list->head;
  while(head != NULL) {
    if( memcmp(value, head->value, list->element_size) == 0) {
      return head;
    }
    head = head->next;
  }
  return NULL;
}


GDA_Node* GDA_list_front(GDA_List* list) {
  return list->head;
}


GDA_Node* GDA_list_back(GDA_List* list) {
  return list->tail;
}
