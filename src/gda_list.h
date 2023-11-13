// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#ifndef __GDA_LIST_H
#define __GDA_LIST_H

#include <stdlib.h>

/**
  This header provides functions for a doubly-linked list.
  The list provides an interface similar to std::list.
  Note that data that is stored in the list gets copied.
  Upon retrieval, a pointer to the memory location is returned,
  where the value is stored.
  It provides the following properties:
    - O(1) insert at the head or tail
    - O(1) removal at the head, tail or when given a pointer
    - O(n) destruction (O(1) amortized)
    - O(n) search
*/


/**
  data type definitions
 */
typedef struct GDA_Node {
  struct GDA_Node* next;
  struct GDA_Node* prev;
  void* value;
} GDA_Node;

typedef struct GDA_List {
  struct GDA_Node* head;
  struct GDA_Node* tail;
  /**
    the number of elements
   */
  size_t size;
  /**
    the size of the value a node holds
   */
  size_t element_size;
} GDA_List;


/**
  function prototypes
 */

/**
  Initializes a new list.
  The list should be deleted by calling list_free.
 */
void GDA_list_create(GDA_List** list, int element_size);

/**
  Removes all elements from list, frees them and frees the list itself.
  The list is set to NULL.
 */
void GDA_list_free(GDA_List** list);

/**
  Returns the size of the list (the number of elements stored in the list);
 */
size_t GDA_list_size(GDA_List* list);

/**
  Adds a new element at the end. The value gets copied.
  The node element that stores the element is returned.
 */
GDA_Node* GDA_list_push_back(GDA_List* list, void* value);

/**
  Adds a new element at the front. The value gets copied.
  The node element that stores the element is returned.
 */
GDA_Node* GDA_list_push_front(GDA_List* list, void* value);

/**
  Inserts a new node before the node called position in the list named list.
  The value gets copied.
  The node element that stores the element is returned.
 */
GDA_Node* GDA_list_insert_before(GDA_List* list, GDA_Node* position, void* value);

/**
  Inserts the node element after the node position in the list named list.
  The node element that stores the element is returned.
*/
GDA_Node* GDA_list_insert_after(GDA_List* list, GDA_Node* position, void* value);

/**
  Delete the last element.
 */
void GDA_list_pop_back(GDA_List* list);

/**
  Deletes the first element.
*/
void GDA_list_pop_front(GDA_List* list);

/**
  Get the next element from the list - if none exists, NULL is returned.
 */
GDA_Node* GDA_list_next(GDA_List* list, GDA_Node* node);

/**
  Get the previous element from the list - if none exists, NULL is returned.
 */
GDA_Node* GDA_list_prev(GDA_List* list, GDA_Node* node);

/**
  Get the value to which the node points to.
 */
void* GDA_list_value(GDA_List* list, GDA_Node* node);

/**
  Copies the values of the list into the given array. Returns the amount of elements
  that are written to the array.
 */
size_t GDA_list_to_array(GDA_List* list, void* array, size_t size_in_elements);

/**
  Erase a single element. The node will be freed.
  Returns the node element that is after the removed element.
  If the head is removed, the new head is returned.
  If the tail is removed, the tail is returned.
  If the list is empty, null is returned.
*/
GDA_Node* GDA_list_erase_single(GDA_List* list, GDA_Node* node);

/**
  Erase multiple elements in a given range, including the given nodes. All affected nodes will be freed.
  TODO: The explanation below needs to be improved, since multiple elements are removed.
  Returns the node elment that is after the removed element.
  If the head is removed, the new head is returned.
  If the tail is removed, the tail is returned.
  If the list is empty, null is returned.
 */
GDA_Node* GDA_list_erase_range(GDA_List* list, GDA_Node* start, GDA_Node* end);

/**
  Apply the given function on every element of the list
 */
void GDA_list_map(GDA_List* list, void (*fun)(void*) );

/**
 Returns a pointer to the element if it is found, and NULL otherwise.
 */
GDA_Node* GDA_list_find_node(GDA_List* list, GDA_Node* node);

/**
  Returns a pointer to the element if it is found, and NULL otherwise.
 */
GDA_Node* GDA_list_find_value(GDA_List* list, void* value);

/**
  Returns the first node (the head).
 */
GDA_Node* GDA_list_front(GDA_List* list);

/**
  Returns the last node (the tail).
 */
GDA_Node* GDA_list_back(GDA_List* list);

#endif // __GDA_LIST_H
