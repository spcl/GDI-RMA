// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#ifndef __GDA_VECTOR_H
#define __GDA_VECTOR_H

#include <stdlib.h>

/**
  This header provides functions for a vector.
  The vector can also be used as a stack.
  The vector provides an interface similar to std::vector
  It provides the following properties:
    - O(1) insert at the tail (amortized)
    - O(1) removal from the tail (amortized)
    - O(1) destruction
*/


/**
  data type definitions
 */
typedef struct GDA_Vector {
  size_t element_size;
  size_t capacity;
  size_t size;
  void* data;
} GDA_Vector;


/**
  function prototypes
 */

/**
  Initialize a new vector that can hold elements of size element_size.
  The vector should be destroyed using vector_free.
 */
void GDA_vector_create(GDA_Vector** vector, size_t element_size, size_t capacity);

/**
  Destroys the vector and frees all allocated resources.
 */
void GDA_vector_free(GDA_Vector** vector);

/**
  Appends a single element to the end of the vector.
  An element should point to a memory location
  containing a single element of size element_size.
  The element, to which the parameter element points to,
  gets copied to the vector.
  If the vector reaches the capacity, it triggers a resize
  of the underlyining array (doubles the size).
 */
void GDA_vector_push_back(GDA_Vector* vector, const void* element);

/**
  Removes the element stored at the last position. If there is only
  1/4 of the space in use, it triggers a resize of the vector (halves the size).
 */
void GDA_vector_pop_back(GDA_Vector* vector);

/**
  Retrieve a pointer the element at position i of the vector.
  No out of bounds access checks are implemented.
 */
void* GDA_vector_at(GDA_Vector* vector, size_t i);

/**
 Returns the number of elements stored in the vector.
 */
size_t GDA_vector_size(GDA_Vector* vector);

#endif // __GDA_VECTOR_H
