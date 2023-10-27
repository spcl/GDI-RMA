// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#include <string.h>

#include "gda_vector.h"


void GDA_vector_create(GDA_Vector** vector, size_t element_size, size_t capacity) {
  *vector = (GDA_Vector*)malloc(sizeof(GDA_Vector));
  (*vector)->element_size = element_size;
  (*vector)->capacity = capacity;
  (*vector)->size = 0;
  (*vector)->data = malloc(element_size*capacity);
}


void GDA_vector_push_back(GDA_Vector* vector, const void* element) {
  if (vector->size >= vector->capacity) {
    vector->capacity = vector->capacity << 1;
    vector->data = realloc(vector->data, vector->capacity*vector->element_size);
  }
  memcpy( ( (char*) vector->data) + vector->element_size*vector->size, element, vector->element_size);
  vector->size++;
}


void* GDA_vector_at(GDA_Vector* vector, size_t i) {
  return (void*)( ( (char*)vector->data) + i*vector->element_size);
}


size_t GDA_vector_size(GDA_Vector* vector) {
  return vector->size;
}


void GDA_vector_pop_back(GDA_Vector* vector) {
  vector->size--;
  if(vector->size < (vector->capacity >> 2) ) { // division by 4
    vector->capacity = vector->capacity >> 1;
    vector->data = realloc(vector->data, vector->capacity * vector->element_size);
  }
}


void GDA_vector_free(GDA_Vector** vector) {
  if ((*vector)->data) {
    free((*vector)->data);
    (*vector)->data = 0;
  }
  free(*vector);
  *vector = NULL;
}
