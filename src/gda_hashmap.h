// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#ifndef __GDA_HASHMAP_H
#define __GDA_HASHMAP_H

#include <inttypes.h>
#include <stdlib.h>

/**
  This header provides functions for a hashmap. See also the pseudo code on Wikipedia (especially for removing an entry):
  https://en.wikipedia.org/wiki/Open_addressing
  The hashmap provides an interface similar to std::unordered_map.
  Note that the interface already assumes that the user provides a key for the value one wants to insert.
  E.g. if you want to insert a string, apply a hash function to determine a good key. If the key is
  a character (byte array), use the function char_to_int as value for the create function, if the key
  is an integer, you can use int_to_int.
  The implementation applys a linear probing and extends the arrays if needed (up to 0.5 load).
  If this threshold is reached, the capacity is doubled. The hashmap is never compressed.
  It provides the following properties:
    - O(1) insert (amortized)
    - O(1) removal
    - O(1) destruction
*/


/**
  constant definitions
 */
#define GDA_HASHMAP_NOT_FOUND -1
#define GDA_HASHMAP_IN_USE 1
#define GDA_HASHMAP_NOT_IN_USE 0


/**
  data type definitions
 */

typedef struct GDA_HashMap {
  /**
    the size of the key
   */
  size_t key_size;
  /**
    the capacity (the number of total elements)
   */
  size_t capacity;
  /**
    the number of used elements
   */
  size_t size;
  /**
    the size of the values we insert
   */
  size_t value_size;
  /**
    the array to store the values
   */
  void* values;
  /**
    the array to store the keys
   */
  void* keys;
  /**
    the array to keep track if a position is used or not
   */
  char* uses;
  /**
    hash function that converts a given key to a position (non-negative integer)
   */
  uint32_t (*f)(void* key, size_t key_size, size_t capacity);
} GDA_HashMap;


/**
  function prototypes
 */

/**
  Creates a new hashmap.
  A few standard functions already exist to convert your key into an integer: int_to_int and char_to_int.
 */
void GDA_hashmap_create(GDA_HashMap** hashmap, size_t key_size, size_t capacity, size_t value_size, uint32_t (*f)(void* key, size_t key_size, size_t capacity) );

/**
  The function deallocates all associated functions and sets the value to NULL.
 */
void GDA_hashmap_free(GDA_HashMap** hashmap);

/**
  Inserts the value (a copy) into the hash map.
 */
int32_t GDA_hashmap_insert(GDA_HashMap* hashmap, void* key, void* value);

/**
  Lookups the position of the element by the given key. Note that the
  index is only valid up to the next insert function (which might trigger a resize operation!)
 */
int32_t GDA_hashmap_find(GDA_HashMap* hashmap, void* key);

/**
  Returns the value at the given position.
 */
void* GDA_hashmap_get_at(GDA_HashMap* hashmap, size_t i);

/**
  Returns the value with the given key.
 */
void* GDA_hashmap_get(GDA_HashMap* hashmap, void* key);

/**
  Removes the element at a given position.
 */
void GDA_hashmap_erase_at(GDA_HashMap* hashmap, size_t i);

/**
  Removes the element by the given key.
 */
void GDA_hashmap_erase(GDA_HashMap* hashmap, void* key);

/**
  Reports the size of the hash map (the number of elements).
 */
size_t GDA_hashmap_size(GDA_HashMap* hashmap);

/**
  Reports the capacity of the hash map (the number elements that can fit).
 */
size_t GDA_hashmap_capacity(GDA_HashMap* hashmap);

/**
  Reports the load, the ratio of occupied elements: (size / capacity).
 */
double GDA_hashmap_load(GDA_HashMap* hashmap);

/**
  Default function to convert an integer key to an integer position.
 */
uint32_t GDA_int_to_int(void* key, size_t key_size, size_t capacity);

/**
  Default function to convert a 64bit integer key to an integer position.
 */
uint32_t GDA_int64_to_int(void* key, size_t key_size, size_t capacity);

/**
  Default function to convert a char key to an integer position.
 */
uint32_t GDA_char_to_int(void* key, size_t key_size, size_t capacity);

/**
  djb2 hash from http://www.cse.yorku.ca/~oz/hash.html
 */
uint64_t GDA_djb2_hash(const unsigned char *str);

/**
  uses djb2 hash (http://www.cse.yorku.ca/~oz/hash.html)

  takes into account not just the property id but also the label
  integer handle
 */
uint64_t GDA_hash_property_id( const unsigned char *property_id, size_t size, uint32_t label_int_handle );

#endif // __GDA_HASHMAP_H
