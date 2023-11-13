// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#include <string.h>

#include "gda_hashmap.h"

// TODO: remove this
#ifndef UNUSED
  #define UNUSED(x) (void)(x)
#endif


void GDA_hashmap_create(GDA_HashMap** hashmap, size_t key_size, size_t capacity, size_t value_size, uint32_t (*f)(void* key, size_t key_size, size_t capacity) ) {
  (*hashmap) = (GDA_HashMap*)calloc(1, sizeof(GDA_HashMap));
  (*hashmap)->key_size = key_size;
  (*hashmap)->value_size = value_size;
  (*hashmap)->capacity = capacity;
  (*hashmap)->values = calloc((*hashmap)->capacity, (*hashmap)->value_size);
  (*hashmap)->keys = calloc((*hashmap)->capacity, (*hashmap)->key_size);
  (*hashmap)->uses = calloc((*hashmap)->capacity, sizeof(char));
  (*hashmap)->size = 0;
  (*hashmap)->f = f;
}


void GDA_hashmap_free(GDA_HashMap** hashmap) {
  free( (*hashmap)->keys);
  free( (*hashmap)->values);
  free( (*hashmap)->uses);
  free(*hashmap);
  *hashmap = NULL;
}


/**
  This function lookups a given key in the hashmap. If the key is not found, then the position is returned
  in the parameter pos_not_found. If the key is found, then the position is returned in the parameter
  pos_found. The function applies a linear probing.
 */
inline void hashmap_get_pos(GDA_HashMap* hashmap, void* key, int32_t* pos_found, int32_t* pos_not_found) {
  *pos_not_found = GDA_HASHMAP_NOT_FOUND;
  *pos_found = GDA_HASHMAP_NOT_FOUND;

  int32_t h = (int32_t)hashmap->f(key, hashmap->key_size, hashmap->capacity);
  int32_t m = (int32_t)hashmap->capacity;

  // linear probing
  for(int32_t pos = h; ; pos++) {
    pos = pos % m;

    if( hashmap->uses[pos] == GDA_HASHMAP_NOT_IN_USE ) {
      *pos_not_found = pos;
      return;
    }
    else if( memcmp( (char*)hashmap->keys + (uint32_t)pos*hashmap->key_size, key, hashmap->key_size) == 0) {
      *pos_found = pos;
      return;
    }
  }
}


/**
  Inserts the value with the given key in the hashmap.
 */
inline int32_t hashmap_insert_single(GDA_HashMap* hashmap, void* key, void* value) {
  int32_t pos_found;
  int32_t pos_not_found;
  hashmap_get_pos(hashmap, key, &pos_found, &pos_not_found);

  if(pos_not_found != GDA_HASHMAP_NOT_FOUND) {
    memcpy( (char*)hashmap->keys + (uint32_t)pos_not_found*hashmap->key_size, key, hashmap->key_size );
    memcpy( (char*)hashmap->values + (uint32_t)pos_not_found*hashmap->value_size, value, hashmap->value_size );
    hashmap->uses[pos_not_found] = GDA_HASHMAP_IN_USE;
    hashmap->size++;
    return pos_not_found;
  }
  return GDA_HASHMAP_NOT_FOUND;
}


int32_t GDA_hashmap_insert(GDA_HashMap* hashmap, void* key, void* value) {
  if(hashmap->size >= hashmap->capacity >> 1) {

    void* keys = hashmap->keys;
    void* values = hashmap->values;
    void* uses = hashmap->uses;
    size_t capacity = hashmap->capacity;

    hashmap->capacity = hashmap->capacity << 1;
    hashmap->values = calloc(hashmap->capacity, hashmap->value_size);
    hashmap->keys = calloc(hashmap->capacity, hashmap->key_size);
    hashmap->uses = calloc(hashmap->capacity, sizeof(char));
    hashmap->size = 0;

    for(uint32_t i = 0; i < capacity; i++) {
      char* pos_key = ((char*) keys ) + i * hashmap->key_size;
      char* pos_value = ((char*)values) + i * hashmap->value_size;
      if( ((char*) uses)[i] != GDA_HASHMAP_NOT_IN_USE ) {
        hashmap_insert_single(hashmap, pos_key, pos_value);
      }
    }

    free(keys);
    free(values);
    free(uses);
  }

  return hashmap_insert_single(hashmap, key, value);
}


int32_t GDA_hashmap_find(GDA_HashMap* hashmap, void* key) {
  int32_t pos_found;
  int32_t pos_not_found;
  hashmap_get_pos(hashmap, key, &pos_found, &pos_not_found);

  if(pos_found != GDA_HASHMAP_NOT_FOUND) {
    return pos_found;
  }
  return GDA_HASHMAP_NOT_FOUND;
}


void* GDA_hashmap_get_at(GDA_HashMap* hashmap, size_t i) {
  return ( (char*)hashmap->values ) + i * hashmap->value_size;
}


void* GDA_hashmap_get(GDA_HashMap* hashmap, void* key) {
  int32_t pos_found;
  int32_t pos_not_found;
  hashmap_get_pos(hashmap, key, &pos_found, &pos_not_found);

  if(pos_found != GDA_HASHMAP_NOT_FOUND) {
    return GDA_hashmap_get_at(hashmap, pos_found);
  }
  return NULL;
}


void GDA_hashmap_erase_at(GDA_HashMap* hashmap, size_t i) {
  if(hashmap->uses[i] == GDA_HASHMAP_NOT_IN_USE) {
    return;
  }

  size_t j = i;
  do {
    hashmap->uses[i] = GDA_HASHMAP_NOT_IN_USE;
    size_t k  = 0;
    do {
      j = (j+1) % hashmap->capacity;
      if(hashmap->uses[j] == GDA_HASHMAP_NOT_IN_USE) {
        hashmap->size--;
        return;
      }

      k = hashmap->f((char*)hashmap->keys + (uint32_t)j*hashmap->key_size, hashmap->key_size, hashmap->capacity);
    } while( (i<=j) ? ((i<k)&&(k<=j)) : ((i<k)||(k<=j))  );

    // copy values from j to i
    memcpy((char*)hashmap->keys + (uint32_t)i*hashmap->key_size, (char*)hashmap->keys + (uint32_t)j*hashmap->key_size, hashmap->key_size );
    memcpy((char*)hashmap->values + (uint32_t)i*hashmap->value_size, (char*)hashmap->values + (uint32_t)j*hashmap->value_size, hashmap->value_size );
    hashmap->uses[i] = hashmap->uses[j];
    i = j;
  }while(1);
}


void GDA_hashmap_erase(GDA_HashMap* hashmap, void* key) {
  int32_t pos_found;
  int32_t pos_not_found;
  hashmap_get_pos(hashmap, key, &pos_found, &pos_not_found);

  if(pos_found != GDA_HASHMAP_NOT_FOUND) {
    GDA_hashmap_erase_at(hashmap, pos_found);
  }
}


size_t GDA_hashmap_size(GDA_HashMap* hashmap) {
  return hashmap->size;
}


size_t GDA_hashmap_capacity(GDA_HashMap* hashmap) {
  return hashmap->capacity;
}


double GDA_hashmap_load(GDA_HashMap* hashmap) {
  return (double)hashmap->size / (double)hashmap->capacity;
}


uint32_t GDA_int_to_int(void* key, size_t key_size, size_t capacity) {
  UNUSED(key_size);
  return (* (uint32_t*)key) % capacity;
}


uint32_t GDA_int64_to_int(void* key, size_t key_size, size_t capacity) {
  UNUSED(key_size);
  return (* (uint64_t*)key) % capacity;
}


uint32_t GDA_char_to_int(void* key, size_t key_size, size_t capacity) {
  uint32_t res = 0;
  for(uint32_t i = 0; i < key_size; i++) {
    res = (res + ((uint8_t*)key)[i] ) % capacity;
  }
  return res % capacity;
}


/**
  copy of the code provided by http://www.cse.yorku.ca/~oz/hash.html
 */
uint64_t GDA_djb2_hash(const unsigned char *str) {
  uint64_t hash = 5381;
  uint64_t c;
  while ( (c = *str++) ) {
    hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
  }
  return hash;
}

/**
  uses djb2 hash with xor (http://www.cse.yorku.ca/~oz/hash.html)

  takes into account not just the ID property but also the label
  integer handle (should usually be just a one Byte integer)
 */
uint64_t GDA_hash_property_id( const unsigned char *property_id, size_t size, uint32_t label_int_handle ) {
  uint64_t hash = 5381;
  /**
    first round with label integer handle
   */
  hash = ((hash << 5) + hash) ^ label_int_handle;

  /**
    deal with the ID property
   */
  for( size_t i=0 ; i<size ; i++ ) {
    hash = ((hash << 5) + hash) ^ property_id[i]; /* hash * 33 + c */
  }

  return hash;
}
