// Copyright (c) 2023 ETH-Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#include <stdlib.h>

#include "data_scheme_1.h"

/**
  the following code was written by Tomasz Czajęcki
  and adapted by Robert Gerstenberger
 */
char upper = 'z';
char lower = 'a';

#define random(type)                                \
  type get_random_##type(type min, type max) {      \
    return (type) (rand() % (max - min + 1) + min); \
  }

random(uint8_t)
random(uint16_t)


uint32_t create_uint32_property( uint32_t max_value ) {
  uint32_t value =
    (((uint32_t) rand() <<  0) & 0x0000FFFFul) |
    (((uint32_t) rand() << 16) & 0xFFFF0000ul);

  return value % max_value;
}

char* create_string_property( uint32_t num_bytes ) {
  char* value = malloc( (num_bytes + 1) * sizeof(char) );
  for( size_t i=0; i<num_bytes; i++ ) {
    value[i] = get_random_uint8_t( lower, upper );
  }
  value[num_bytes] = '\0';

  return value;
}

uint64_t create_uint64_property( uint64_t max_value ) {
  uint64_t value =
    (((uint64_t) rand() <<  0) & 0x000000000000FFFFull) |
    (((uint64_t) rand() << 16) & 0x00000000FFFF0000ull) |
    (((uint64_t) rand() << 32) & 0x0000FFFF00000000ull) |
    (((uint64_t) rand() << 48) & 0xFFFF000000000000ull);

  return value % max_value;
}

/**
  end of code written by Tomasz Czajęcki
 */

const char* vertex_label_names[VERTEX_LABEL_COUNT] = { "Company", "Person", "Place", "Project", "Resource" };
const char* edge_label_names[EDGE_LABEL_COUNT] = { "canBeUsedWith", "canUse", "foundAt", "hasBranchesAt", "impacts",
  "inBusinessWith", "influences", "inVicinityOf", "isPartOf", "knows", "needs", "supports", "uses", "wasIn", "worksAt" };

uint64_t* vlabel_range;
uint8_t edge_matrix[VERTEX_LABEL_COUNT][VERTEX_LABEL_COUNT];

void data_scheme_1_init( uint64_t nglobalverts ) {

  vlabel_range = malloc( VERTEX_LABEL_COUNT * sizeof(uint64_t) );

  /* adapt the code below to change the label distribution of the vertices */
  vlabel_range[0] = nglobalverts * 5 / 100; /* 5% */
  vlabel_range[1] = nglobalverts * 20 / 100; /* 15% */
  vlabel_range[2] = nglobalverts * 25 / 100; /* 5% */
  vlabel_range[3] = nglobalverts * 75 / 100; /* 50% */
  vlabel_range[4] = nglobalverts; /* should be 25% */

  /**
    matrix to label the edges based on the labels of incident vertices

    matrix layout:
                company         person        place             project         resource
    company     inBusinessWith  worksAt       hasBranchesAt     supports        uses
    person      worksAt         knows         wasIn             isPartOf        canUse
    place       hasBranchesAt   wasIn         inVicinityOf      impacts         foundAt
    project     supports        isPartOf      impacts           influences      needs
    resource    uses            canUse        foundAt           needs           canBeUsedWith
   */

  edge_matrix[0][0] = 5;  /* company  - company:  inBusinessWith */
  edge_matrix[0][1] = 14; /* company  - person:   worksAt */
  edge_matrix[0][2] = 3;  /* company  - place:    hasBranchesAt */
  edge_matrix[0][3] = 11; /* company  - project:  supports */
  edge_matrix[0][4] = 12; /* company  - resource: uses */
  edge_matrix[1][0] = 14; /* person   - company:  worksAt */
  edge_matrix[1][1] = 9;  /* person   - person:   knows */
  edge_matrix[1][2] = 13; /* person   - place:    wasIn */
  edge_matrix[1][3] = 8;  /* person   - project:  isPartOf */
  edge_matrix[1][4] = 1;  /* person   - resource: canUse */
  edge_matrix[2][0] = 3;  /* place    - company:  hasBranchesAt */
  edge_matrix[2][1] = 13; /* place    - person:   wasIn */
  edge_matrix[2][2] = 7;  /* place    - place:    inVicinityOf */
  edge_matrix[2][3] = 4;  /* place    - project:  impacts */
  edge_matrix[2][4] = 2;  /* place    - resource: foundAt */
  edge_matrix[3][0] = 11; /* project  - company:  supports */
  edge_matrix[3][1] = 8;  /* project  - person:   isPartOf */
  edge_matrix[3][2] = 4;  /* project  - place:    impacts */
  edge_matrix[3][3] = 6;  /* project  - project:  influences */
  edge_matrix[3][4] = 10; /* project  - resource: needs */
  edge_matrix[4][0] = 12; /* resource - company:  uses */
  edge_matrix[4][1] = 1;  /* resource - person:   canUse */
  edge_matrix[4][2] = 2;  /* resource - place:    foundAt */
  edge_matrix[4][3] = 10; /* resource - project:  needs */
  edge_matrix[4][4] = 0;  /* resource - resource: canBeUsedWith */
}


void data_scheme_1_finalize() {
  free( vlabel_range );
}


uint8_t data_scheme_1_assign_elabel( uint64_t origin, uint64_t target ) {
  uint8_t idx_origin = VERTEX_LABEL_COUNT-1;
  uint8_t idx_target = VERTEX_LABEL_COUNT-1;
  for( uint8_t i=0 ; i<VERTEX_LABEL_COUNT-1 ; i++ ) {
    if( origin < vlabel_range[i] ) {
      idx_origin = i;
      break;
    }
  }
  for( uint8_t i=0 ; i<VERTEX_LABEL_COUNT-1 ; i++ ) {
    if( target < vlabel_range[i] ) {
      idx_target = i;
      break;
    }
  }

  return edge_matrix[idx_origin][idx_target];
}
