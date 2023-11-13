// Copyright (c) 2023 ETH-Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#ifndef __LPG_GRAPH500_DATA_SCHEME_1_H
#define __LPG_GRAPH500_DATA_SCHEME_1_H

#include <inttypes.h>

/**
  the following code was written by Tomasz Czajęcki
 */

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

uint8_t get_random_uint8_t(uint8_t min, uint8_t max);
uint16_t get_random_uint16_t(uint16_t min, uint16_t max);

uint32_t create_uint32_property( uint32_t max_value );
uint64_t create_uint64_property( uint64_t max_value );
char* create_string_property( uint32_t num_bytes );

/**
  end of code written by Tomasz Czajęcki
 */

#define VERTEX_LABEL_COUNT 5 /* do not change, please */
#define EDGE_LABEL_COUNT 15 /* do not change, please */

extern const char* vertex_label_names[];
extern const char* edge_label_names[];

extern uint64_t* vlabel_range;
extern uint8_t edge_matrix[VERTEX_LABEL_COUNT][VERTEX_LABEL_COUNT];

void data_scheme_1_init( uint64_t nglobalverts );
void data_scheme_1_finalize();

uint8_t data_scheme_1_assign_elabel( uint64_t origin, uint64_t target );

#endif // __LPG_GRAPH500_DATA_SCHEME_1_H
