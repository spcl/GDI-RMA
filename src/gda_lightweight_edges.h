// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Roman Haag

#ifndef GDA_LIGHTWEIGHTEDGES_H
#define GDA_LIGHTWEIGHTEDGES_H

#include "gdi.h"

/**
  a labeled lightweight edge block has a size of 10 elements, which
  are each of an 8 Byte size (pointer), so in total 80 Bytes
 */
#define GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE 10
#define GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES  (GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE * sizeof(GDA_DPointer))

/**
  Definition Block:
  80 Bytes of the labeled lightweight edges data structure meaning 8
  Bytes of metadata + 8 Bytes of label data + 8 * 8 Bytes of pointer
  data are called a block.
 */

/**
  Use the GDA_Vector to allocate one block and zero out the metadata.
  Using only 1 block should not be a problem as the dynamic resizing of
  GDA_Vector handles many edge insertions.
 */
void GDA_LightweightEdgesInit( GDI_VertexHolder vertex );

/**
  Adjusts the meta data according to the edge_orientation, adds the
  distributed pointer and increases the lightweight_edge_insert_offset.

  If the new lightweight_edge_insert_offset exceeds
  lightweight_edge_size (size of the whole buffer), the buffer size is
  doubled, lightweight_edge_size is updated accordingly.

  If lightweight_edge_insert_offset points to a new block, its meta data
  and label data are initialized and lightweight_edge_insert_offset will
  point to the first element of that new block afterwards.
 */
void GDA_LightweightEdgesAddEdge( int edge_orientation, GDA_DPointer dpointer, GDI_VertexHolder vertex, uint32_t* edge_offset );

/**
  Check if the meta data of the edge is 0. If so, set removed_flag to
  false, else set that meta data to 0 and removed_flag to true.
 */
void GDA_LightweightEdgesRemove( uint32_t edge_offset, GDI_VertexHolder vertex, bool* removed_flag );

/**
  Check if the meta data of the edge is 0. If so, set found_flag to
  false, else return dpointer and edge_orientation of the edge and set
  found_flag to true.
 */
void GDA_LightweightEdgesGetEdge( GDA_DPointer* dpointer, int* edge_orientation, uint32_t edge_offset, GDI_VertexHolder vertex, bool* found_flag );

/**
  Check if the meta data of the edge is 0. If so, set found_flag to
  false, else return label integer handle of the edge and set found_flag
  to true.
 */
void GDA_LightweightEdgesGetLabel( uint8_t* label_int_handle, uint32_t edge_offset, GDI_VertexHolder vertex, bool* found_flag );

/**
  Use two iterators that go over all the meta data: one from the front
  and one from the back. The forward iterator looks for free space (e.g.
  Bytes in the meta data that are zero). The backwards iterator looks
  for edges from the end.
  Once both iterators meet the respective condition, the edge from the
  backwards iterator gets copied to the forward iterator, meaning the
  meta and label data and the actual pointer get copied. This process
  continues until the two iterators cross over.

  The data structure is not actually shrunk, since this function is only
  called during the closing of a transaction, after which the data
  structure is freed.
 */
void GDA_LightweightEdgesShrink( GDI_VertexHolder vertex );

/**
  Iterate over the whole meta data and filter out the edges that have
  the correct orientation. Fill in the offsets in array_of_offsets[].
  Stop if array_of_offsets[] would overflow and return the
  GDI_ERROR_TRUNCATE. Else just go over everything and return
  GDI_SUCCESS.

  returns only GDI_SUCCESS and GDI_ERROR_TRUNCATE
 */
int GDA_LightweightEdgesFilterEdges( uint32_t array_of_offsets[], size_t count, size_t* resultcount, int edge_orientation, GDI_VertexHolder vertex );

/**
  Iterate over the whole metadata and filter out the edges that have the
  correct orientation. If a edge meets the condition, increment the count.
 */
void GDA_LightweightEdgesNumEdges( size_t* resultcount, int edge_orientation, GDI_VertexHolder vertex );

/**
  Iterate over the whole meta data and filter out the edges that have
  the correct orientation and a label in the whitelist. Fill in the
  offsets in array_of_offsets[]. Stop if array_of_offsets[] would
  overflow and return the GDI_ERROR_TRUNCATE. Else just go over
  everything and return GDI_SUCCESS.

  returns only GDI_SUCCESS and GDI_ERROR_TRUNCATE
 */
int GDA_LightweightEdgesFilterEdgesWithLabelWhitelist( uint32_t array_of_offsets[], size_t count, size_t* resultcount, int edge_orientation, uint8_t label_whitelist[], size_t list_size, GDI_VertexHolder vertex );

/**
  Iterate over the whole meta and label data and filter out the edges
  that have the correct orientation and a label in the whitelist. If a
  edge meets the conditions, increment the count.
 */
void GDA_LightweightEdgesNumEdgesWithLabelWhitelist( size_t* resultcount, int edge_orientation, uint8_t label_whitelist[], size_t list_size, GDI_VertexHolder vertex );

/**
  Iterate over the whole meta data and filter out the edges that have
  the correct orientation and a label not on the blacklist. Fill in the
  offsets in array_of_offsets[]. Stop if array_of_offsets[] would
  overflow and return the GDI_ERROR_TRUNCATE. Else just go over
  everything and return GDI_SUCCESS.

  returns only GDI_SUCCESS and GDI_ERROR_TRUNCATE
 */
int GDA_LightweightEdgesFilterEdgesWithLabelBlacklist( uint32_t array_of_offsets[], size_t count, size_t* resultcount, int edge_orientation, uint8_t label_blacklist[], size_t list_size, GDI_VertexHolder vertex );


/**
  Iterate over the whole meta and label data and filter out the edges
  that have the correct orientation and a label not on the blacklist.
  If a edge meets the conditions, increment the count.
 */
void GDA_LightweightEdgesNumEdgesWithLabelBlacklist( size_t* resultcount, int edge_orientation, uint8_t label_blacklist[], size_t list_size, GDI_VertexHolder vertex );

/**
  Change the meta data of the edge found at edge_offset to
  edge_orientation.
 */
void GDA_LightweightEdgesSetEdgeOrientation( int edge_orientation, uint32_t edge_offset, GDI_VertexHolder vertex );

/**
  Change the DPointer of the edge found at edge_offset.
 */
void GDA_LightweightEdgesSetDPointer( GDA_DPointer dpointer, uint32_t edge_offset, GDI_VertexHolder vertex );

/**
  update the label integer handle of the edge found at edge_offset
 */
void GDA_LightweightEdgesSetLabel( uint8_t label_int_handle, uint32_t edge_offset, GDI_VertexHolder vertex );

/**
  find the offset of an edge in the lightweight edge data structure of
  the other vertex
 */
uint32_t GDA_LightweightEdgesFindEdge( int original_edge_orientation, GDA_DPointer other_vertex, uint8_t label_int_handle, GDI_VertexHolder vertex );

/**
  prints the edge orientation and the vertex UID of the second vertex
  for all edges

  each edge is printed on a separate line in the following format:
  rank | orientation | label | vertex UID

  orientation:
  EMP = empty
  INC = incoming
  OUT = outgoing
  UND = undirected
 */
void GDA_LightweightEdgesPrint( GDI_VertexHolder vertex );

/**
  Helper function that gives back the pointer to the metadata of an edge
  at edge_offset. Globaly visible as it gets used in the testing
  interface.
 */
static inline void GDA_LightweightEdgesGetMetadataPointerWithOffset( uint8_t** metadata_pointer, GDI_VertexHolder vertex, uint32_t edge_offset ) {
  uint32_t block_start_offset, offset;
  block_start_offset = (edge_offset / GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE) * GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE /* round to the next integer divisible by GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE */;
  offset = edge_offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE - 2;
  *metadata_pointer = ((uint8_t*) (vertex->lightweight_edge_data+block_start_offset)) + offset;
}

#endif //#ifndef GDA_LIGHTWEIGHTEDGES_H
