// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Roman Haag

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "gda_lightweight_edges.h"

#define GDA_EDGE_EMPTY                          0


void GDA_LightweightEdgesInit( GDI_VertexHolder vertex ) {
  assert( vertex != NULL );

  vertex->lightweight_edge_data = malloc( GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES );
  vertex->lightweight_edge_size = GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES;
  /**
    set the meta data of the first block of edges to unused (= 0)
   */
  vertex->lightweight_edge_data[0] = GDA_EDGE_EMPTY;
  /**
    set the integer handles of the label data of the first block to
    GDI_LABEL_NONE (= 0)
   */
  vertex->lightweight_edge_data[1] = 0;
  vertex->lightweight_edge_insert_offset = 2;
}


void GDA_LightweightEdgesAddEdge( int edge_orientation, GDA_DPointer dpointer, GDI_VertexHolder vertex, uint32_t* edge_offset ) {
  /**
    input validation
   */
  assert( vertex != NULL );
  assert( edge_offset != NULL );
  assert( (edge_orientation == GDI_EDGE_INCOMING) || (edge_orientation == GDI_EDGE_OUTGOING) || (edge_orientation == GDI_EDGE_UNDIRECTED) );

  /**
    calculate offset of metadata
   */
  uint8_t* metadata;
  GDA_LightweightEdgesGetMetadataPointerWithOffset( &metadata, vertex, vertex->lightweight_edge_insert_offset );
  *metadata = (uint8_t) edge_orientation;

  /**
    insert the actual edge and update the return buffer
   */
  vertex->lightweight_edge_data[vertex->lightweight_edge_insert_offset] = dpointer;
  *edge_offset = vertex->lightweight_edge_insert_offset++;

  /**
    resize the labeled lightweight edge data, if necessary
   */
  if( (vertex->lightweight_edge_insert_offset * 8 /* sizeof(GDA_DPointer) */) >= vertex->lightweight_edge_size ) {
    vertex->lightweight_edge_size = vertex->lightweight_edge_size << 1;
    vertex->lightweight_edge_data = realloc( vertex->lightweight_edge_data, vertex->lightweight_edge_size );
  }
  if( vertex->lightweight_edge_insert_offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE == 0 ) {
    /**
      start a new block of lightweight edges, so set the meta data to
      unused (= 0) and the integer handles of the label data to
      GDI_LABEL_NONE (= 0)
     */
    vertex->lightweight_edge_data[vertex->lightweight_edge_insert_offset++] = GDA_EDGE_EMPTY;
    vertex->lightweight_edge_data[vertex->lightweight_edge_insert_offset++] = 0;
  }
}


void GDA_LightweightEdgesRemove( uint32_t edge_offset, GDI_VertexHolder vertex, bool* removed_flag ) {
  /**
    input validation
   */
  assert( vertex != NULL );
  assert( removed_flag != NULL );
  assert( (edge_offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE != 0) && (edge_offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE != 1) );
  assert( edge_offset < vertex->lightweight_edge_insert_offset );

  /**
    calculate offset of metadata
   */
  uint8_t* metadata;
  GDA_LightweightEdgesGetMetadataPointerWithOffset( &metadata, vertex, edge_offset );

  /**
    removal of the edge

    maybe the other bits need to be masked at some point
    but as they are currently not used they always will be 0
   */
  if( *metadata ) {
    *metadata = GDA_EDGE_EMPTY;
    *removed_flag = true;
  } else {
    *removed_flag = false;
  }
}


void GDA_LightweightEdgesGetEdge( GDA_DPointer* dpointer, int* edge_orientation, uint32_t edge_offset, GDI_VertexHolder vertex, bool* found_flag ) {
  /**
    input validation
   */
  assert( dpointer != NULL );
  assert( edge_orientation != NULL );
  assert( vertex != NULL );
  assert( found_flag != NULL );
  assert( (edge_offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE != 0) && (edge_offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE != 1) );
  assert( edge_offset < vertex->lightweight_edge_insert_offset );

  /**
    calculate offset of metadata
   */
  uint8_t* metadata;
  GDA_LightweightEdgesGetMetadataPointerWithOffset( &metadata, vertex, edge_offset );

  /**
    we assume that meta data either contains a correct value (edge
    orientation) or 0 (not valid anymore)
   */
  if( *metadata ) {
    *found_flag = true;
    *edge_orientation = ((int) *metadata) + 256u;
    *dpointer = vertex->lightweight_edge_data[edge_offset];
  } else {
    *found_flag = false;
  }
}


void GDA_LightweightEdgesGetLabel( uint8_t* label_int_handle, uint32_t edge_offset, GDI_VertexHolder vertex, bool* found_flag ) {
  /**
    input validation
   */
  assert( label_int_handle != NULL );
  assert( vertex != NULL );
  assert( found_flag != NULL );
  assert( (edge_offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE != 0) && (edge_offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE != 1) );
  assert( edge_offset < vertex->lightweight_edge_insert_offset );

  /**
    calculate offset of metadata
   */
  uint8_t* metadata;
  GDA_LightweightEdgesGetMetadataPointerWithOffset( &metadata, vertex, edge_offset );

  /**
    retrieval of the edge label
   */
  if( *metadata ) {
    *found_flag = true;
    *label_int_handle = *(metadata+8);
  } else {
    *found_flag = false;
  }
}


void GDA_LightweightEdgesShrink( GDI_VertexHolder vertex ) {
  /**
  input validation
 */
  assert( vertex != NULL );

  uint32_t offset, forward_offset, backward_offset, forward_block, backward_block;
  uint8_t *start_of_edge_array, *end_of_edge_array, *forward_iterator, *backward_iterator;

  /**
    initialisation
   */
  offset = vertex->lightweight_edge_insert_offset;
  /**
    offsets inside a block
   */
  forward_offset = 0;
  /**
    offset mod 10
   */
  backward_offset = offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE;

  forward_block = 0;
  /**
    offset div 10
   */
  backward_block = offset / GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE;
  /**
    need to cast to a uint8_t* as we iterate over Bytes
   */
  start_of_edge_array = (uint8_t *) vertex->lightweight_edge_data;
  /**
    end_of_edge_array points to the metadata of the last edge. It can be 0.
   */
  end_of_edge_array = start_of_edge_array + (backward_block * GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES) + backward_offset;
  forward_iterator = start_of_edge_array;
  backward_iterator = end_of_edge_array;

  /**
    main loop
   */
  while( forward_iterator < backward_iterator ) {
    uint8_t forward_element = *forward_iterator;
    /**
      go forwards and find a free slot
     */
    while( (forward_iterator < end_of_edge_array) && (forward_element & (uint8_t) (GDI_EDGE_INCOMING | GDI_EDGE_OUTGOING | GDI_EDGE_UNDIRECTED) /* last three bits are not 0 so not a free slot */) ) {
      if( forward_offset == 7 ) {
        /**
          go to the next block
         */
        forward_offset = 0;
        forward_block++;
      } else {
        forward_offset++;
      }
      forward_iterator = start_of_edge_array + (forward_block * GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES) + forward_offset;
      forward_element = *forward_iterator;
    }

    /**
      go backwards and find an edge
     */
    uint8_t backward_element = *backward_iterator;
    while( (backward_iterator > start_of_edge_array) && !(backward_element & (uint8_t) (GDI_EDGE_INCOMING | GDI_EDGE_OUTGOING | GDI_EDGE_UNDIRECTED))) {
      if( backward_offset == 0 ) {
        /**
          go to the previous block
         */
        backward_offset = 7;
        backward_block--;
      } else {
        backward_offset--;
      }
      backward_iterator = start_of_edge_array + (backward_block * GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES) + backward_offset;
      backward_element = *backward_iterator;
    }

    /**
      check if after iterator adjustment we are still performing a valid move
     */
    if( forward_iterator < backward_iterator ) {
      uint64_t *edge, *free_slot;

      /**
        move the meta data into the free slot
       */
      *forward_iterator = *backward_iterator;
      /**
        move the label data into the free slot
       */
      *(forward_iterator+8) = *(backward_iterator+8);
      /**
        move pointer into the free slot
       */
      edge = (uint64_t *) start_of_edge_array + (backward_block * GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE) + backward_offset + 2;
      free_slot = (uint64_t *) start_of_edge_array + (forward_block * GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE) + forward_offset + 2;
      *free_slot = *edge;
      /**
        delete old edge meta data
        needed for correctness of the algorithm, otherwise the backward_iterator is not adjusted
       */
      *backward_iterator = 0;
    }
  }

  /**
    size in number of blocks
    for Bytes multiply with a factor of GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES
    for number of edges multiply with a factor of 8

    using forward_block instead of backward_block ensures there is still
    space left to insert new edges
   */
  vertex->lightweight_edge_insert_offset = forward_block * GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE + forward_offset + 2;
}


/**
  returns either GDI_SUCCESS or GDI_ERROR_TRUNCATE
 */
int GDA_LightweightEdgesFilterEdges( uint32_t array_of_offsets[], size_t count, size_t* resultcount, int edge_orientation, GDI_VertexHolder vertex ) {
  /**
    input validation
   */
  assert( array_of_offsets != NULL );
  assert( resultcount != NULL );
  assert( vertex != NULL );
  assert( count > 0 );
  assert( ((uint8_t) edge_orientation > 0) && ((uint8_t) edge_orientation < 8) );

  /**
    initialisation
   */
  uint8_t* metadata_iterator;
  uint8_t orientation;
  uint32_t offset, max_offset;
  *resultcount = 0;
  offset = 2;
  max_offset = vertex->lightweight_edge_insert_offset;
  metadata_iterator = (uint8_t*) vertex->lightweight_edge_data;
  orientation = (uint8_t) edge_orientation;

  /**
    main loop
   */
  while( offset < max_offset ) {
    if( orientation & *metadata_iterator ) {
      if( *resultcount < count ) {
        array_of_offsets[*resultcount] = offset;
        (*resultcount)++;
      } else {
        return GDI_ERROR_TRUNCATE;
      }
    }
    offset++;
    metadata_iterator++;
    if( offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE == 0 ) {
      /**
        go to the next block
       */
      offset += 2;
      metadata_iterator += GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES - sizeof(GDA_DPointer);
    }
  }

  return GDI_SUCCESS;
}


void GDA_LightweightEdgesNumEdges( size_t* resultcount, int edge_orientation, GDI_VertexHolder vertex ) {
  /**
    input validation
   */
  assert( resultcount != NULL );
  assert( vertex != NULL );
  assert( ((uint8_t) edge_orientation > 0) && ((uint8_t) edge_orientation < 8) );

  /**
    initialisation
   */
  uint8_t* metadata_iterator;
  uint8_t orientation;
  uint32_t offset, max_offset;
  *resultcount = 0;
  offset = 2;
  max_offset = vertex->lightweight_edge_insert_offset;
  metadata_iterator = (uint8_t*) vertex->lightweight_edge_data;
  orientation = (uint8_t) edge_orientation;

  /**
    main loop
   */
  while( offset < max_offset ) {
    if( orientation & *metadata_iterator ) {
      (*resultcount)++;
    }
    offset++;
    metadata_iterator++;
    if( offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE == 0 ) {
      /**
        go to the next block
       */
      offset += 2;
      metadata_iterator += GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES - sizeof(GDA_DPointer);
    }
  }
}


int GDA_LightweightEdgesFilterEdgesWithLabelWhitelist( uint32_t array_of_offsets[], size_t count, size_t* resultcount, int edge_orientation, uint8_t label_whitelist[], size_t list_size, GDI_VertexHolder vertex ) {
  /**
    input validation
   */
  assert( array_of_offsets != NULL );
  assert( resultcount != NULL );
  assert( label_whitelist != NULL );
  assert( vertex != NULL );
  assert( count > 0 );
  assert( list_size > 0 );
  assert( ((uint8_t) edge_orientation > 0) && ((uint8_t) edge_orientation < 8) );

  /**
    initialisation
   */
  uint8_t* metadata_iterator;
  uint8_t orientation;
  uint32_t offset, max_offset;
  *resultcount = 0;
  offset = 2;
  max_offset = vertex->lightweight_edge_insert_offset;
  metadata_iterator = (uint8_t*) vertex->lightweight_edge_data;
  orientation = (uint8_t) edge_orientation;

  /**
    main loop
   */
  while( offset < max_offset ) {
    if( orientation & *metadata_iterator ) {
      /**
        edge orientation fits
       */
      uint8_t label = *(metadata_iterator+8);
      for( size_t i=0 ; i<list_size ; i++ ) {
        if( label == label_whitelist[i] ) {
          /**
            edge fits constraint
           */
          if( *resultcount < count ) {
            array_of_offsets[*resultcount] = offset;
            (*resultcount)++;
          } else {
            return GDI_ERROR_TRUNCATE;
          }
          /**
            don't need to check the rest of the list
           */
          break;
        }
      }
    }
    offset++;
    metadata_iterator++;
    if( offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE == 0 ) {
      /**
        go to the next block
       */
      offset += 2;
      metadata_iterator += GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES - sizeof(GDA_DPointer);
    }
  }

  return GDI_SUCCESS;
}


void GDA_LightweightEdgesNumEdgesWithLabelWhitelist( size_t* resultcount, int edge_orientation, uint8_t label_whitelist[], size_t list_size, GDI_VertexHolder vertex ) {
  /**
    input validation
   */
  assert( resultcount != NULL );
  assert( label_whitelist != NULL );
  assert( vertex != NULL );
  assert( list_size > 0 );
  assert( ((uint8_t) edge_orientation > 0) && ((uint8_t) edge_orientation < 8) );

  /**
    initialisation
   */
  uint8_t* metadata_iterator;
  uint8_t orientation;
  uint32_t offset, max_offset;
  *resultcount = 0;
  offset = 2;
  max_offset = vertex->lightweight_edge_insert_offset;
  metadata_iterator = (uint8_t*) vertex->lightweight_edge_data;
  orientation = (uint8_t) edge_orientation;

  /**
    main loop
   */
  while( offset < max_offset ) {
    if( orientation & *metadata_iterator ) {
      /**
        edge orientation fits
       */
      uint8_t label = *(metadata_iterator+8);
      for( size_t i=0 ; i<list_size ; i++ ) {
        if( label == label_whitelist[i] ) {
          /**
            edge fits constraint
           */
          (*resultcount)++;
          /**
            don't need to check the rest of the list
           */
          break;
        }
      }
    }
    offset++;
    metadata_iterator++;
    if( offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE == 0 ) {
      /**
        go to the next block
       */
      offset += 2;
      metadata_iterator += GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES - sizeof(GDA_DPointer);
    }
  }
}


int GDA_LightweightEdgesFilterEdgesWithLabelBlacklist( uint32_t array_of_offsets[], size_t count, size_t* resultcount, int edge_orientation, uint8_t label_blacklist[], size_t list_size, GDI_VertexHolder vertex ) {
  /**
    input validation
   */
  assert( array_of_offsets != NULL );
  assert( resultcount != NULL );
  assert( label_blacklist != NULL );
  assert( vertex != NULL );
  assert( count > 0 );
  assert( list_size > 0 );
  assert( ((uint8_t) edge_orientation > 0) && ((uint8_t) edge_orientation < 8) );

  /**
    initialisation
   */
  uint8_t* metadata_iterator;
  uint8_t orientation;
  uint32_t offset, max_offset;
  *resultcount = 0;
  offset = 2;
  max_offset = vertex->lightweight_edge_insert_offset;
  metadata_iterator = (uint8_t*) vertex->lightweight_edge_data;
  orientation = (uint8_t) edge_orientation;

  /**
    main loop
   */
  while( offset < max_offset ) {
    if( orientation & *metadata_iterator ) {
      /**
        edge orientation fits
       */
      uint8_t label = *(metadata_iterator+8);
      bool copy_flag = true;
      for( size_t i=0 ; i<list_size ; i++ ) {
        if( label == label_blacklist[i] ) {
          /**
            edge fits constraint
           */
          copy_flag = false;
          /**
            don't need to check the rest of the list
           */
          break;
        }
      }
      if( copy_flag ) {
        if( *resultcount < count ) {
          array_of_offsets[*resultcount] = offset;
          (*resultcount)++;
        } else {
          return GDI_ERROR_TRUNCATE;
        }
      }
    }
    offset++;
    metadata_iterator++;
    if( offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE == 0 ) {
      /**
        go to the next block
       */
      offset += 2;
      metadata_iterator += GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES - sizeof(GDA_DPointer);
    }
  }

  return GDI_SUCCESS;
}


void GDA_LightweightEdgesNumEdgesWithLabelBlacklist( size_t* resultcount, int edge_orientation, uint8_t label_blacklist[], size_t list_size, GDI_VertexHolder vertex ) {
  /**
    input validation
   */
  assert( resultcount != NULL );
  assert( label_blacklist != NULL );
  assert( vertex != NULL );
  assert( list_size > 0 );
  assert( ((uint8_t) edge_orientation > 0) && ((uint8_t) edge_orientation < 8) );

  /**
    initialisation
   */
  uint8_t* metadata_iterator;
  uint8_t orientation;
  uint32_t offset, max_offset;
  *resultcount = 0;
  offset = 2;
  max_offset = vertex->lightweight_edge_insert_offset;
  metadata_iterator = (uint8_t*) vertex->lightweight_edge_data;
  orientation = (uint8_t) edge_orientation;

  /**
    main loop
   */
  while( offset < max_offset ) {
    if( orientation & *metadata_iterator ) {
      /**
        edge orientation fits
       */
      uint8_t label = *(metadata_iterator+8);
      bool count_flag = true;
      for( size_t i=0 ; i<list_size ; i++ ) {
        if( label == label_blacklist[i] ) {
          /**
            edge fits constraint
           */
          count_flag = false;
          /**
            don't need to check the rest of the list
           */
          break;
        }
      }
      if( count_flag ) {
        (*resultcount)++;
      }
    }
    offset++;
    metadata_iterator++;
    if( offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE == 0 ) {
      /**
        go to the next block
       */
      offset += 2;
      metadata_iterator += GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES - sizeof(GDA_DPointer);
    }
  }
}


void GDA_LightweightEdgesSetEdgeOrientation( int edge_orientation, uint32_t edge_offset, GDI_VertexHolder vertex ) {
  /**
    input validation
   */
  assert( vertex != NULL );
  assert( (edge_offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE != 0) && (edge_offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE != 1) );
  assert( edge_offset < vertex->lightweight_edge_insert_offset );
  assert( (edge_orientation == GDI_EDGE_UNDIRECTED) || (edge_orientation == GDI_EDGE_INCOMING) || (edge_orientation == GDI_EDGE_OUTGOING) );

  uint8_t* metadata;
  GDA_LightweightEdgesGetMetadataPointerWithOffset( &metadata, vertex, edge_offset );
  /**
    trying to update a deleted edge
   */
  assert( *metadata != 0 );

  /**
    passed all tests
   */
  *metadata = (uint8_t) edge_orientation;
}


void GDA_LightweightEdgesSetDPointer( GDA_DPointer dpointer, uint32_t edge_offset, GDI_VertexHolder vertex ) {
  /**
    input validation
   */
  assert( vertex != NULL );
  assert( (edge_offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE != 0) && (edge_offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE != 1) );
  assert( edge_offset < vertex->lightweight_edge_insert_offset );

#ifndef NDEBUG
  uint8_t* metadata;
  GDA_LightweightEdgesGetMetadataPointerWithOffset( &metadata, vertex, edge_offset );
  /**
    trying to update a deleted edge
   */
  assert( *metadata != 0 );
#endif

  /**
    passed all tests
   */
  vertex->lightweight_edge_data[edge_offset] = dpointer;
}


void GDA_LightweightEdgesSetLabel( uint8_t label_int_handle, uint32_t edge_offset, GDI_VertexHolder vertex ) {
  /**
    input validation
   */
  assert( vertex != NULL );
  assert( (edge_offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE != 0) && (edge_offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE != 1) );
  assert( edge_offset < vertex->lightweight_edge_insert_offset );

  uint8_t* metadata;
  GDA_LightweightEdgesGetMetadataPointerWithOffset( &metadata, vertex, edge_offset );
  /**
    trying to update a deleted edge
   */
  assert( *metadata != 0 );

  /**
    passed all tests
   */
  *(metadata+8) = label_int_handle;
}


/**
  find an edge in the lightweight edge data structure of the other
  vertex

  return the first index that matches the edge orientation, label and
  the distributed address of the other vertex

  returns zero in case no matching edge is found
 */
uint32_t GDA_LightweightEdgesFindEdge( int original_edge_orientation, GDA_DPointer other_vertex, uint8_t label_int_handle, GDI_VertexHolder vertex ) {
  /**
    input validation
   */
  assert( vertex != NULL );
  assert( (original_edge_orientation == GDI_EDGE_UNDIRECTED) || (original_edge_orientation == GDI_EDGE_INCOMING) || (original_edge_orientation == GDI_EDGE_OUTGOING) );

  uint8_t edge_orientation;
  if( original_edge_orientation == GDI_EDGE_UNDIRECTED ) {
    edge_orientation = (uint8_t) GDI_EDGE_UNDIRECTED;
  } else {
    if( original_edge_orientation == GDI_EDGE_INCOMING ) {
      edge_orientation = (uint8_t) GDI_EDGE_OUTGOING;
    } else {
      /**
        original_edge_orientation should be GDI_EDGE_OUTGOING
       */
      edge_orientation = (uint8_t) GDI_EDGE_INCOMING;
    }
  }

  uint8_t* metadata_iterator = (uint8_t*) vertex->lightweight_edge_data;
  GDA_DPointer* dp_iterator =  &(vertex->lightweight_edge_data[2]);
  GDA_DPointer* max = &(vertex->lightweight_edge_data[vertex->lightweight_edge_insert_offset]);

  /**
    main loop
   */
  uint8_t metadata_offset = 0;
  while( dp_iterator < max ) {
    if( (edge_orientation & *metadata_iterator) && (*dp_iterator == other_vertex) && (*(metadata_iterator+8) == label_int_handle) ) {
      return ((char*)dp_iterator - (char*)vertex->lightweight_edge_data) / sizeof(GDA_DPointer);
    }
    dp_iterator++;
    metadata_iterator++;
    if( metadata_offset == 7 ) {
      /**
        go to the next block
       */
      dp_iterator += 2;
      metadata_iterator += GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES - sizeof(GDA_DPointer);
      metadata_offset = 0;
    } else {
      metadata_offset++;
    }
  }

  return 0;
}


void GDA_LightweightEdgesPrint( GDI_VertexHolder vertex ) {
  /**
    input validation
   */
  assert( vertex != NULL );

  /**
    initialisation
   */
  uint8_t* metadata_iterator;
  uint32_t offset, max_offset;
  offset = 2;
  max_offset = vertex->lightweight_edge_insert_offset;
  metadata_iterator = (uint8_t*) vertex->lightweight_edge_data;

  /**
    main loop
   */
  while( offset < max_offset ) {
    if( *metadata_iterator == 0 ) {
      printf("%5i|EMP|%3u|%20lu\n", vertex->transaction->db->commrank, *(metadata_iterator+8), vertex->lightweight_edge_data[offset]);
    }
    if( *metadata_iterator == 1 ) {
      printf("%5i|INC|%3u|%20lu\n", vertex->transaction->db->commrank, *(metadata_iterator+8), vertex->lightweight_edge_data[offset]);
    }
    if( *metadata_iterator == 2 ) {
      printf("%5i|OUT|%3u|%20lu\n", vertex->transaction->db->commrank, *(metadata_iterator+8), vertex->lightweight_edge_data[offset]);
    }
    if( *metadata_iterator == 3 ) {
      printf("%5i|UND|%3u|%20lu\n", vertex->transaction->db->commrank, *(metadata_iterator+8), vertex->lightweight_edge_data[offset]);
    }

    offset++;
    metadata_iterator++;
    if( offset % GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE == 0 ) {
      /**
        go to the next block
       */
      offset += 2;
      metadata_iterator += GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE_BYTES - sizeof(GDA_DPointer);
    }
  }
}
