// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#include <assert.h>
#include <string.h>

#include "gdi.h"
#include "gda_block.h"
#include "gda_lightweight_edges.h"
#include "gda_vertex.h"

void GDA_AssociateVertex( GDI_Vertex_uid internal_uid, GDI_Transaction transaction, GDI_VertexHolder vertex ) {
  vertex->delete_flag = false;
  vertex->write_flag = false;
  vertex->creation_flag = false;

  GDA_vector_push_back( transaction->vertices, &vertex );

  /**
    insert the new vertex into the hash map that translates from the
    vertex UID to the local address of the GDI_VertexHolder object
   */
  GDA_hashmap_insert( transaction->v_translate_d2l, &internal_uid /* key */, &vertex /* value */ );

  /**
    create the list that keeps track of all edge objects, that are associated
    with the current vertex
   */
  GDA_list_create( &(vertex->edges), sizeof(GDI_EdgeHolder) /* element size */ );

  uint64_t block_size = transaction->db->block_size;
  char* buf = malloc( block_size );
  char* source_address;

  GDA_GetBlock( buf, internal_uid, transaction->db );
  RMA_Win_flush_all( transaction->db->win_blocks );

  /**
    read the segment meta data
   */
  uint32_t num_blocks = *(uint32_t*)(buf+GDA_OFFSET_NUM_BLOCKS);
  uint32_t num_edges = *(uint32_t*)(buf+GDA_OFFSET_NUM_LIGHTWEIGHT_EDGES);
  vertex->property_size = *(uint64_t*)(buf+GDA_OFFSET_SIZE_PROPERTY_DATA);
  vertex->unused_space = *(uint64_t*)(buf+GDA_OFFSET_SIZE_UNUSED_SPACE);

  /**
    initialise the data structure that keeps track of blocks associated with this vertex
    and add the primary block to that data structure

    allocate a few additional elements, so that we don't have to immediately resize
    once we add a new block
   */
  vertex->blocks->element_size = sizeof(GDA_DPointer);
  vertex->blocks->capacity = num_blocks + 8;
  vertex->blocks->data = realloc(vertex->blocks->data, vertex->blocks->capacity * vertex->blocks->element_size );
  vertex->blocks->size = num_blocks;

  *(uint64_t*)(vertex->blocks->data) = internal_uid;

  /**
    already added the primary block
   */
  num_blocks--;

  /**
    put the remaining block addresses in that data structure
   */
  uint32_t remaining_buf_data = block_size - GDA_VERTEX_METADATA_SIZE;
  uint64_t remaining_block_data = num_blocks * sizeof(GDA_DPointer);
  /**
    already fetched the primary block
   */
  uint32_t blk_cnt = 1;
  GDA_DPointer* dp = vertex->blocks->data;

  source_address = buf + GDA_VERTEX_METADATA_SIZE;
  if( remaining_block_data <= remaining_buf_data ) {
    /**
      entire block data is in the primary block
     */
    memcpy( ((char*)(vertex->blocks->data))+sizeof(GDA_DPointer), source_address, remaining_block_data );

    source_address += remaining_block_data;
    remaining_buf_data -= remaining_block_data;
  } else {
    /**
      block data is distributed in at least two blocks
     */
    memcpy( ((char*)(vertex->blocks->data))+sizeof(GDA_DPointer), source_address, remaining_buf_data );
    remaining_block_data -= remaining_buf_data;

    uint32_t last_flush = 0;
    /**
      round down

      next_flush is at least 1
     */
    uint32_t next_flush = remaining_buf_data/sizeof(GDA_DPointer);
    /**
      number of blocks that only contain block addresses,
      so we round down
     */
    uint32_t num_block_data_blocks = remaining_block_data/block_size;

    char* target_address = vertex->blocks->data;
    target_address += sizeof(GDA_DPointer) + remaining_buf_data;

    for( uint32_t i=0 ; i<num_block_data_blocks ; i++ ) {
      if( i == next_flush ) {
        /**
          fetched all possible blocks, so now we have to flush
          to get the next block addresses
         */
        RMA_Win_flush_all( transaction->db->win_blocks );

        next_flush += ((next_flush - last_flush) * block_size) / sizeof(GDA_DPointer);
        last_flush = i;
      }

      GDA_GetBlock( target_address, dp[blk_cnt++], transaction->db );
      target_address += block_size;
    }
    RMA_Win_flush_all( transaction->db->win_blocks );

    remaining_block_data -= num_block_data_blocks * block_size;

    if( remaining_block_data > 0 ) {
      /**
        next block contains block addresses and lightweight edges
        (and maybe property data)
       */
      GDA_GetBlock( buf, dp[blk_cnt++], transaction->db );
      RMA_Win_flush_all( transaction->db->win_blocks );

      /**
        unpack the data
       */
      memcpy( target_address, buf, remaining_block_data );

      source_address = buf+remaining_block_data;
      remaining_buf_data = block_size - remaining_block_data;
    } else {
      RMA_Win_flush_all( transaction->db->win_blocks ); // TODO: Do we need this?
      remaining_buf_data = 0;
    }
  }

  /**
    initialise the lightweight edge data
   */
  vertex->lightweight_edge_insert_offset = num_edges / (GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE-2) * GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE + num_edges % (GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE-2) + 2;
  /**
    we have to round up because we always have to allocate enough memory
    for a complete lightweight edge block

    add one more lightweight edge block, so that we don't have to immediately
    reallocate (in case the current lightweight edge block is completely full,
    there will two completely empty lightweight edge blocks at the end)
   */
  vertex->lightweight_edge_size = (num_edges / (GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE-2) + 1 /* round up */ + 1 /* one additional block of edges */) * GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE * sizeof(GDA_DPointer);
  vertex->lightweight_edge_data = malloc( vertex->lightweight_edge_size );

  /**
    number of Bytes that need to be copied/transfered for the lightweight edges
   */
  uint64_t remaining_lightweight_edge_data = vertex->lightweight_edge_insert_offset * sizeof(GDA_DPointer);

  if( (num_edges % (GDA_LIGHTWEIGHT_EDGES_BLOCK_SIZE-2)) == 0 ) {
    /**
      have to initialise the next lightweight edge block
     */
    vertex->lightweight_edge_data[vertex->lightweight_edge_insert_offset-2] = 0; /* meta data */
    vertex->lightweight_edge_data[vertex->lightweight_edge_insert_offset-1] = 0; /* label data */
    remaining_lightweight_edge_data -= 2 * sizeof(GDA_DPointer);
  }

  /**
    start copying/transfering the lightweight edge data
   */
  if( remaining_lightweight_edge_data > 0 ) {
    char* target_address = (char*) (vertex->lightweight_edge_data);

    /**
      check whether we need to unpack anything from an already fetched block
     */
    if( remaining_buf_data > 0 ) {
      if( remaining_lightweight_edge_data <= remaining_buf_data ) {
        memcpy( target_address, source_address, remaining_lightweight_edge_data );

        source_address += remaining_lightweight_edge_data;
        remaining_buf_data -= remaining_lightweight_edge_data;
        remaining_lightweight_edge_data = 0;
      } else {
        /**
          buffer only contains some of lightweight edge data
         */
        memcpy( target_address, source_address, remaining_buf_data );

        target_address += remaining_buf_data;
        remaining_lightweight_edge_data -= remaining_buf_data;
        remaining_buf_data = 0;
      }
    }

    /**
      fetch the lightweight edge data directly into the local data structure
     */
    uint32_t num_lightweight_edge_blocks = remaining_lightweight_edge_data/block_size;
    for( uint32_t i=0 ; i<num_lightweight_edge_blocks ; i++ ) {
      GDA_GetBlock( target_address, dp[blk_cnt++], transaction->db );

      target_address += block_size;
    }

    remaining_lightweight_edge_data -= num_lightweight_edge_blocks * block_size;

    if( remaining_lightweight_edge_data > 0 ) {
      /**
        the remaining lightweight edge data is stored together in a block
        with property data
       */
      GDA_GetBlock( buf, dp[blk_cnt++], transaction->db );
      RMA_Win_flush_all( transaction->db->win_blocks );

      memcpy( target_address, buf, remaining_lightweight_edge_data );

      source_address = buf + remaining_lightweight_edge_data;
      remaining_buf_data = block_size - remaining_lightweight_edge_data;
    }
  }

  /**
    initialise the property data
   */
  vertex->property_data = malloc( vertex->property_size );

  // TODO: not accurate
  uint64_t remaining_property_data = vertex->property_size;

  /**
    start copying/transfering the property data
   */
  if( remaining_property_data > 0 ) {
    char* target_address = (char*) (vertex->property_data);

    /**
      check whether we need to unpack anything from an already fetched block
     */
    if( remaining_property_data <= remaining_buf_data ) {
      memcpy( target_address, source_address, remaining_property_data );

      /**
        no need to set the other variables, since we are done
       */
      remaining_property_data = 0;
    } else {
      /**
        buffer only contains some of property data
       */
      memcpy( target_address, source_address, remaining_buf_data );

      target_address += remaining_buf_data;
      remaining_property_data -= remaining_buf_data;
    }

    /**
      fetch the property data directly into the local data structure
     */
    uint32_t num_property_blocks = remaining_property_data/block_size;

    for( uint32_t i=0 ; i<num_property_blocks ; i++ ) {
      GDA_GetBlock( target_address, dp[blk_cnt++], transaction->db );

      target_address += block_size;
    }

    remaining_property_data -= num_property_blocks * block_size;

    if( remaining_property_data > 0 ) {
      /**
        last block is only partially filled with property data
       */
      GDA_GetBlock( buf, dp[blk_cnt++], transaction->db );
      RMA_Win_flush_all( transaction->db->win_blocks );

      memcpy( target_address, buf, remaining_property_data );
    }
  }

  assert( blk_cnt == vertex->blocks->size );

  /**
    make sure that all data is properly in place
   */
  RMA_Win_flush_all( transaction->db->win_blocks );

  free( buf );
}
