// Copyright (c) 2023 ETH-Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#ifndef __LPG_GRAPH500_GRAPH_H
#define __LPG_GRAPH500_GRAPH_H

#include "make_graph.h"

void loadEdgesFromEdgeListFile( const char* inputFilename, const bool startAtOne, MPI_Offset* edge_count, packed_edge** edges );
void generateEdgeGraph500Kronecker( uint32_t edge_factor, uint32_t SCALE, MPI_Offset* edge_count, packed_edge** buf );
void createGraphDatabase( uint32_t block_size, uint64_t memory_size, uint64_t nglobalverts, MPI_Offset edge_count, packed_edge* buf, bool directed,
  GDI_Database* graph_db, GDI_Label** vertexlabels, GDI_Label** edgelabels, GDI_PropertyType** propertytypes );

#endif // __LPG_GRAPH500_GRAPH_H
