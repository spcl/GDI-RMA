// Copyright (c) 2022 ETH-Zurich.
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

#endif // __LPG_GRAPH500_GRAPH_H
