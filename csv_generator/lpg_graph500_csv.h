// Copyright (c) 2021 ETH-Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#ifndef __LPG_GRAPH500_CSV_H
#define __LPG_GRAPH500_CSV_H

void lpg_graph500_csv( uint64_t nglobalverts, MPI_Offset edge_count, packed_edge* edges, const char* output_prefix );

#endif // __LPG_GRAPH500_CSV_H
