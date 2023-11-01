// Copyright (c) 2023 ETH-Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#ifndef __LPG_GRAPH500_BENCHMARK_H
#define __LPG_GRAPH500_BENCHMARK_H

#include "gdi.h"

void benchmark_bfs( GDI_Database db, GDI_Label* vlabels, uint64_t* bfs_roots, uint32_t num_measurements );
void benchmark_bi( GDI_Database db, GDI_Label* vlabels, GDI_Label* elabels, GDI_PropertyType* ptypes, uint64_t nglobal_verts, uint32_t num_measurements );
void benchmark_cdlp( GDI_Database db, /* GDI_Label* vlabels, */ uint64_t nglobalverts, uint32_t max_num_iterations, uint32_t num_measurements );
void benchmark_gnn( GDI_Database db, /* GDI_Label* vlabels, */ uint64_t nglobalverts, uint32_t num_layers, uint32_t num_features, uint32_t num_measurements );
void benchmark_k_hop( GDI_Database db, GDI_Label* vlabels, uint64_t* bfs_roots, uint32_t num_measurements );
void benchmark_lcc( GDI_Database db, /* GDI_Label* vlabels, */ uint64_t nglobalverts, uint32_t num_measurements );
void benchmark_linkbench( GDI_Database db, GDI_Label* vlabels, GDI_Label* elabels, GDI_PropertyType* ptypes, uint64_t nglobalverts, uint32_t num_measurements );
void benchmark_pagerank( GDI_Database db, /* GDI_Label* vlabels, */ uint64_t nglobalverts, double damping_factor, uint32_t num_iterations, uint32_t num_measurements );
void benchmark_wcc( GDI_Database db, /* GDI_Label* vlabels, */ uint64_t nglobalverts, uint32_t num_iterations, uint32_t num_measurements );

#endif // __LPG_GRAPH500_BENCHMARK_H
