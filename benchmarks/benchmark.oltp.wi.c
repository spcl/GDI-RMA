// Copyright (c) 2023 ETH-Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#include <assert.h>
#include <stdio.h>
#include <time.h>

#include "rma.h"
#include "gdi.h"

#include "liblsb.h"

#include "data_scheme_1.h"
#include "queries.h"

#define WARMUP_TRESHOLD 100

void benchmark_linkbench( GDI_Database db, GDI_Label* vlabels, GDI_Label* elabels, GDI_PropertyType* ptypes, uint64_t nglobalverts, uint32_t num_measurements ) {
  int status;

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if( num_measurements <= WARMUP_TRESHOLD ) {
    if( rank == 0 ) {
      fprintf( stderr, "Number of queries (%u) is less than the number of queries for warmup (%i), so no measurements would be performed.\n", num_measurements, WARMUP_TRESHOLD );
    }
    MPI_Abort( MPI_COMM_WORLD, 1 );
  }

  srand( time(NULL) + rank );

  /**
    20% read/80% write workload from the G-Tran paper:
    Total 23 queries with equal prob
    20 % read => each 1.8%:
    - get vertex by property
    - get vertex by id
    - get adjacent vertices (incoming edges)
    - get adjacent vertices (outgoing edges)
    - get adjacent vertices (by edge label)
    => linkbench_get_*_vertex 9.1%
    - get edge by property
    - get edge by label
    - get edge by id
    - get edge labels for vertex (incoming edges)
    - get edge labels for vertex (outgoing edges)
    - get edge labels for vertex (either)
    => linkbench_get_edges 10.9%

    80% write => each 6.66%:
    - add vertex with properties
    - set vertex property
    - add vertex + edges
    => linkbench_add_vertex 20%
    - delete vertex by id
    => linkbench_delete_vertex 6.7%
    - update vertex property
    - remove vertex property
    => linkbench_update_vertex 13.3%
    - add edge
    - add edge with properties
    - set edge property
    - update edge property
    - delete edge by id
    - remove edge property
    => linkbench_add_edge 40%
   */
  uint8_t linkbench_query_count = 7;
  uint16_t* linkbench_query_probability = malloc( linkbench_query_count * sizeof(uint16_t) );
  linkbench_query_probability[0] =  91; /* linkbench_get_*_vertex */
  linkbench_query_probability[1] =  291; /* linkbench_add_vertex */
  linkbench_query_probability[2] =  358; /* linkbench_delete_vertex */
  linkbench_query_probability[3] =  491; /* linkbench_update_vertex */
  linkbench_query_probability[4] =  491; /* linkbench_count_edges */
  linkbench_query_probability[5] =  600; /* linkbench_get_edges */
  linkbench_query_probability[6] = 1000; /* linkbench_add_edge: added probability from delete edge */

#ifndef THROUGHPUT
  LSB_Init( "gdi_oltp.wi.lat" /* project name */, 0 /* autoprofiling interval, deactivated */);

  MPI_Barrier( MPI_COMM_WORLD ); /* start all queries more or less at the same time */
#else
  LSB_Init( "gdi_oltp.wi.tp" /* project name */, 0 /* autoprofiling interval, deactivated */);

  int failed_queries = 0;
  int deleted_vertices = 0;
  LSB_Set_Rparam_int( "num_queries", 0 );
  LSB_Set_Rparam_int( "failed_queries", 0 );
  LSB_Set_Rparam_int( "deleted_vertices", 0 );
#endif

  for( uint32_t i=0 ; i<num_measurements ; i++ ) {

#ifdef THROUGHPUT
  if( i == WARMUP_TRESHOLD /* warm up period is over */ ) {
    failed_queries = 0;
    deleted_vertices = 0;
    MPI_Barrier( MPI_COMM_WORLD );
    /**
      measure the latency of a single barrier, so that we can determine
      the overhead for the subsequent measurement
     */
    LSB_Res(); /* start measurement */
    MPI_Barrier( MPI_COMM_WORLD );
    LSB_Rec(0 /* barrier */); /* finish measurement */
    LSB_Set_Rparam_int( "num_queries", num_measurements - WARMUP_TRESHOLD );
    LSB_Res(); /* start measurement */
  }
#endif
    /* select query */
    uint16_t random_num = rand() % 1000;
    uint8_t query_idx;
    for( query_idx = 0; query_idx<linkbench_query_count ; query_idx++ ) {
      if( random_num < linkbench_query_probability[query_idx] ) {
        break;
      }
    }

    assert( query_idx < linkbench_query_count );

    switch( query_idx ) {
      case 0:
        {
          /* retrieve properties of vertex */
          uint64_t application_level_ID = (
            (((uint64_t) rand() <<  0) & 0x000000000000FFFFull) |
            (((uint64_t) rand() << 16) & 0x00000000FFFF0000ull) |
            (((uint64_t) rand() << 32) & 0x0000FFFF00000000ull) |
            (((uint64_t) rand() << 48) & 0xFFFF000000000000ull)
                                           ) % nglobalverts;

          uint8_t idx;
          for( idx=0 ; idx<VERTEX_LABEL_COUNT ; idx++ ) {
            if( application_level_ID < vlabel_range[idx] ) {
              break;
            }
          }

          assert( idx < VERTEX_LABEL_COUNT );

          int ret;
          GDI_Transaction transaction;
          if( idx == 0 ) {
            /* company */
            lb_prop_company_t* return_data;

#ifndef THROUGHPUT
            LSB_Res(); /* start measurement */
#endif

            status = GDI_StartTransaction( db, &transaction );
            assert( status == GDI_SUCCESS );

            ret = linkbench_get_company_vertex( vlabels[0], application_level_ID, ptypes, transaction, &return_data );

            if( ret != 0 ) {
              status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_ABORT );
              assert( status == GDI_SUCCESS );
#ifdef THROUGHPUT
              ret == 1 ? deleted_vertices++ : failed_queries++;
#endif
            } else {
              status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_COMMIT );
              assert( status == GDI_SUCCESS );

#ifndef THROUGHPUT
              LSB_Rec(0 /* query type */); /* finish measurement */
#endif

              linkbench_cleanup_prop_company( &return_data );
            }
          } else {
            if( idx == 1 ) {
              /* person */
              lb_prop_person_t* return_data;

#ifndef THROUGHPUT
              LSB_Res(); /* start measurement */
#endif

              status = GDI_StartTransaction( db, &transaction );
              assert( status == GDI_SUCCESS );

              ret = linkbench_get_person_vertex( vlabels[1], application_level_ID, ptypes, transaction, &return_data );

              if( ret != 0 ) {
                status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_ABORT );
                assert( status == GDI_SUCCESS );
#ifdef THROUGHPUT
                ret == 1 ? deleted_vertices++ : failed_queries++;
#endif
              } else {
                status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_COMMIT );
                assert( status == GDI_SUCCESS );

#ifndef THROUGHPUT
                LSB_Rec(0 /* query type */); /* finish measurement */
#endif

                linkbench_cleanup_prop_person( &return_data );
              }
            } else {
              if( idx == 2 ) {
                /* place */
                lb_prop_place_t* return_data;

#ifndef THROUGHPUT
                LSB_Res(); /* start measurement */
#endif

                status = GDI_StartTransaction( db, &transaction );
                assert( status == GDI_SUCCESS );

                ret = linkbench_get_place_vertex( vlabels[2], application_level_ID, ptypes, transaction, &return_data );

                if( ret != 0 ) {
                  status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_ABORT );
                  assert( status == GDI_SUCCESS );
#ifdef THROUGHPUT
                  ret == 1 ? deleted_vertices++ : failed_queries++;
#endif
                } else {
                  status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_COMMIT );
                  assert( status == GDI_SUCCESS );

#ifndef THROUGHPUT
                  LSB_Rec(0 /* query type */); /* finish measurement */
#endif

                  linkbench_cleanup_prop_place( &return_data );
                }
              } else {
                if( idx == 3 ) {
                  /* project */
                  lb_prop_project_t* return_data;

#ifndef THROUGHPUT
                  LSB_Res(); /* start measurement */
#endif

                  status = GDI_StartTransaction( db, &transaction );
                  assert( status == GDI_SUCCESS );

                  ret = linkbench_get_project_vertex( vlabels[3], application_level_ID, ptypes, transaction, &return_data );

                  if( ret != 0 ) {
                    status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_ABORT );
                    assert( status == GDI_SUCCESS );
#ifdef THROUGHPUT
                    ret == 1 ? deleted_vertices++ : failed_queries++;
#endif
                  } else {
                    status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_COMMIT );
                    assert( status == GDI_SUCCESS );

#ifndef THROUGHPUT
                    LSB_Rec(0 /* query type */); /* finish measurement */
#endif

                    linkbench_cleanup_prop_project( &return_data );
                  }
                } else {
                  /* ressource */
                  lb_prop_ressource_t* return_data;

#ifndef THROUGHPUT
                  LSB_Res(); /* start measurement */
#endif

                  status = GDI_StartTransaction( db, &transaction );
                  assert( status == GDI_SUCCESS );

                  ret = linkbench_get_ressource_vertex( vlabels[4], application_level_ID, ptypes, transaction, &return_data );

                  if( ret != 0 ) {
                    status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_ABORT );
                    assert( status == GDI_SUCCESS );
#ifdef THROUGHPUT
                    ret == 1 ? deleted_vertices++ : failed_queries++;
#endif
                  } else {
                    status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_COMMIT );
                    assert( status == GDI_SUCCESS );

#ifndef THROUGHPUT
                    LSB_Rec(0 /* query type */); /* finish measurement */
#endif

                    linkbench_cleanup_prop_ressource( &return_data );
                  }
                }
              }
            }
          }

          break;
        }
      case 1:
        {
          /* add vertex */
          GDI_Transaction transaction;

#ifndef THROUGHPUT
          LSB_Res(); /* start measurement */
#endif

          status = GDI_StartTransaction( db, &transaction );
          assert( status == GDI_SUCCESS );

          uint64_t application_level_ID = linkbench_add_vertex( vlabels, ptypes, nglobalverts, transaction );

          status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_COMMIT );
          assert( status == GDI_SUCCESS );

#ifndef THROUGHPUT
          LSB_Rec(1 /* query type */); /* finish measurement */
#endif

          break;
        }
      case 2:
        {
          /* delete vertex */
          uint64_t application_level_ID = (
            (((uint64_t) rand() <<  0) & 0x000000000000FFFFull) |
            (((uint64_t) rand() << 16) & 0x00000000FFFF0000ull) |
            (((uint64_t) rand() << 32) & 0x0000FFFF00000000ull) |
            (((uint64_t) rand() << 48) & 0xFFFF000000000000ull)
                                           ) % nglobalverts;

          uint8_t idx;
          for( idx=0 ; idx<VERTEX_LABEL_COUNT ; idx++ ) {
            if( application_level_ID < vlabel_range[idx] ) {
              break;
            }
          }

          assert( idx < VERTEX_LABEL_COUNT );

          int ret;
          GDI_Transaction transaction;

#ifndef THROUGHPUT
          LSB_Res(); /* start measurement */
#endif

          status = GDI_StartTransaction( db, &transaction );
          assert( status == GDI_SUCCESS );

          ret = linkbench_delete_vertex( vlabels[idx], application_level_ID, transaction );

          if( ret != 0 ) {
            status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_ABORT );
            assert( status == GDI_SUCCESS );
#ifdef THROUGHPUT
            ret == 1 ? deleted_vertices++ : failed_queries++;
#endif
          } else {
            status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_COMMIT );
            assert( status == GDI_SUCCESS );

#ifndef THROUGHPUT
            LSB_Rec(2 /* query type */); /* finish measurement */
#endif
          }

          break;
        }
      case 3:
        {
          /* update vertex properties */
          uint64_t application_level_ID = (
            (((uint64_t) rand() <<  0) & 0x000000000000FFFFull) |
            (((uint64_t) rand() << 16) & 0x00000000FFFF0000ull) |
            (((uint64_t) rand() << 32) & 0x0000FFFF00000000ull) |
            (((uint64_t) rand() << 48) & 0xFFFF000000000000ull)
                                           ) % nglobalverts;

          uint8_t idx;
          for( idx=0 ; idx<VERTEX_LABEL_COUNT ; idx++ ) {
            if( application_level_ID < vlabel_range[idx] ) {
              break;
            }
          }

          assert( idx < VERTEX_LABEL_COUNT );

          int ret;
          GDI_Transaction transaction;

#ifndef THROUGHPUT
          LSB_Res(); /* start measurement */
#endif

          status = GDI_StartTransaction( db, &transaction );
          assert( status == GDI_SUCCESS );

          ret = linkbench_update_vertex( vlabels[idx], application_level_ID, vlabels, ptypes, transaction );

          if( ret != 0 ) {
            status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_ABORT );
            assert( status == GDI_SUCCESS );
#ifdef THROUGHPUT
            ret == 1 ? deleted_vertices++ : failed_queries++;
#endif
          } else {
            status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_COMMIT );
            assert( status == GDI_SUCCESS );

#ifndef THROUGHPUT
            LSB_Rec(3 /* query type */); /* finish measurement */
#endif
          }

          break;
        }
      case 4:
        {
          /* count edges */
          uint64_t application_level_ID = (
            (((uint64_t) rand() <<  0) & 0x000000000000FFFFull) |
            (((uint64_t) rand() << 16) & 0x00000000FFFF0000ull) |
            (((uint64_t) rand() << 32) & 0x0000FFFF00000000ull) |
            (((uint64_t) rand() << 48) & 0xFFFF000000000000ull)
                                           ) % nglobalverts;

          uint8_t idx;
          for( idx=0 ; idx<VERTEX_LABEL_COUNT ; idx++ ) {
            if( application_level_ID < vlabel_range[idx] ) {
              break;
            }
          }

          assert( idx < VERTEX_LABEL_COUNT );

          uint8_t edge_idx = edge_matrix[idx][rand() % 5];
          size_t edge_count;

          int ret;
          GDI_Transaction transaction;

#ifndef THROUGHPUT
          LSB_Res(); /* start measurement */
#endif

          status = GDI_StartTransaction( db, &transaction );
          assert( status == GDI_SUCCESS );

          ret = linkbench_count_edges( vlabels[idx], application_level_ID, elabels[edge_idx], transaction, db, &edge_count );

          if( ret != 0 ) {
            status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_ABORT );
            assert( status == GDI_SUCCESS );
#ifdef THROUGHPUT
            ret == 1 ? deleted_vertices++ : failed_queries++;
#endif
          } else {
            status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_COMMIT );
            assert( status == GDI_SUCCESS );

#ifndef THROUGHPUT
            LSB_Rec(4 /* query type */); /* finish measurement */
#endif
          }

          break;
        }
      case 5:
        {
          /* retrieve edges */
          uint64_t application_level_ID = (
            (((uint64_t) rand() <<  0) & 0x000000000000FFFFull) |
            (((uint64_t) rand() << 16) & 0x00000000FFFF0000ull) |
            (((uint64_t) rand() << 32) & 0x0000FFFF00000000ull) |
            (((uint64_t) rand() << 48) & 0xFFFF000000000000ull)
                                           ) % nglobalverts;

          uint8_t idx;
          for( idx=0 ; idx<VERTEX_LABEL_COUNT ; idx++ ) {
            if( application_level_ID < vlabel_range[idx] ) {
              break;
            }
          }

          assert( idx < VERTEX_LABEL_COUNT );

          uint8_t edge_idx = edge_matrix[idx][rand() % 5];
          size_t edge_count;
          GDI_Edge_uid* edge_uids = NULL;

          int ret;
          GDI_Transaction transaction;

#ifndef THROUGHPUT
          LSB_Res(); /* start measurement */
#endif

          status = GDI_StartTransaction( db, &transaction );
          assert( status == GDI_SUCCESS );

          ret = linkbench_get_edges( vlabels[idx], application_level_ID, elabels[edge_idx], transaction, db, &edge_count, &edge_uids );

          if( ret != 0 ) {
            status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_ABORT );
            assert( status == GDI_SUCCESS );
#ifdef THROUGHPUT
            ret == 1 ? deleted_vertices++ : failed_queries++;
#endif
          } else {
            status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_COMMIT );
            assert( status == GDI_SUCCESS );

#ifndef THROUGHPUT
            LSB_Rec(5 /* query type */); /* finish measurement */
#endif
          }

          free( edge_uids );

          break;
        }
      case 6:
        {
          /* add edge */
          uint64_t origin_ID = (
            (((uint64_t) rand() <<  0) & 0x000000000000FFFFull) |
            (((uint64_t) rand() << 16) & 0x00000000FFFF0000ull) |
            (((uint64_t) rand() << 32) & 0x0000FFFF00000000ull) |
            (((uint64_t) rand() << 48) & 0xFFFF000000000000ull)
                               ) % nglobalverts;

          uint8_t origin_idx;
          for( origin_idx=0 ; origin_idx<VERTEX_LABEL_COUNT ; origin_idx++ ) {
            if( origin_ID < vlabel_range[origin_idx] ) {
              break;
            }
          }

          assert( origin_idx < VERTEX_LABEL_COUNT );

          uint64_t target_ID = (
            (((uint64_t) rand() <<  0) & 0x000000000000FFFFull) |
            (((uint64_t) rand() << 16) & 0x00000000FFFF0000ull) |
            (((uint64_t) rand() << 32) & 0x0000FFFF00000000ull) |
            (((uint64_t) rand() << 48) & 0xFFFF000000000000ull)
                               ) % nglobalverts;

          uint8_t target_idx;
          for( target_idx=0 ; target_idx<VERTEX_LABEL_COUNT ; target_idx++ ) {
            if( target_ID < vlabel_range[target_idx] ) {
              break;
            }
          }

          assert( target_idx < VERTEX_LABEL_COUNT );

          int ret;
          GDI_Transaction transaction;

#ifndef THROUGHPUT
          LSB_Res(); /* start measurement */
#endif

          status = GDI_StartTransaction( db, &transaction );
          assert( status == GDI_SUCCESS );

          ret = linkbench_add_edge( vlabels[origin_idx], origin_ID, vlabels[target_idx], target_ID, elabels[edge_matrix[origin_idx][target_idx]], transaction );

          if( ret != 0 ) {
            status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_ABORT );
            assert( status == GDI_SUCCESS );
#ifdef THROUGHPUT
            ret == 1 ? deleted_vertices++ : failed_queries++;
#endif
          } else {
            status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_COMMIT );
            assert( status == GDI_SUCCESS );

#ifndef THROUGHPUT
            LSB_Rec(6 /* query type */); /* finish measurement */
#endif
          }

          break;
        }
    }
  }

#ifdef THROUGHPUT
  MPI_Barrier( MPI_COMM_WORLD );
  LSB_Set_Rparam_int( "failed_queries", failed_queries );
  LSB_Set_Rparam_int( "deleted_vertices", deleted_vertices );
  LSB_Rec(1 /* queries */); /* finish measurement */
#endif

  LSB_Finalize();

  /**
    clean up
   */
  free( linkbench_query_probability );
}
