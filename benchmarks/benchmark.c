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

void benchmark_bfs( GDI_Database db, GDI_Label* vlabels, uint64_t* bfs_roots, uint32_t num_measurements ) {
  int status;

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  LSB_Init( "gdi_bfs" /* project name */, 0 /* autoprofiling interval, deactivated */);

  MPI_Barrier( MPI_COMM_WORLD );

  GDI_Transaction transaction;

  for( uint32_t i=0 ; i<num_measurements ; i++ ) {
    uint8_t* depth;

    LSB_Set_Rparam_long( "root", bfs_roots[i] );
    LSB_Res(); /* start measurement */

    status = GDI_StartCollectiveTransaction( db, &transaction );
    assert( status == GDI_SUCCESS );

    /* determine vertex label corresponding to the application-level ID of the start vertex */
    uint8_t idx;
    for( idx=0 ; idx<VERTEX_LABEL_COUNT ; idx++ ) {
      if( bfs_roots[i] < vlabel_range[idx] ) {
        break;
      }
    }
    assert( idx < VERTEX_LABEL_COUNT );

    uint64_t* v_ids;
    size_t elem_cnt;

    int ret = nod_bfs_sort_u32( vlabels[idx], bfs_roots[i], transaction, &depth, &v_ids, &elem_cnt );

    if( ret != 0 ) {
      status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_ABORT );
      assert( status == GDI_SUCCESS );
    } else {
      status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_COMMIT );
      assert( status == GDI_SUCCESS );

      LSB_Rec(0 /* type */); /* finish measurement */
    }

    free( depth );
    free( v_ids );
  }

  LSB_Finalize();
}


void benchmark_k_hop( GDI_Database db, GDI_Label* vlabels, uint64_t* bfs_roots, uint32_t num_measurements ) {
  int status;

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  LSB_Init( "gdi_k_hop" /* project name */, 0 /* autoprofiling interval, deactivated */);

  MPI_Barrier( MPI_COMM_WORLD );

  GDI_Transaction transaction;

  for( uint8_t hop=1 ; hop<=4 ; hop++ ) {
    for( uint32_t i=0 ; i<num_measurements ; i++ ) {
      LSB_Set_Rparam_long( "root", bfs_roots[i] );
      LSB_Res(); /* start measurement */

      status = GDI_StartCollectiveTransaction( db, &transaction );
      assert( status == GDI_SUCCESS );

      /* determine vertex label corresponding to the application-level ID of the start vertex */
      uint8_t idx;
      for( idx=0 ; idx<VERTEX_LABEL_COUNT ; idx++ ) {
        if( bfs_roots[i] < vlabel_range[idx] ) {
          break;
        }
      }
      assert( idx < VERTEX_LABEL_COUNT );

      GDI_Vertex_uid* v_ids;
      size_t elem_cnt;

      int ret = nod_k_hop( vlabels[idx], bfs_roots[i], transaction, hop, &v_ids, &elem_cnt );

      if( ret != 0 ) {
        status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_ABORT );
        assert( status == GDI_SUCCESS );
      } else {
        status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_COMMIT );
        assert( status == GDI_SUCCESS );

        LSB_Rec(hop /* type */); /* finish measurement */

        free( v_ids );
      }
    }
  }

  LSB_Finalize();
}


/**
  code written by George Mitenkov
 */
void benchmark_cdlp( GDI_Database db, /* GDI_Label* vlabels, */ uint64_t nglobalverts, uint32_t max_num_iterations, uint32_t num_measurements ) {
  int status;

  LSB_Init( "gdi_cdlp" /* project name */, 0 /* autoprofiling interval, deactivated */);
  LSB_Set_Rparam_int( "max_num_iterations", max_num_iterations );

  MPI_Barrier( MPI_COMM_WORLD );

  GDI_Transaction transaction;

  for( uint32_t i=0 ; i<num_measurements ; i++ ) {
    uint64_t* labels;
    uint64_t* v_ids;
    size_t elem_cnt;

    LSB_Res(); /* start measurement */

    status = GDI_StartCollectiveTransaction( db, &transaction );
    assert( status == GDI_SUCCESS );

    int ret = nod_cdlp_nonblocking_sorted( nglobalverts, transaction, max_num_iterations, &labels, &v_ids, &elem_cnt );

    if( ret != 0 ) {
      status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_ABORT );
      assert( status == GDI_SUCCESS );
    } else {
      status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_COMMIT );
      assert( status == GDI_SUCCESS );

      LSB_Rec(0 /* type */); /* finish measurement */

#ifdef GDEBUG
      /**
        additional things to do:
        * ensure that labels is not freed, after the previous variant is run
        * use an additional buffer cmp_labels
       */
      if (i == 0) {
        for( size_t j=0 ; j<elem_cnt ; j++ ) {
          if( labels[j] != cmp_labels[j] ) {
            printf( "label[%" PRIu64 "] = %" PRIu64 " instead of %" PRIu64 "\n", v_ids[j], labels[j], cmp_labels[j] );
          }
        }
      }

      free( cmp_labels );
#endif

      free( labels );
      free( v_ids );
    }
  }

  LSB_Finalize();
}


/**
  code written by George Mitenkov
 */
void benchmark_pagerank( GDI_Database db, /* GDI_Label* vlabels, */ uint64_t nglobalverts, double damping_factor, uint32_t num_iterations, uint32_t num_measurements ) {
  int status;

  LSB_Init( "gdi_pr" /* project name */, 0 /* autoprofiling interval, deactivated */);
  LSB_Set_Rparam_int( "num_iterations", num_iterations );
  LSB_Set_Rparam_double( "damping_factor", damping_factor );

  MPI_Barrier( MPI_COMM_WORLD );

  GDI_Transaction transaction;

  for( uint32_t i=0 ; i<num_measurements ; i++ ) {
    double* scores;
    size_t elem_cnt;
    uint64_t* v_ids;

    LSB_Res(); /* start measurement */

    status = GDI_StartCollectiveTransaction( db, &transaction );
    assert( status == GDI_SUCCESS );

    int ret = nod_pagerank_nonblocking_sorted( nglobalverts, transaction, num_iterations, damping_factor, &scores, &v_ids, &elem_cnt );

    if( ret != 0 ) {
      status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_ABORT );
      assert( status == GDI_SUCCESS );
    } else {
      status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_COMMIT );
      assert( status == GDI_SUCCESS );

      LSB_Rec(0 /* type */); /* finish measurement */

#ifdef GDEBUG
      if (i == 0) {
        int rank, comm_size;
        MPI_Comm_rank( MPI_COMM_WORLD, &rank );
        MPI_Comm_size( MPI_COMM_WORLD, &comm_size );

        uint64_t local_size = (nglobalverts + comm_size - 1) / comm_size;
        uint64_t global_start_position = MIN( rank * local_size, nglobalverts );
        uint64_t local_num_verts = MIN( nglobalverts - global_start_position, local_size );

        for( uint64_t j=0 ; j<local_num_verts ; j++ ) {
          printf( "scores[%" PRIu64 "] = %.10f\n", global_start_position + j, scores[j] );
        }
      }
#endif

      free( scores );
    }
  }

  LSB_Finalize();
}


/**
  code written by George Mitenkov
 */
void benchmark_wcc( GDI_Database db, /* GDI_Label* vlabels, */ uint64_t nglobalverts, uint32_t num_iterations, uint32_t num_measurements ) {
  int status;

  LSB_Init( "gdi_wcc" /* project name */, 0 /* autoprofiling interval, deactivated */);
  LSB_Set_Rparam_int( "num_iterations", num_iterations );

  MPI_Barrier( MPI_COMM_WORLD );

  GDI_Transaction transaction;

  for( uint32_t i=0 ; i<num_measurements ; i++ ) {
    uint64_t* components;
    size_t elem_cnt;
    uint64_t* v_ids;

    LSB_Res(); /* start measurement */

    status = GDI_StartCollectiveTransaction( db, &transaction );
    assert( status == GDI_SUCCESS );

    int ret = nod_wcc_pull_nonblocking_sorted( nglobalverts, transaction, num_iterations, &components, &v_ids, &elem_cnt );

    if( ret != 0 ) {
      status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_ABORT );
      assert( status == GDI_SUCCESS );
    } else {
      status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_COMMIT );
      assert( status == GDI_SUCCESS );

      LSB_Rec(0 /* type */); /* finish measurement */

#ifdef GDEBUG
      /**
        additional things to do:
        * ensure that components is not freed, after the previous variant is run
        * use an additional buffer cmp_components
       */
      if( i == 0 ) {
        for( size_t j=0 ; j<elem_cnt ; j++ ) {
          if( components[j] != cmp_components[j] ) {
            printf( "components don't match for %" PRIu64 ": %" PRIu64 " and %" PRIu64 "\n", v_ids[j], components[j], cmp_components[j] );
          }
        }
      }

      free( cmp_components );
#endif

      free( components );
      free( v_ids );
    }
  }

  LSB_Finalize();
}


#define PRINT_Y_PRED(n, r, c)                                                     \
        uint64_t local_size = (n + c - 1) / c;                                    \
        uint64_t global_start_position = MIN( r * local_size, n );                \
        uint64_t local_num_verts = MIN( n - global_start_position, local_size );  \
        for( size_t j=0 ; j<local_num_verts ; j++ ) {                             \
          printf( "y_pred[%" PRIu64 "] = [ ", global_start_position + j);         \
          for( size_t k=0; k<num_features; k++ ) {                                \
            printf(" %.10f ", y_pred[j * num_features + k]);                      \
          }                                                                       \
          printf("]\n");                                                          \
        }

/**
  code written by George Mitenkov
 */
void benchmark_gnn( GDI_Database db, /* GDI_Label* vlabels, */ uint64_t nglobalverts, uint32_t num_layers, uint32_t num_features, uint32_t num_measurements ) {
  int status;

  LSB_Init( "gdi_gnn" /* project name */, 0 /* autoprofiling interval, deactivated */);
  LSB_Set_Rparam_int( "num_layers", num_layers );
  LSB_Set_Rparam_int( "num_features", num_features );

  int rank;
  MPI_Comm_rank( MPI_COMM_WORLD, &rank );

  /**
    genarate random weights and biases for each layer.
   */
  double* bias = malloc( num_layers * num_features * sizeof(double) );
  if( bias == NULL ) {
    fprintf( stderr, "%i - %s line %i: Not able to allocate memory\n", rank, (char*) __FILE__, __LINE__ );
    MPI_Abort( MPI_COMM_WORLD, 1 );
  }

  double* weights = malloc( num_layers * num_features * num_features * sizeof(double) );
  if( bias == NULL ) {
    fprintf( stderr, "%i - %s line %i: Not able to allocate memory\n", rank, (char*) __FILE__, __LINE__ );
    MPI_Abort( MPI_COMM_WORLD, 1 );
  }

  /* again, set weights and biases deterministicly for testing */
  for( uint32_t l=0 ; l<num_layers ; l++ ) {
    for( uint32_t i=0 ; i<num_features ; i++ ) {
      bias[l * num_features + i] = 0.1;
      for( uint32_t j=0 ; j<num_features ; j++ ) {
        weights[l * num_features * num_features + i * num_features + j] = 0.2;
      }
    }
  }

  MPI_Barrier( MPI_COMM_WORLD );

  GDI_Transaction transaction;

  for( uint32_t i=0 ; i<num_measurements ; i++ ) {
    double* y_pred;

    size_t elem_cnt;
    uint64_t* v_ids;

    LSB_Res(); /* start measurement */

    status = GDI_StartCollectiveTransaction( db, &transaction );
    assert( status == GDI_SUCCESS );

    int ret = nod_gnn_blocking_sorted( nglobalverts, transaction, num_layers, num_features, weights, bias, &y_pred, &v_ids, &elem_cnt );

    if( ret != 0 ) {
      status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_ABORT );
      assert( status == GDI_SUCCESS );
    } else {
      status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_COMMIT );
      assert( status == GDI_SUCCESS );

      LSB_Rec(2 /* type */); /* finish measurement */

#ifdef GDEBUG
      /**
        additional things to do:
        * ensure that y_pred is not freed after the previous variant is executed
        * use an additional buffer cmp_y_pred
       */
      if (i == 0) {
        double episolon = 0.00001;
        for( size_t j=0 ; j<elem_cnt ; j++ ) {
          for( uint32_t k=0; k<num_features; k++ ) {
            if( (cmp_y_pred[j * num_features + k] - y_pred[j * num_features + k]) >= episolon ) {
              printf( "blocked+sorted: y_pred[%" PRIu64 ",%u] = %.10f not identical with %.10f\n", v_ids[j], k, y_pred[j * num_features + k], cmp_y_pred[j * num_features + k] );
            }
          }
        }
      }

      free( cmp_y_pred );
#endif

      free( y_pred );
      free( v_ids );
    }
  }

  LSB_Finalize();

  /**
    clean up
   */
  free( weights );
  free( bias );
}
#undef PRINT_Y_PRED

/**
  code written by George Mitenkov
 */
void benchmark_lcc( GDI_Database db, /* GDI_Label* vlabels, */ uint64_t nglobalverts, uint32_t num_measurements ) {
  int status;

  LSB_Init( "gdi_lcc" /* project name */, 0 /* autoprofiling interval, deactivated */);

  MPI_Barrier( MPI_COMM_WORLD );

  GDI_Transaction transaction;

  for( uint32_t i=0 ; i<num_measurements ; i++ ) {
    double* coefficients;

    size_t elem_cnt;
    uint64_t* v_ids;

    LSB_Res(); /* start measurement */

    status = GDI_StartCollectiveTransaction( db, &transaction );
    assert( status == GDI_SUCCESS );

    int ret = nod_lcc( nglobalverts, transaction, &coefficients, &v_ids, &elem_cnt );

    if( ret != 0 ) {
      status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_ABORT );
      assert( status == GDI_SUCCESS );
    } else {
      status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_COMMIT );
      assert( status == GDI_SUCCESS );

      LSB_Rec(1 /* type */); /* finish measurement */

#ifdef GDEBUG
      if (i == 0) {
        for( size_t j=0 ; j<elem_cnt ; j++ ) {
          printf( "coefficients[%" PRIu64 "] = %.10f (NOD)\n", v_ids[j], coefficients[j] );
        }
      }
#endif

      free( coefficients );
      free( v_ids );
    }
  }

  LSB_Finalize();
}


void benchmark_bi( GDI_Database db, GDI_Label* vlabels, GDI_Label* elabels, GDI_PropertyType* ptypes, uint64_t nglobal_verts, uint32_t num_measurements ) {
  int status;

  LSB_Init( "gdi_bi" /* project name */, 0 /* autoprofiling interval, deactivated */);

  MPI_Barrier( MPI_COMM_WORLD );

  GDI_Transaction transaction;

  for( uint32_t i=0 ; i<num_measurements ; i++ ) {
    LSB_Res(); /* start measurement */

    status = GDI_StartCollectiveTransaction( db, &transaction );
    assert( status == GDI_SUCCESS );

    char* res_name;
    size_t* v_count;
    size_t elem_cnt;

    int ret = business_intelligence( vlabels, elabels, ptypes, nglobal_verts, transaction, db, db->comm, 100, 'c', &res_name, &v_count, &elem_cnt );

    if( ret != 0 ) {
      status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_ABORT );
      assert( status == GDI_SUCCESS );
    } else {
      status = GDI_CloseCollectiveTransaction( &transaction, GDI_TRANSACTION_COMMIT );
      assert( status == GDI_SUCCESS );

      LSB_Rec(0 /* type */); /* finish measurement */

#if 0
      if( i == 0 ) {
        for( size_t j=0 ; j<elem_cnt ; j++ ) {
          printf("%i: %s - %zu\n", db->commrank, res_name+j*101, v_count[j]);
        }

        uint64_t lcnt = elem_cnt;
        uint64_t gcnt;
        MPI_Reduce( &lcnt, &gcnt, 1, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD );

        if( db->commrank == 0 ) {
          printf( "%" PRIu64 " resource vertices returned\n", gcnt );
        }
      }
#endif

      free( res_name );
      free( v_count );
    }
  }

  LSB_Finalize();
}
