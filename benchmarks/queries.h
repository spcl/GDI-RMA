// Copyright (c) 2023 ETH-Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#ifndef __LPG_GRAPH500_QUERY_H
#define __LPG_GRAPH500_QUERY_H

GDI_Date create_birthdate_property();


/**
  Linkbench: obj_insert(ot, version, time, data)

  ignores version and time and generates the property data and
  label (=object type/ot) on the fly

  returns an application-level ID of the new vertex, which is
  bigger or equal to nglobal_verts
 */
uint64_t linkbench_add_vertex( GDI_Label* vlabels, GDI_PropertyType* ptypes, uint64_t nglobal_verts, GDI_Transaction transaction );


/**
  Linkbench: obj_delete(ot, id)

  return codes:
  0 - success
  1 - vertex with vlabel and application_level_ID was not found
  2 - transaction critical error
 */
int linkbench_delete_vertex( GDI_Label vlabel, uint64_t application_level_ID, GDI_Transaction transaction );


/**
  Linkbench: obj_update(ot, id, version, time, data)

  updates the firstName and lastName properties of a person vertex or
  the name property for all vertices with a different label

  ignores version and time and generates the new property data on the
  fly

  return codes:
  0 - success
  1 - vertex with vlabel and application_level_ID was not found
  2 - transaction critical error
 */
int linkbench_update_vertex( GDI_Label vlabel, uint64_t application_level_ID, GDI_Label* vlabels, GDI_PropertyType* ptypes, GDI_Transaction transaction );


/**
  Linkbench: obj_get(ot, id)

  only covers vertices with label 'Company'

  return codes:
  0 - success
  1 - vertex with vlabel and application_level_ID was not found
  2 - transaction critical error
 */
typedef struct lb_prop_company {
  char* name;
  char* type;
  uint64_t revenue;
} lb_prop_company_t;

int linkbench_get_company_vertex( GDI_Label vlabel, uint64_t application_level_ID, GDI_PropertyType* ptypes, GDI_Transaction transaction, lb_prop_company_t** return_data );
void linkbench_cleanup_prop_company( lb_prop_company_t** company );


/**
  Linkbench: obj_get(ot, id)

  only covers vertices with label 'Person'

  return codes:
  0 - success
  1 - vertex with vlabel and application_level_ID was not found
  2 - transaction critical error
 */
typedef struct lb_prop_person {
  char* firstName;
  char* lastName;
  char* email;
  GDI_Date birthday;
} lb_prop_person_t;

int linkbench_get_person_vertex( GDI_Label vlabel, uint64_t application_level_ID, GDI_PropertyType* ptypes, GDI_Transaction transaction, lb_prop_person_t** return_data );
void linkbench_cleanup_prop_person( lb_prop_person_t** person );


/**
  Linkbench: obj_get(ot, id)

  only covers vertices with label 'Place'

  return codes:
  0 - success
  1 - vertex with vlabel and application_level_ID was not found
  2 - transaction critical error
 */
typedef struct lb_prop_place {
  char* name;
  uint32_t longitude;
  uint32_t latitude;
} lb_prop_place_t;

int linkbench_get_place_vertex( GDI_Label vlabel, uint64_t application_level_ID, GDI_PropertyType* ptypes, GDI_Transaction transaction, lb_prop_place_t** return_data );
void linkbench_cleanup_prop_place( lb_prop_place_t** place );


/**
  Linkbench: obj_get(ot, id)

  only covers vertices with label 'Project'

  return codes:
  0 - success
  1 - vertex with vlabel and application_level_ID was not found
  2 - transaction critical error
 */
typedef struct lb_prop_project {
  char* name;
  uint32_t budget;
} lb_prop_project_t;

int linkbench_get_project_vertex( GDI_Label vlabel, uint64_t application_level_ID, GDI_PropertyType* ptypes, GDI_Transaction transaction, lb_prop_project_t** return_data );
void linkbench_cleanup_prop_project( lb_prop_project_t** project );


/**
  linkbench: obj_get(ot, id)

  only covers vertices with label 'Ressource'

  return codes:
  0 - success
  1 - vertex with vlabel and application_level_id was not found
  2 - transaction critical error
 */
typedef struct lb_prop_ressource {
  char* name;
  char* formula;
  uint32_t density;
  uint32_t meltingPoint;
} lb_prop_ressource_t;

int linkbench_get_ressource_vertex( GDI_Label vlabel, uint64_t application_level_ID, GDI_PropertyType* ptypes, GDI_Transaction transaction, lb_prop_ressource_t** return_data );
void linkbench_cleanup_prop_ressource( lb_prop_ressource_t** ressource );

/**
  linkbench: assoc_insert(at, id1, id2, vis, time, version, data)

  ignores version, time and visbility and GDI doesn't support
  properties for edges at the moment

  return codes:
  0 - success
  1 - one of the vertices was not found
  2 - transaction critical error
 */
int linkbench_add_edge( GDI_Label origin_label, uint64_t origin_ID, GDI_Label target_label, uint64_t target_ID, GDI_Label edge_label, GDI_Transaction transaction );


/**
  linkbench: assoc_count(at, id)

  return codes:
  0 - success
  1 - vertex with vlabel and application_level_ID was not found
  2 - transaction critical error
 */
int linkbench_count_edges( GDI_Label vlabel, uint64_t application_level_ID, GDI_Label edge_label, GDI_Transaction transaction, GDI_Database db, size_t* edge_count );


/**
  linkbench: assoc_range(at, id, max_time, limit)

  ignores max_time and limit

  return codes:
  0 - success
  1 - vertex with vlabel and application_level_ID was not found
  2 - transaction critical error
 */
int linkbench_get_edges( GDI_Label vlabel, uint64_t application_level_ID, GDI_Label edge_label, GDI_Transaction transaction, GDI_Database db, size_t* edge_count, GDI_Edge_uid** edge_uids );


int nod_bfs_sort_u32( GDI_Label vlabel, uint64_t start_vertex, GDI_Transaction transaction, uint8_t** depth, uint64_t** v_ids, size_t* elem_cnt );

int nod_k_hop( GDI_Label vlabel, uint64_t start_vertex, GDI_Transaction transaction, uint8_t k, GDI_Vertex_uid** v_ids, size_t* elem_cnt );


/**
  Runs the community detection with label propagation algorithm for
  'max_num_iterations' and returns assigned labels on successful execution.

  Implementation requires the graph to be directed.

  return value:
    0  - success
    1  - no vertices in database

  abort value:
    -2 - not enough memory
    -4 - transaction-critical error
 */
int nod_cdlp_nonblocking_sorted( uint64_t global_num_verts, GDI_Transaction transaction, uint32_t max_num_iterations, uint64_t** labels, uint64_t** v_ids, size_t* elem_cnt );


/**
  Runs the PageRank algorithm for 'num_iterations' and with a
  'damping_factor'. Returns assigned scores on successful execution.

  Implementation requires the graph to be directed.

  return value:
    0  - success
    1  - no vertices in database

  abort value:
    -2 - not enough memory
    -4 - transaction-critical error
 */
int nod_pagerank_nonblocking_sorted( uint64_t global_num_verts, GDI_Transaction transaction, uint32_t num_iterations, double damping_factor, double** scores, uint64_t** v_ids, size_t* elem_cnt );


/**
  Runs the weakly connected components algorithm for `max_num_iterations`
  and returns assigned components on successful execution.

  Implementation requires the graph to be undirected.

  return value:
    0  - success
    1  - no vertices in database

  abort value:
    -2 - not enough memory
    -4 - transaction-critical error
 */
int nod_wcc_pull_nonblocking_sorted( uint64_t global_num_verts, GDI_Transaction transaction, uint32_t max_num_iterations, uint64_t** components, uint64_t** v_ids, size_t* elem_cnt );


/**
  Runs inference on a simple Graph Convolutional Network (GCN) with `num_layers`
  layers. The network is parametrized by a number of features, N, each vertex holds
  (as a GDI property).
  Additionally, for each layer takes pre-computed weights (N x N x num_layers) and
  biases (N x num_layers).
  Returns `y_pred` - tesult of the inference algorithm.

  Any GCN can be defined using two primitives: aggregate function (combine inputs from
  the neighbors) and update function (just like in MLP). In our implementation, we use

    aggregate:  sum of inputs with symmetric normalization  ,
    update:     ReLU(W*m + b)                               , m is result of aggregation.

  Implementation requires the graph to be directed and assumes that GDI property
  representing vertex features are correclty initialized. Also, we assume that the
  input graph ALREADY HAS self-loops.

  return value:
    0  - success
    1  - no vertices in database

  abort value:
    -2 - not enough memory
    -4 - transaction-critical error
 */
int nod_gnn_blocking_sorted( uint64_t global_num_verts, GDI_Transaction transaction, uint32_t num_layers, uint32_t num_features, double* weights, double* bias, double** y_pred, uint64_t** v_ids, size_t* elem_cnt );


/**
  Computes the local clustering coefficient of each vertex in the graph and
  returns it in `coefficients`. Under the hood, this algorithm uses triangle
  counting.

  Implementation requires the graph to be undirected, and assumes no self-loops
  and multiple edges.

  return value:
    0  - success
    1  - no vertices in database

  abort value:
    -2 - not enough memory
    -4 - transaction-critical error
 */
int nod_lcc( uint64_t global_num_verts, GDI_Transaction transaction, double** coefficients, uint64_t** v_ids, size_t* elem_cnt );

/**
  retrieve the resource vertices that are used by projects whose name
  starts with a character identical to the letter argument and the number
  of projects for a given resource vertex that fulfill that constraint

  letter should be in the range of 'a' to 'z' or 'A' to 'Z' and the same
  across all ranks.

  return codes:
  2: argument letter is not a..z or A..Z

  abort codes:
  -2: not enough memory
  -4: transaction critical error occured
 */
int business_intelligence( GDI_Label* vlabels, GDI_Label* elabels, GDI_PropertyType* ptypes, uint64_t nglobal_verts,
  GDI_Transaction transaction, GDI_Database db, MPI_Comm comm, size_t limit, char letter, char** res_name, size_t** v_count,
  size_t* elem_cnt );

#endif // __LPG_GRAPH500_QUERY_H
