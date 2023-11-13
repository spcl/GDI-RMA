// Copyright (c) 2023 ETH-Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#include <assert.h>
#include <ctype.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

#include "gdi.h"
#include "data_scheme_1.h"
#include "queries.h"
#include "radix7.h"

#define ASSOC_TRESHOLD 50000

/**
  macro source code by George Mitenkov
 */
#define bound_memory( counter )           \
  counter++;                              \
  if( counter == ASSOC_TRESHOLD ) {       \
    GDA_PurgeBuffer( transaction );       \
    counter = 0;                          \
  }

/**
  the following code was written by Tomasz Czajęcki
  and adapted by Robert Gerstenberger
 */

/* changes to this code should also be reflected in the CSV version */
GDI_Date create_birthdate_property() {
#ifndef NDEBUG
  int status;
#endif
  GDI_Date value;

  uint16_t year = get_random_uint16_t( 1900, 2000 );
  uint8_t month = get_random_uint8_t( 1, 12 );
  // TODO: too simple, but should always provide a valid date
  uint8_t day = get_random_uint8_t( 1, 28 );

#ifndef NDEBUG
  status = GDI_SetDate( year, month, day, &value );
#else
  GDI_SetDate( year, month, day, &value );
#endif
  assert( status == GDI_SUCCESS );

  return value;
}

/**
  end of code written by Tomasz Czajęcki
 */


void GDA_PurgeBuffer( GDI_Transaction transaction ) {
  assert( transaction->type == GDI_COLLECTIVE_TRANSACTION );

  size_t vec_size = transaction->vertices->size;
  for( size_t i=0 ; i<vec_size ; i++ ) {
    GDI_VertexHolder vertex = *(GDI_VertexHolder*)GDA_vector_at( transaction->vertices, i );
    free( vertex->property_data );
    free( vertex->lightweight_edge_data );

    GDA_list_free( &(vertex->edges) );
    GDA_vector_free( &(vertex->blocks) );
    free( vertex );
  }
  transaction->vertices->size = 0;

  GDA_hashmap_free( &(transaction->v_translate_d2l) );
  GDA_hashmap_create( &(transaction->v_translate_d2l), sizeof(GDA_DPointer) /* key size */, 32 /* capacity */, sizeof(void*) /* value size */, &GDA_int64_to_int );
}


uint64_t linkbench_add_vertex( GDI_Label* vlabels, GDI_PropertyType* ptypes, uint64_t nglobal_verts, GDI_Transaction transaction ) {
  int status;

  uint64_t application_level_ID;
  do {
    application_level_ID = create_uint64_property( 0xFFFFFFFFFFFFFFFF );
  } while( application_level_ID < nglobal_verts );

  uint8_t idx = rand() % VERTEX_LABEL_COUNT;

  GDI_VertexHolder vertex;
  status = GDI_CreateVertex( &application_level_ID, 8 /* size */, transaction, &vertex );
  assert( status == GDI_SUCCESS );

  status = GDI_AddLabelToVertex( vlabels[idx], vertex );
  assert( status == GDI_SUCCESS );

  /* add properties */
  if( idx == 0 ) {
    /* company */

    int numBytes = rand() % 100;
    char* string = create_string_property( numBytes );
    status = GDI_AddPropertyToVertex( string, numBytes, ptypes[0] /* name */, vertex );
    assert( status == GDI_SUCCESS );
    free( string );

    /* TODO: could be improved by having a few distinct types */
    numBytes = rand() % 10;
    string = create_string_property( numBytes );
    status = GDI_AddPropertyToVertex( string, numBytes, ptypes[1] /* type */, vertex );
    assert( status == GDI_SUCCESS );
    free( string );

    uint64_t num = create_uint64_property( 1000000000 /* at most 10.000.000,00 of some currency */ );
    status = GDI_AddPropertyToVertex( &num, 1, ptypes[2] /* revenue */, vertex );
    assert( status == GDI_SUCCESS );
  } else {
    if( idx == 1 ) {
      /* person */

      int numBytes = rand() % 100;
      char* string = create_string_property( numBytes );
      status = GDI_AddPropertyToVertex( string, numBytes, ptypes[3] /* firstName */, vertex );
      assert( status == GDI_SUCCESS );
      free( string );

      numBytes = rand() % 100;
      string = create_string_property( numBytes );
      status = GDI_AddPropertyToVertex( string, numBytes, ptypes[4] /* lastName */, vertex );
      assert( status == GDI_SUCCESS );
      free( string );

      numBytes = rand() % 1000;
      string = create_string_property( numBytes );
      status = GDI_AddPropertyToVertex( string, numBytes, ptypes[5] /* email */, vertex );
      assert( status == GDI_SUCCESS );
      free( string );

      GDI_Date birthdate = create_birthdate_property();
      status = GDI_AddPropertyToVertex( &birthdate, 1, ptypes[6] /* birthday */, vertex );
      assert( status == GDI_SUCCESS );
    } else {
      if( idx == 2 ) {
        /* place */

        int numBytes = rand() % 100;
        char* string = create_string_property( numBytes );
        status = GDI_AddPropertyToVertex( string, numBytes, ptypes[0] /* name */, vertex );
        assert( status == GDI_SUCCESS );
        free( string );

        uint32_t num = create_uint32_property( 12960000 /* max: 360 degree * 60 minutes * 60 seconds * 10 fraction */ );
        status = GDI_AddPropertyToVertex( &num, 1, ptypes[7] /* longitude */, vertex );
        assert( status == GDI_SUCCESS );

        num = create_uint32_property( 6480000 /* 180 degree * 60 minutes * 60 seconds * 10 fraction */ );
        status = GDI_AddPropertyToVertex( &num, 1, ptypes[8] /* latitude */, vertex );
        assert( status == GDI_SUCCESS );
      } else {
        if( idx == 3 ) {
          /* project */

          int numBytes = rand() % 100;
          char* string = create_string_property( numBytes );
          status = GDI_AddPropertyToVertex( string, numBytes, ptypes[0] /* name */, vertex );
          assert( status == GDI_SUCCESS );
          free( string );

          uint32_t num = create_uint32_property( 0xFFFFFFFF );
          status = GDI_AddPropertyToVertex( &num, 1, ptypes[9] /* budget */, vertex );
          assert( status == GDI_SUCCESS );
        } else {
          /* ressource */
          assert( idx == 4 );

          int numBytes = rand() % 100;
          char* string = create_string_property( numBytes );
          status = GDI_AddPropertyToVertex( string, numBytes, ptypes[0] /* name */, vertex );
          assert( status == GDI_SUCCESS );
          free( string );

          numBytes = rand() % 100;
          string = create_string_property( numBytes );
          status = GDI_AddPropertyToVertex( string, numBytes, ptypes[12] /* formula */, vertex );
          assert( status == GDI_SUCCESS );
          free( string );

          uint32_t num = create_uint32_property( 1000000 /* in g/L */ );
          status = GDI_AddPropertyToVertex( &num, 1, ptypes[10] /* density */, vertex );
          assert( status == GDI_SUCCESS );

          num = create_uint32_property( 100000 /* in 0.01 K */ );
          status = GDI_AddPropertyToVertex( &num, 1, ptypes[11] /* meltingPoint */, vertex );
          assert( status == GDI_SUCCESS );
        }
      }
    }
  }

  return application_level_ID;
}


int linkbench_delete_vertex( GDI_Label vlabel, uint64_t application_level_ID, GDI_Transaction transaction ) {
  int status;

  bool found_flag;
  GDI_Vertex_uid vertex_uid;
  status = GDI_TranslateVertexID( &found_flag, &vertex_uid, vlabel, &application_level_ID, sizeof(uint64_t), transaction );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  if( !found_flag ) {
    return 1;
  }

  GDI_VertexHolder vertex;
  status = GDI_AssociateVertex( vertex_uid, transaction, &vertex );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  status = GDI_FreeVertex( &vertex );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  return 0;
}


int linkbench_update_vertex( GDI_Label vlabel, uint64_t application_level_ID, GDI_Label* vlabels, GDI_PropertyType* ptypes, GDI_Transaction transaction ) {
   int status;

  bool found_flag;
  GDI_Vertex_uid vertex_uid;
  status = GDI_TranslateVertexID( &found_flag, &vertex_uid, vlabel, &application_level_ID, sizeof(uint64_t), transaction );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  if( !found_flag ) {
    return 1;
  }

  GDI_VertexHolder vertex;
  status = GDI_AssociateVertex( vertex_uid, transaction, &vertex );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  if( vlabel == vlabels[1] ) {
    /* person */

    status = GDI_RemovePropertiesFromVertex( ptypes[3] /* firstName */, vertex );
    assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
    if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
      return 2;
    }

    status = GDI_RemovePropertiesFromVertex( ptypes[4] /* lastName */, vertex );
    assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
    if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
      return 2;
    }

    int numBytes = rand() % 100;
    char* string = create_string_property( numBytes );
    status = GDI_AddPropertyToVertex( string, numBytes, ptypes[3] /* firstName */, vertex );
    assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
    if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
      return 2;
    }
    free( string );

    numBytes = rand() % 100;
    string = create_string_property( numBytes );
    status = GDI_AddPropertyToVertex( string, numBytes, ptypes[4] /* lastName */, vertex );
    assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
    if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
      return 2;
    }
    free( string );
  } else {
    status = GDI_RemovePropertiesFromVertex( ptypes[0] /* name */, vertex );
    assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
    if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
      return 2;
    }

    int numBytes = rand() % 100;
    char* string = create_string_property( numBytes );
    status = GDI_AddPropertyToVertex( string, numBytes, ptypes[0] /* name */, vertex );
    assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
    if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
      return 2;
    }
    free( string );
  }

  return 0;
}


int linkbench_get_company_vertex( GDI_Label vlabel, uint64_t application_level_ID, GDI_PropertyType* ptypes, GDI_Transaction transaction, lb_prop_company_t** return_data ) {
  int status;

  bool found_flag;
  GDI_Vertex_uid vertex_uid;
  status = GDI_TranslateVertexID( &found_flag, &vertex_uid, vlabel, &application_level_ID, sizeof(uint64_t), transaction );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  if( !found_flag ) {
    return 1;
  }

  GDI_VertexHolder vertex;
  status = GDI_AssociateVertex( vertex_uid, transaction, &vertex );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  *return_data = malloc( sizeof(lb_prop_company_t) );
  (*return_data)->name = malloc( (100 + 1) * sizeof(char) );
  (*return_data)->type = malloc( (10 + 1) * sizeof(char) );

  size_t resultcount, offset_resultcount;
  size_t array_of_offsets[2];

  status = GDI_GetPropertiesOfVertex( (*return_data)->name, 101 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[0] /* name */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) );

  status = GDI_GetPropertiesOfVertex( (*return_data)->type, 11 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[1] /* type */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) );

  status = GDI_GetPropertiesOfVertex( &((*return_data)->revenue), 1 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[2] /* revenue */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) && (resultcount == 1) );

  return 0;
}


int linkbench_get_person_vertex( GDI_Label vlabel, uint64_t application_level_ID, GDI_PropertyType* ptypes, GDI_Transaction transaction, lb_prop_person_t** return_data ) {
  int status;

  bool found_flag;
  GDI_Vertex_uid vertex_uid;
  status = GDI_TranslateVertexID( &found_flag, &vertex_uid, vlabel, &application_level_ID, sizeof(uint64_t), transaction );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  if( !found_flag ) {
    return 1;
  }

  GDI_VertexHolder vertex;
  status = GDI_AssociateVertex( vertex_uid, transaction, &vertex );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  *return_data = malloc( sizeof(lb_prop_person_t) );
  (*return_data)->firstName = malloc( (100 + 1) * sizeof(char) );
  (*return_data)->lastName = malloc( (100 + 1) * sizeof(char) );
  (*return_data)->email = malloc( (1000 + 1) * sizeof(char) );

  size_t resultcount, offset_resultcount;
  size_t array_of_offsets[2];

  status = GDI_GetPropertiesOfVertex( (*return_data)->firstName, 101 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[3] /* firstName */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) );

  status = GDI_GetPropertiesOfVertex( (*return_data)->lastName, 101 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[4] /* lastName */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) );

  status = GDI_GetPropertiesOfVertex( (*return_data)->email, 1001 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[5] /* email */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) );

  status = GDI_GetPropertiesOfVertex( &((*return_data)->birthday), 1 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[6] /* birthday */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) && (resultcount == 1) );

  return 0;
}


int linkbench_get_place_vertex( GDI_Label vlabel, uint64_t application_level_ID, GDI_PropertyType* ptypes, GDI_Transaction transaction, lb_prop_place_t** return_data ) {
  int status;

  bool found_flag;
  GDI_Vertex_uid vertex_uid;
  status = GDI_TranslateVertexID( &found_flag, &vertex_uid, vlabel, &application_level_ID, sizeof(uint64_t), transaction );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  if( !found_flag ) {
    return 1;
  }

  GDI_VertexHolder vertex;
  status = GDI_AssociateVertex( vertex_uid, transaction, &vertex );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  *return_data = malloc( sizeof(lb_prop_place_t) );
  (*return_data)->name = malloc( (100 + 1) * sizeof(char) );

  size_t resultcount, offset_resultcount;
  size_t array_of_offsets[2];

  status = GDI_GetPropertiesOfVertex( (*return_data)->name, 101 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[0] /* name */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) );

  status = GDI_GetPropertiesOfVertex( &((*return_data)->longitude), 1 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[7] /* longitude */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) && (resultcount == 1) );

  status = GDI_GetPropertiesOfVertex( &((*return_data)->latitude), 1 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[8] /* latitude */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) && (resultcount == 1) );

  return 0;
}


int linkbench_get_project_vertex( GDI_Label vlabel, uint64_t application_level_ID, GDI_PropertyType* ptypes, GDI_Transaction transaction, lb_prop_project_t** return_data ) {
  int status;

  bool found_flag;
  GDI_Vertex_uid vertex_uid;
  status = GDI_TranslateVertexID( &found_flag, &vertex_uid, vlabel, &application_level_ID, sizeof(uint64_t), transaction );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  if( !found_flag ) {
    return 1;
  }

  GDI_VertexHolder vertex;
  status = GDI_AssociateVertex( vertex_uid, transaction, &vertex );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  *return_data = malloc( sizeof(lb_prop_project_t) );
  (*return_data)->name = malloc( (100 + 1) * sizeof(char) );

  size_t resultcount, offset_resultcount;
  size_t array_of_offsets[2];

  status = GDI_GetPropertiesOfVertex( (*return_data)->name, 101 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[0] /* name */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) );

  status = GDI_GetPropertiesOfVertex( &((*return_data)->budget), 1 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[9] /* budget */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) && (resultcount == 1) );

  return 0;
}


int linkbench_get_ressource_vertex( GDI_Label vlabel, uint64_t application_level_ID, GDI_PropertyType* ptypes, GDI_Transaction transaction, lb_prop_ressource_t** return_data ) {
  int status;

  bool found_flag;
  GDI_Vertex_uid vertex_uid;
  status = GDI_TranslateVertexID( &found_flag, &vertex_uid, vlabel, &application_level_ID, sizeof(uint64_t), transaction );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  if( !found_flag ) {
    return 1;
  }

  GDI_VertexHolder vertex;
  status = GDI_AssociateVertex( vertex_uid, transaction, &vertex );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  *return_data = malloc( sizeof(lb_prop_ressource_t) );
  (*return_data)->name = malloc( (100 + 1) * sizeof(char) );
  (*return_data)->formula = malloc( (100 + 1) * sizeof(char) );

  size_t resultcount, offset_resultcount;
  size_t array_of_offsets[2];

  status = GDI_GetPropertiesOfVertex( (*return_data)->name, 101 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[0] /* name */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) );

  status = GDI_GetPropertiesOfVertex( (*return_data)->formula, 101 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[12] /* formula */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) );

  status = GDI_GetPropertiesOfVertex( &((*return_data)->density), 1 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[10] /* density */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) && (resultcount == 1) );

  status = GDI_GetPropertiesOfVertex( &((*return_data)->meltingPoint), 1 /* buf size */, &resultcount,
    array_of_offsets, 2 /* offset count */, &offset_resultcount, ptypes[11] /* meltingPoint */, vertex );
  assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) && (resultcount == 1) );

  return 0;
}


void linkbench_cleanup_prop_company( lb_prop_company_t** company ) {
  free( (*company)->name );
  free( (*company)->type );

  free( *company );
}


void linkbench_cleanup_prop_person( lb_prop_person_t** person ) {
  free( (*person)->firstName );
  free( (*person)->lastName );
  free( (*person)->email );

  free( *person );
}


void linkbench_cleanup_prop_place( lb_prop_place_t** place ) {
  free( (*place)->name );

  free( *place );
}


void linkbench_cleanup_prop_project( lb_prop_project_t** project ) {
  free( (*project)->name );

  free( *project );
}


void linkbench_cleanup_prop_ressource( lb_prop_ressource_t** ressource ) {
  free( (*ressource)->name );
  free( (*ressource)->formula );

  free( *ressource );
}


int linkbench_add_edge( GDI_Label origin_label, uint64_t origin_ID, GDI_Label target_label, uint64_t target_ID, GDI_Label edge_label, GDI_Transaction transaction ) {
  int status;

  bool found_flag;
  GDI_Vertex_uid vertex_uid;
  status = GDI_TranslateVertexID( &found_flag, &vertex_uid, origin_label, &origin_ID, sizeof(uint64_t), transaction );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  if( !found_flag ) {
    return 1;
  }

  GDI_VertexHolder v_origin;
  status = GDI_AssociateVertex( vertex_uid, transaction, &v_origin );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  status = GDI_TranslateVertexID( &found_flag, &vertex_uid, target_label, &target_ID, sizeof(uint64_t), transaction );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  if( !found_flag ) {
    return 1;
  }

  GDI_VertexHolder v_target;
  status = GDI_AssociateVertex( vertex_uid, transaction, &v_target );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  GDI_EdgeHolder edge;
  status = GDI_CreateEdge( GDI_EDGE_UNDIRECTED, v_origin, v_target, &edge );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  status = GDI_AddLabelToEdge( edge_label, edge );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  return 0;
}


int linkbench_count_edges( GDI_Label vlabel, uint64_t application_level_ID, GDI_Label edge_label, GDI_Transaction transaction, GDI_Database db, size_t* edge_count ) {
  int status;

  bool found_flag;
  GDI_Vertex_uid vertex_uid;
  status = GDI_TranslateVertexID( &found_flag, &vertex_uid, vlabel, &application_level_ID, sizeof(uint64_t), transaction );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  if( !found_flag ) {
    return 1;
  }

  GDI_VertexHolder vertex;
  status = GDI_AssociateVertex( vertex_uid, transaction, &vertex );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  /**
    create constraint
   */
  GDI_Constraint constraint;
  GDI_Subconstraint subconstraint;

  status = GDI_CreateConstraint( db, &constraint );
  assert( status == GDI_SUCCESS );
  status = GDI_CreateSubconstraint( db, &subconstraint );
  assert( status == GDI_SUCCESS );
  status = GDI_AddLabelConditionToSubconstraint( edge_label, GDI_EQUAL, subconstraint );
  assert( status == GDI_SUCCESS );
  status = GDI_AddSubconstraintToConstraint( subconstraint, constraint );
  assert( status == GDI_SUCCESS );

  status = GDI_GetEdgesOfVertex( NULL, 0, edge_count, constraint, GDI_EDGE_UNDIRECTED, vertex );
  assert( status == GDI_SUCCESS );

  /**
    clean up
   */
  status = GDI_FreeSubconstraint( &subconstraint );
  assert( status == GDI_SUCCESS );
  status = GDI_FreeConstraint( &constraint );
  assert( status == GDI_SUCCESS );

  return 0;
}


int linkbench_get_edges( GDI_Label vlabel, uint64_t application_level_ID, GDI_Label edge_label, GDI_Transaction transaction, GDI_Database db, size_t* edge_count, GDI_Edge_uid** edge_uids ) {
  int status;

  bool found_flag;
  GDI_Vertex_uid vertex_uid;
  status = GDI_TranslateVertexID( &found_flag, &vertex_uid, vlabel, &application_level_ID, sizeof(uint64_t), transaction );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  if( !found_flag ) {
    return 1;
  }

  GDI_VertexHolder vertex;
  status = GDI_AssociateVertex( vertex_uid, transaction, &vertex );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  /**
    create constraint
   */
  GDI_Constraint constraint;
  GDI_Subconstraint subconstraint;

  status = GDI_CreateConstraint( db, &constraint );
  assert( status == GDI_SUCCESS );
  status = GDI_CreateSubconstraint( db, &subconstraint );
  assert( status == GDI_SUCCESS );
  status = GDI_AddLabelConditionToSubconstraint( edge_label, GDI_EQUAL, subconstraint );
  assert( status == GDI_SUCCESS );
  status = GDI_AddSubconstraintToConstraint( subconstraint, constraint );
  assert( status == GDI_SUCCESS );

  *edge_uids = malloc( 32 * sizeof(GDI_Edge_uid) );
  status = GDI_GetEdgesOfVertex( *edge_uids, 32, edge_count, constraint, GDI_EDGE_UNDIRECTED, vertex );
  if( status == GDI_ERROR_TRUNCATE ) {
    status = GDI_GetEdgesOfVertex( NULL, 0, edge_count, constraint, GDI_EDGE_UNDIRECTED, vertex );
    assert( status == GDI_SUCCESS );
    *edge_uids = realloc( *edge_uids, *edge_count * sizeof(GDI_Edge_uid) );
    status = GDI_GetEdgesOfVertex( *edge_uids, *edge_count, edge_count, constraint, GDI_EDGE_UNDIRECTED, vertex );
  }
  assert( status == GDI_SUCCESS );

  /**
    clean up
   */
  status = GDI_FreeSubconstraint( &subconstraint );
  assert( status == GDI_SUCCESS );
  status = GDI_FreeConstraint( &constraint );
  assert( status == GDI_SUCCESS );

  return 0;
}


/**
  start_vertex, vlabel and transaction need to be the same
  on all participating processes

  uses int to count vertices, so not safe for very large graphs

  depth and v_ids have to be freed by the caller
  TODO: v_ids only works for uint64_t

  return value:
    0 - success
    1 - no vertices in database

  abort value:
    -2 - not enough memory
    -3 - vertex not found
    -4 - transaction-critical error
 */
int nod_bfs_sort_u32( GDI_Label vlabel, uint64_t start_vertex, GDI_Transaction transaction, uint8_t** depth, uint64_t** v_ids, size_t* elem_cnt ) {
  int status;

  size_t assoc_cnt = 0;

  uint64_t current_count = 0;
  uint64_t current_size = 1000;
  uint32_t* current = malloc( current_size * sizeof(uint32_t) );
  if( current == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  bool found_flag;
  GDI_Vertex_uid vertex_uid;

  status = GDI_TranslateVertexID( &found_flag, &vertex_uid, vlabel, &start_vertex, sizeof(uint64_t), transaction );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  if( !found_flag ) {
    fprintf( stderr, "Rank %i: didn't find %" PRIu64 ".\n", transaction->db->commrank, start_vertex );
    MPI_Abort( transaction->db->comm, -3 );
  }

  uint64_t voffset, vrank;
  GDA_GetDPointer( &voffset, &vrank, vertex_uid );

  if( vrank == (uint64_t)(transaction->db->commrank) ) {
    current[current_count++] = voffset;
  }

  size_t output_size = 1000;
  *depth = malloc( output_size * sizeof(uint8_t) );
  *v_ids = malloc( output_size * sizeof(uint64_t) );
  *elem_cnt = 0;

  GDA_HashMap* depth_hm;
  GDA_hashmap_create( &depth_hm, sizeof(uint32_t) /* key size */, ASSOC_TRESHOLD /* capacity */, sizeof(uint8_t) /* value size */, &GDA_int_to_int );

  size_t adjacent_count = 32;
  GDI_Vertex_uid* vertex_uids = malloc( adjacent_count * sizeof(GDI_Vertex_uid) );
  if( vertex_uids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  uint32_t** buffer = malloc( transaction->db->commsize * sizeof(uint32_t*) );
  if( buffer == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }
  /**
    buffer_cnt:
    0..csize-1 contains the fill level of the respective buffer
    csize..2*csize-1 contains the size of the respective buffer
   */
  int* buffer_cnt = malloc( 2 * transaction->db->commsize * sizeof(int) );
  if( buffer_cnt == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }
  for( unsigned int i=0 ; i<transaction->db->commsize ; i++ ) {
    buffer_cnt[transaction->db->commsize+i] = 32;
    buffer[i] = malloc( 32 * sizeof(uint32_t) );
    if( buffer[i] == NULL ) {
      fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
      MPI_Abort( transaction->db->comm, -2 );
    }
  }

  int* send_count = malloc( transaction->db->commsize * sizeof(int) );
  if( send_count == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  int* recv_count = malloc( transaction->db->commsize * sizeof(int) );
  if( recv_count == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  int* send_displacement = malloc( transaction->db->commsize * sizeof(int) );
  if( send_displacement == NULL ) {
     fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
     MPI_Abort( transaction->db->comm, -2 );
  }
  send_displacement[0] = 0;

  int* recv_displacement = malloc( transaction->db->commsize * sizeof(int) );
  if( recv_displacement == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }
  recv_displacement[0] = 0;

  uint8_t depth_level = 0;

  while( true ) {
    for( unsigned int i=0 ; i<transaction->db->commsize ; i++ ) {
      buffer_cnt[i] = 0;
    }

    for( size_t i=0 ; i<current_count ; i++ ) {
      GDA_hashmap_insert( depth_hm, &current[i], &depth_level );

      assoc_cnt++;
      if( assoc_cnt == ASSOC_TRESHOLD ) {
        GDA_PurgeBuffer( transaction );
        assoc_cnt = 0;
      }

      GDI_VertexHolder vertex;
      GDA_SetDPointer( current[i], transaction->db->commrank, &vertex_uid );
      status = GDI_AssociateVertex( vertex_uid, transaction, &vertex );
      assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
      if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
        fprintf( stderr, "Rank %i: vertex association was transaction-critical.\n", transaction->db->commrank );
        MPI_Abort( transaction->db->comm, -4 );
      }

      if( *elem_cnt == output_size ) {
        output_size = output_size << 1;
        *depth = realloc( *depth, output_size * sizeof(uint8_t) );
        *v_ids = realloc( *v_ids, output_size * sizeof(uint64_t) );
      }

      size_t resultcount, array_of_offsets[2], offset_resultcount;
      status = GDI_GetPropertiesOfVertex( &((*v_ids)[*elem_cnt]), 8, &resultcount,
        array_of_offsets, 2 /* offset count */, &offset_resultcount, GDI_PROPERTY_TYPE_ID, vertex );
      assert( (status == GDI_SUCCESS) && (offset_resultcount == 2) );

      (*depth)[(*elem_cnt)++] = depth_level;
#if 0
      printf("depth[%lu] = %u\n", (*v_ids)[*elem_cnt], depth_level);
#endif

      size_t neighbors_count;
      status = GDI_GetNeighborVerticesOfVertex( vertex_uids, adjacent_count, &neighbors_count, GDI_CONSTRAINT_NULL, GDI_EDGE_UNDIRECTED, vertex );
      if( status == GDI_ERROR_TRUNCATE ) {
        status = GDI_GetNeighborVerticesOfVertex( NULL, 0, &adjacent_count, GDI_CONSTRAINT_NULL, GDI_EDGE_UNDIRECTED, vertex );
        assert( status == GDI_SUCCESS );
        vertex_uids = realloc( vertex_uids, adjacent_count * sizeof(GDI_Vertex_uid) );
        if( vertex_uids == NULL ) {
          fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
          MPI_Abort( transaction->db->comm, -2 );
        }
        status = GDI_GetNeighborVerticesOfVertex( vertex_uids, adjacent_count, &neighbors_count, GDI_CONSTRAINT_NULL, GDI_EDGE_UNDIRECTED, vertex );
      }
      assert( status == GDI_SUCCESS );

      /* add neighbors of current vertex to the appropriate buffers */
      for( size_t j=0 ; j<neighbors_count ; j++ ) {
        GDA_GetDPointer( &voffset, &vrank, vertex_uids[j] );

        if( buffer_cnt[vrank] == buffer_cnt[transaction->db->commsize+vrank] ) {
          /* buffer is full */
          buffer_cnt[transaction->db->commsize+vrank] *= 2;
          buffer[vrank] = realloc( buffer[vrank], buffer_cnt[transaction->db->commsize+vrank] * sizeof(uint32_t) );
          if( buffer[vrank] == NULL ) {
            fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
            MPI_Abort( transaction->db->comm, -2 );
          }
        }
        *(buffer[vrank]+buffer_cnt[vrank]) = voffset;
        buffer_cnt[vrank]++;
      }
    }

    /**
      determine the unique vertex UIDs for each target rank buffer

      additionally send_count, send_displacement as well as the send
      buffer are being prepared
     */
    int send_buffer_size = ASSOC_TRESHOLD;
    uint32_t* send_buffer = malloc( send_buffer_size * sizeof(uint32_t) );
    if( send_buffer == NULL ) {
      fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
      MPI_Abort( transaction->db->comm, -2 );
    }

    send_count[0] = 0;
    if( buffer_cnt[0] > 0 ) {
      radix_sort7_u32( buffer[0], buffer_cnt[0] );

      send_buffer[send_count[0]++] = *(buffer[0]);
      for( int i=1 ; i<buffer_cnt[0] ; i++ ) {
        if( *(buffer[0]+i) != *(buffer[0]+i-1) ) {
          if( send_count[0] == send_buffer_size ) {
            send_buffer_size = send_buffer_size << 1;
            send_buffer = realloc( send_buffer, send_buffer_size * sizeof(uint32_t) );
            if( send_buffer == NULL ) {
              fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
              MPI_Abort( transaction->db->comm, -2 );
            }
          }
          send_buffer[send_count[0]++] = *(buffer[0]+i);
        }
      }
    }

    for( unsigned int i=1 ; i<transaction->db->commsize ; i++ ) {
      send_count[i] = 0;
      send_displacement[i] = send_displacement[i-1] + send_count[i-1];
      if( buffer_cnt[i] > 0 ) {
        radix_sort7_u32( buffer[i], buffer_cnt[i] );

        if( (send_displacement[i]+send_count[i]) == send_buffer_size ) {
          send_buffer_size = send_buffer_size << 1;
          send_buffer = realloc( send_buffer, send_buffer_size * sizeof(uint32_t) );
          if( send_buffer == NULL ) {
            fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
            MPI_Abort( transaction->db->comm, -2 );
          }
        }
        send_buffer[send_displacement[i]+send_count[i]++] = *(buffer[i]);
        for( int j=1 ; j<buffer_cnt[i] ; j++ ) {
          if( *(buffer[i]+j) != *(buffer[i]+j-1) ) {
            if( (send_displacement[i]+send_count[i]) == send_buffer_size ) {
              send_buffer_size = send_buffer_size << 1;
              send_buffer = realloc( send_buffer, send_buffer_size * sizeof(uint32_t) );
              if( send_buffer == NULL ) {
                fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
                MPI_Abort( transaction->db->comm, -2 );
              }
            }
            send_buffer[send_displacement[i]+send_count[i]++] = *(buffer[i]+j);
          }
        }
      }
    }

    /* distribute the number of elements to receive */
    MPI_Alltoall( send_count, 1, MPI_INT, recv_count, 1, MPI_INT, transaction->db->comm );

    /* receive the data for the next frontier */
    for( unsigned int i=1 ; i<transaction->db->commsize ; i++ ) {
      recv_displacement[i] = recv_displacement[i-1] + recv_count[i-1];
    }

    size_t total_recv_count = recv_displacement[transaction->db->commsize-1] + recv_count[transaction->db->commsize-1]; /* in elements */

    uint32_t* next_frontier = malloc( total_recv_count * sizeof(uint32_t) );
    if( next_frontier == NULL ) {
      fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
      MPI_Abort( transaction->db->comm, -2 );
    }

    MPI_Alltoallv( send_buffer, send_count, send_displacement, MPI_UINT32_T, next_frontier, recv_count, recv_displacement, MPI_UINT32_T, transaction->db->comm );

    free( send_buffer );

    /* sort all received vertex UIDs, and only write the unique ones into the next frontier */
    current_count = 0;
    if( total_recv_count > 0 ) {
      radix_sort7_u32( next_frontier, total_recv_count );

      size_t pos = 0;
      while( pos < total_recv_count ) {
        if( GDA_hashmap_find( depth_hm, &next_frontier[pos++] ) == GDA_HASHMAP_NOT_FOUND ) {
          *current = next_frontier[pos-1];
          current_count++;
          break;
        }
      }
      for( size_t i=pos ; i<total_recv_count ; i++ ) {
        if( (next_frontier[i] != next_frontier[i-1]) && (GDA_hashmap_find( depth_hm, &next_frontier[i] ) == GDA_HASHMAP_NOT_FOUND) ) {
          if( current_count == current_size ) {
            current_size = current_size << 1;
            current = realloc( current, current_size * sizeof(uint32_t) );
            if( current == NULL ) {
              fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
              MPI_Abort( transaction->db->comm, -2 );
            }
          }
          current[current_count++] = next_frontier[i];
        }
      }
    }

    free( next_frontier );

    uint64_t global_next_frontier_count;
    MPI_Allreduce( &current_count, &global_next_frontier_count, 1, MPI_UINT64_T, MPI_SUM, transaction->db->comm );

    if( global_next_frontier_count == 0 ) {
      /* we are done */
      free( current );
      free( vertex_uids );
      for( unsigned int i=0 ; i<transaction->db->commsize ; i++ ) {
        free( buffer[i] );
      }
      free( buffer );
      free( buffer_cnt );
      free( send_count );
      free( recv_count );
      free( send_displacement );
      free( recv_displacement );
      GDA_hashmap_free( &depth_hm );

      return 0;
    }

    depth_level++;
  }
}


/**
  start_vertex, vlabel and transaction need to be the same
  on all participating processes

  uses int to count vertices, so not safe for very large graphs

  depth and v_ids have to be freed by the caller
  TODO: v_ids only works for uint64_t

  return value:
    0 - success
    1 - no vertices in database

  abort value:
    -2 - not enough memory
    -3 - vertex not found
    -4 - transaction-critical error
 */
int nod_k_hop( GDI_Label vlabel, uint64_t start_vertex, GDI_Transaction transaction, uint8_t k, GDI_Vertex_uid** v_ids, size_t* elem_cnt ) {
  int status;

  size_t assoc_cnt = 0;

  uint64_t current_count = 0;
  uint64_t current_size = 1000;
  uint32_t* current = malloc( current_size * sizeof(uint32_t) );
  if( current == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  bool found_flag;
  GDI_Vertex_uid vertex_uid;

  status = GDI_TranslateVertexID( &found_flag, &vertex_uid, vlabel, &start_vertex, sizeof(uint64_t), transaction );
  assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
  if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
    return 2;
  }

  if( !found_flag ) {
    fprintf( stderr, "Rank %i: didn't find %" PRIu64 ".\n", transaction->db->commrank, start_vertex );
    MPI_Abort( transaction->db->comm, -3 );
  }

  uint64_t voffset, vrank;
  GDA_GetDPointer( &voffset, &vrank, vertex_uid );

  if( vrank == (uint64_t)(transaction->db->commrank) ) {
    current[current_count++] = voffset;
  }

  GDA_HashMap* depth_hm;
  GDA_hashmap_create( &depth_hm, sizeof(uint32_t) /* key size */, ASSOC_TRESHOLD /* capacity */, sizeof(uint8_t) /* value size */, &GDA_int_to_int );

  size_t adjacent_count = 32;
  GDI_Vertex_uid* vertex_uids = malloc( adjacent_count * sizeof(GDI_Vertex_uid) );
  if( vertex_uids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  uint32_t** buffer = malloc( transaction->db->commsize * sizeof(uint32_t*) );
  if( buffer == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }
  /**
    buffer_cnt:
    0..csize-1 contains the fill level of the respective buffer
    csize..2*csize-1 contains the size of the respective buffer
   */
  int* buffer_cnt = malloc( 2 * transaction->db->commsize * sizeof(int) );
  if( buffer_cnt == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }
  for( unsigned int i=0 ; i<transaction->db->commsize ; i++ ) {
    buffer_cnt[transaction->db->commsize+i] = 32;
    buffer[i] = malloc( 32 * sizeof(uint32_t) );
    if( buffer[i] == NULL ) {
      fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
      MPI_Abort( transaction->db->comm, -2 );
    }
  }

  int* send_count = malloc( transaction->db->commsize * sizeof(int) );
  if( send_count == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  int* recv_count = malloc( transaction->db->commsize * sizeof(int) );
  if( recv_count == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  int* send_displacement = malloc( transaction->db->commsize * sizeof(int) );
  if( send_displacement == NULL ) {
     fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
     MPI_Abort( transaction->db->comm, -2 );
  }
  send_displacement[0] = 0;

  int* recv_displacement = malloc( transaction->db->commsize * sizeof(int) );
  if( recv_displacement == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }
  recv_displacement[0] = 0;

  for( uint8_t depth_level=0 ; depth_level < k ; depth_level++ ) {
    for( unsigned int i=0 ; i<transaction->db->commsize ; i++ ) {
      buffer_cnt[i] = 0;
    }

    for( size_t i=0 ; i<current_count ; i++ ) {
      GDA_hashmap_insert( depth_hm, &current[i], &depth_level );

      assoc_cnt++;
      if( assoc_cnt == ASSOC_TRESHOLD ) {
        GDA_PurgeBuffer( transaction );
        assoc_cnt = 0;
      }

      GDI_VertexHolder vertex;
      GDA_SetDPointer( current[i], transaction->db->commrank, &vertex_uid );
      status = GDI_AssociateVertex( vertex_uid, transaction, &vertex );
      assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );
      if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
        fprintf( stderr, "Rank %i: vertex association was transaction-critical.\n", transaction->db->commrank );
        MPI_Abort( transaction->db->comm, -4 );
      }

      size_t neighbors_count;
      status = GDI_GetNeighborVerticesOfVertex( vertex_uids, adjacent_count, &neighbors_count, GDI_CONSTRAINT_NULL, GDI_EDGE_UNDIRECTED, vertex );
      if( status == GDI_ERROR_TRUNCATE ) {
        status = GDI_GetNeighborVerticesOfVertex( NULL, 0, &adjacent_count, GDI_CONSTRAINT_NULL, GDI_EDGE_UNDIRECTED, vertex );
        assert( status == GDI_SUCCESS );
        vertex_uids = realloc( vertex_uids, adjacent_count * sizeof(GDI_Vertex_uid) );
        if( vertex_uids == NULL ) {
          fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
          MPI_Abort( transaction->db->comm, -2 );
        }
        status = GDI_GetNeighborVerticesOfVertex( vertex_uids, adjacent_count, &neighbors_count, GDI_CONSTRAINT_NULL, GDI_EDGE_UNDIRECTED, vertex );
      }
      assert( status == GDI_SUCCESS );

      /* add neighbors of current vertex to the appropriate buffers */
      for( size_t j=0 ; j<neighbors_count ; j++ ) {
        GDA_GetDPointer( &voffset, &vrank, vertex_uids[j] );

        if( buffer_cnt[vrank] == buffer_cnt[transaction->db->commsize+vrank] ) {
          /* buffer is full */
          buffer_cnt[transaction->db->commsize+vrank] *= 2;
          buffer[vrank] = realloc( buffer[vrank], buffer_cnt[transaction->db->commsize+vrank] * sizeof(uint32_t) );
          if( buffer[vrank] == NULL ) {
            fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
            MPI_Abort( transaction->db->comm, -2 );
          }
        }
        *(buffer[vrank]+buffer_cnt[vrank]) = voffset;
        buffer_cnt[vrank]++;
      }
    }

    /**
      determine the unique vertex UIDs for each target rank buffer

      additionally send_count, send_displacement as well as the send
      buffer are being prepared
     */
    int send_buffer_size = ASSOC_TRESHOLD;
    uint32_t* send_buffer = malloc( send_buffer_size * sizeof(uint32_t) );
    if( send_buffer == NULL ) {
      fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
      MPI_Abort( transaction->db->comm, -2 );
    }

    send_count[0] = 0;
    if( buffer_cnt[0] > 0 ) {
      radix_sort7_u32( buffer[0], buffer_cnt[0] );

      send_buffer[send_count[0]++] = *(buffer[0]);
      for( int i=1 ; i<buffer_cnt[0] ; i++ ) {
        if( *(buffer[0]+i) != *(buffer[0]+i-1) ) {
          if( send_count[0] == send_buffer_size ) {
            send_buffer_size = send_buffer_size << 1;
            send_buffer = realloc( send_buffer, send_buffer_size * sizeof(uint32_t) );
            if( send_buffer == NULL ) {
              fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
              MPI_Abort( transaction->db->comm, -2 );
            }
          }
          send_buffer[send_count[0]++] = *(buffer[0]+i);
        }
      }
    }

    for( unsigned int i=1 ; i<transaction->db->commsize ; i++ ) {
      send_count[i] = 0;
      send_displacement[i] = send_displacement[i-1] + send_count[i-1];
      if( buffer_cnt[i] > 0 ) {
        radix_sort7_u32( buffer[i], buffer_cnt[i] );

        if( (send_displacement[i]+send_count[i]) == send_buffer_size ) {
          send_buffer_size = send_buffer_size << 1;
          send_buffer = realloc( send_buffer, send_buffer_size * sizeof(uint32_t) );
          if( send_buffer == NULL ) {
            fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
            MPI_Abort( transaction->db->comm, -2 );
          }
        }
        send_buffer[send_displacement[i]+send_count[i]++] = *(buffer[i]);
        for( int j=1 ; j<buffer_cnt[i] ; j++ ) {
          if( *(buffer[i]+j) != *(buffer[i]+j-1) ) {
            if( (send_displacement[i]+send_count[i]) == send_buffer_size ) {
              send_buffer_size = send_buffer_size << 1;
              send_buffer = realloc( send_buffer, send_buffer_size * sizeof(uint32_t) );
              if( send_buffer == NULL ) {
                fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
                MPI_Abort( transaction->db->comm, -2 );
              }
            }
            send_buffer[send_displacement[i]+send_count[i]++] = *(buffer[i]+j);
          }
        }
      }
    }

    /* distribute the number of elements to receive */
    MPI_Alltoall( send_count, 1, MPI_INT, recv_count, 1, MPI_INT, transaction->db->comm );

    /* receive the data for the next frontier */
    for( unsigned int i=1 ; i<transaction->db->commsize ; i++ ) {
      recv_displacement[i] = recv_displacement[i-1] + recv_count[i-1];
    }

    size_t total_recv_count = recv_displacement[transaction->db->commsize-1] + recv_count[transaction->db->commsize-1]; /* in elements */

    uint32_t* next_frontier = malloc( total_recv_count * sizeof(uint32_t) );
    if( next_frontier == NULL ) {
      fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
      MPI_Abort( transaction->db->comm, -2 );
    }

    MPI_Alltoallv( send_buffer, send_count, send_displacement, MPI_UINT32_T, next_frontier, recv_count, recv_displacement, MPI_UINT32_T, transaction->db->comm );

    free( send_buffer );

    /* sort all received vertex UIDs, and only write the unique ones into the next frontier */
    current_count = 0;
    if( total_recv_count > 0 ) {
      radix_sort7_u32( next_frontier, total_recv_count );

      size_t pos = 0;
      while( pos < total_recv_count ) {
        if( GDA_hashmap_find( depth_hm, &next_frontier[pos++] ) == GDA_HASHMAP_NOT_FOUND ) {
          *current = next_frontier[pos-1];
          current_count++;
          break;
        }
      }
      for( size_t i=pos ; i<total_recv_count ; i++ ) {
        if( (next_frontier[i] != next_frontier[i-1]) && (GDA_hashmap_find( depth_hm, &next_frontier[i] ) == GDA_HASHMAP_NOT_FOUND) ) {
          if( current_count == current_size ) {
            current_size = current_size << 1;
            current = realloc( current, current_size * sizeof(uint32_t) );
            if( current == NULL ) {
              fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
              MPI_Abort( transaction->db->comm, -2 );
            }
          }
          current[current_count++] = next_frontier[i];
        }
      }
    }

    free( next_frontier );
  }

  /* assemble the results */
  *elem_cnt = current_count;
  *v_ids = malloc( current_count * sizeof(GDI_Vertex_uid) );
  for( size_t i=0 ; i<current_count ; i++ ) {
    GDA_SetDPointer( current[i], transaction->db->commrank, &((*v_ids)[i]) );
  }

  free( current );
  free( vertex_uids );
  for( unsigned int i=0 ; i<transaction->db->commsize ; i++ ) {
    free( buffer[i] );
  }
  free( buffer );
  free( buffer_cnt );
  free( send_count );
  free( recv_count );
  free( send_displacement );
  free( recv_displacement );
  GDA_hashmap_free( &depth_hm );

  return 0;
}


/**
  source code by George Mitenkov: helper function for CDLP
 */
uint64_t max_frequency( uint64_t* histogram, size_t n ) {
  /**
    First, sort the input.
   */
  radix_sort7( histogram, n );

  /**
    Use sliding window to scan all values in linear time and selecting the
    most frequent value.
   */
  size_t i = 0, j = 0;
  size_t frequency = 0, best_frequency = 0;
  uint64_t best_label = histogram[0];

  while( j<n ) {
    if( histogram[i] == histogram[j] ) {
      j++;
      frequency++;
    } else {
      if( frequency > best_frequency ) {
        best_frequency = frequency;
        best_label = histogram[i];
      }
      i = j;
      frequency = 0;
    }
  }

  /**
    It can happen that the current value is the most frequent. In this case
    return it.
   */
  if( best_frequency<frequency ) {
    return histogram[i];
  } else {
    return best_label;
  }
}


/**
  based on CDLP code by George Mitenkov
  optimized by Robert Gerstenberger
 */
int nod_cdlp_nonblocking_sorted( uint64_t global_num_verts, GDI_Transaction transaction, uint32_t max_num_iterations, uint64_t** labels, uint64_t** v_ids, size_t* elem_cnt ) {
  int status;

  size_t assoc_count = 0; /* track the number of vertices associated with the collective transaction */

  /**
    next step is a bit iffy: we compute the vertex UIDs of the vertices
    local to this process

    equivalent would be a GDI_GetLocalVerticesOfIndex call

    normally afterwards, we would need to extract the highest block
    offset to use the memory allocations of the windows; because of
    how the database is created, local_num_verts is that maximum
   */
  uint64_t local_num_verts = global_num_verts / transaction->db->commsize;
  if( (uint64_t)(transaction->db->commrank) < (global_num_verts - local_num_verts * transaction->db->commsize) ) {
    local_num_verts++;
  }

  GDI_Vertex_uid* local_uids = malloc( local_num_verts * sizeof(GDI_Vertex_uid) );
  if( local_uids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
    GDA_SetDPointer( i * transaction->db->block_size, transaction->db->commrank, &local_uids[i] );
  }

  *v_ids = malloc( local_num_verts * sizeof(uint64_t) );
  if( *v_ids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  /**
    data allocations
   */
  uint64_t* curr_labels = malloc( local_num_verts * sizeof(uint64_t) );
  if( curr_labels == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  uint64_t* degrees = malloc( local_num_verts * sizeof(uint64_t) );
  if( degrees == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  /**
    Every rank shares the labels from previous iteration with other
    processes. For every vertex v, it's label is initialized with the
    application-level ID of the vertex: L(v) = v.
   */
  RMA_Win labels_window;
  uint64_t* prev_labels;
  RMA_Win_allocate( local_num_verts * sizeof(uint64_t), sizeof(uint64_t), MPI_INFO_NULL, transaction->db->comm, &prev_labels, &labels_window );

  RMA_Win_lock_all( 0, labels_window );

  /**
    initialization
   */
  for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
    GDI_VertexHolder vertex;
    bound_memory( assoc_count );
    status = GDI_AssociateVertex( local_uids[i], transaction, &vertex );
    assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );

    if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
      fprintf( stderr, "Rank %i: vertex association was transaction-critical.\n", transaction->db->commrank );
      MPI_Abort( transaction->db->comm, -4 );
    }

    size_t result_count, offset_result_count, array_of_offsets[2];
    status = GDI_GetPropertiesOfVertex( &degrees[i], 1, &result_count, array_of_offsets, 2, &offset_result_count, GDI_PROPERTY_TYPE_DEGREE, vertex );
    assert( (status == GDI_SUCCESS) && (offset_result_count == 2) );

    status = GDI_GetPropertiesOfVertex( &curr_labels[i], 8, &result_count,
      array_of_offsets, 2 /* offset count */, &offset_result_count, GDI_PROPERTY_TYPE_ID, vertex );
    assert( (status == GDI_SUCCESS) && (offset_result_count == 2) );

    (*v_ids)[i] = curr_labels[i];
  }

  size_t max_adjacent_count = 0;
  for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
    if( degrees[i] > max_adjacent_count ) {
      max_adjacent_count = degrees[i];
    }
  }

  GDI_Vertex_uid* neighbor_uids = malloc( max_adjacent_count * sizeof(GDI_Vertex_uid) );
  if( neighbor_uids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  uint64_t* neighbor_labels = malloc( max_adjacent_count * sizeof(uint64_t) );
  if( neighbor_labels == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  /**
    start the actual CDLP algorithm
   */
  for( uint32_t iteration = 0 ; iteration < max_num_iterations ; iteration++ ) {
    for( size_t i=0; i<local_num_verts; i++ ) {
      prev_labels[i] = curr_labels[i];
    }

    /* ensure that the data in the window is updated */
    MPI_Barrier( transaction->db->comm );

    for( size_t i=0 ; i<local_num_verts ; i++ ) {
      GDI_VertexHolder vertex;
      bound_memory( assoc_count );
      status = GDI_AssociateVertex( local_uids[i], transaction, &vertex );
      assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );

      if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
        fprintf( stderr, "Rank %i: vertex association was transaction-critical.\n", transaction->db->commrank );
        MPI_Abort( transaction->db->comm, -4 );
      }

      /* obtain the neighbors */
      size_t neighbors_count;
      status = GDI_GetNeighborVerticesOfVertex( neighbor_uids, max_adjacent_count, &neighbors_count, GDI_CONSTRAINT_NULL, GDI_EDGE_UNDIRECTED, vertex );
      assert( status == GDI_SUCCESS );

      radix_sort7( neighbor_uids, neighbors_count );

      /* go through the neighbors */
      for( size_t j=0; j<neighbors_count; j++) {
        uint64_t toffset, trank;
        GDA_GetDPointer( &toffset, &trank, neighbor_uids[j] );
        toffset = toffset / transaction->db->block_size;

        RMA_Get( &neighbor_labels[j], 1, MPI_UINT64_T, trank, toffset, 1, MPI_UINT64_T, labels_window );

        while( ((j+1) < neighbors_count) && (neighbor_uids[j] == neighbor_uids[j+1]) ) {
          j++;
        }
      }
      RMA_Win_flush_all( labels_window );

      /**
        choose a new label for the current vertex: the one that occurs
        the most in the neighbors. To ensure that all labels are chosen
        deterministically, if any two labels occur the same number of
        times, the one with a smallest value is picked.

        Note: guard for special case when a vertex has no neighbors, as
        max_frequency() does not account for that.

        Comment: it might have been possible (but challenging) to
        compute labels on the fly, but that would be a different online
        algorithm. Collecting labels and computing the right label in
        linear time suffices here.
       */
      if( neighbors_count > 0 ) {
        for( size_t j=0; j<neighbors_count; j++) {
          while( ((j+1) < neighbors_count) && (neighbor_uids[j] == neighbor_uids[j+1]) ) {
            neighbor_labels[j+1] = neighbor_labels[j];
            j++;
          }
        }
        curr_labels[i] = max_frequency( neighbor_labels, neighbors_count );
      }
    }

    MPI_Barrier( transaction->db->comm );
  }

  /**
    clean up
   */
  RMA_Win_unlock_all( labels_window );
  RMA_Win_free( &labels_window );

  free( neighbor_labels );
  free( neighbor_uids );
  free( local_uids );
  free( degrees );

  *labels = curr_labels;
  *elem_cnt = local_num_verts;
  return 0;
}


/**
  based on pagerank_pull code by George Mitenkov
  optimized by Robert Gerstenberger

  use of GDI_GetNeighborVerticesOfVertex works with the current
  implementation, which is just a shorthand for GDI_GetEdgesOfVertex
  instead of only returning the unique vertices of the neighborhood like
  the changed version in the specification suggests
 */
int nod_pagerank_nonblocking_sorted( uint64_t global_num_verts, GDI_Transaction transaction, uint32_t num_iterations, double damping_factor, double** scores, uint64_t** v_ids, size_t* elem_cnt ) {
  int status;

  size_t assoc_count = 0; /* track the number of vertices associated with the collective transaction */

  /**
    next step is a bit iffy: we compute the vertex UIDs of the vertices
    local to this process

    equivalent would be a GDI_GetLocalVerticesOfIndex call

    normally afterwards, we would need to extract the highest block
    offset to use the memory allocations of the windows; because of
    how the database is created, local_num_verts is that maximum
   */
  uint64_t local_num_verts = global_num_verts / transaction->db->commsize;
  if( (uint64_t)(transaction->db->commrank) < (global_num_verts - local_num_verts * transaction->db->commsize) ) {
    local_num_verts++;
  }

  GDI_Vertex_uid* local_uids = malloc( local_num_verts * sizeof(GDI_Vertex_uid) );
  if( local_uids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
    GDA_SetDPointer( i * transaction->db->block_size, transaction->db->commrank, &local_uids[i] );
  }

  *v_ids = malloc( local_num_verts * sizeof(uint64_t) );
  if( *v_ids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 ); /* a bit iffy */
  }

  /**
   PageRank uses 2 constants:
     intitial score: initialized to 1/|V|.
     teleport score: initialized to (1 - d)/|V|

    Compute them in advance.
   */
  double initial_score = 1.0 / global_num_verts;
  double teleport_score = (1.0 - damping_factor) / global_num_verts;

  /**
    data allocations
   */
  size_t adjacent_count = 32;
  GDI_Vertex_uid* neighbor_uids = malloc( adjacent_count * sizeof(GDI_Vertex_uid) );
  if( neighbor_uids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  double* neighbor_score = malloc( adjacent_count * sizeof(double) );
  if( neighbor_score == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  uint64_t* neighbor_outdegree = malloc( adjacent_count * sizeof(uint64_t) );
  if( neighbor_outdegree == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  double* curr_scores = malloc( local_num_verts * sizeof(double) );
  if( curr_scores == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  /**
    This is a pull version of PageRank, therefore we need to create a window
    which other processes can access to get PageRank scores from the previous
    iteration.
   */
  RMA_Win scores_window;
  double* prev_scores = NULL;
  RMA_Win_allocate( local_num_verts * sizeof(double), sizeof(double), MPI_INFO_NULL, transaction->db->comm, &prev_scores, &scores_window );

  for( size_t i=0; i<local_num_verts; i++ ) {
    curr_scores[i] = initial_score;
  }

  RMA_Win_lock_all( 0, scores_window );

  uint64_t* out_degrees = malloc( local_num_verts * sizeof(uint64_t) );
  if( out_degrees == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
    GDI_VertexHolder vertex;
    bound_memory( assoc_count );
    status = GDI_AssociateVertex( local_uids[i], transaction, &vertex );
    assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );

    if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
      fprintf( stderr, "Rank %i: vertex association was transaction-critical.\n", transaction->db->commrank );
      MPI_Abort( transaction->db->comm, -4 );
    }

    size_t result_count, offset_result_count, array_of_offsets[2];
    status = GDI_GetPropertiesOfVertex( &out_degrees[i], 1, &result_count, array_of_offsets, 2, &offset_result_count, GDI_PROPERTY_TYPE_OUTDEGREE, vertex );
    assert( (status == GDI_SUCCESS) && (offset_result_count == 2) );

    status = GDI_GetPropertiesOfVertex( &((*v_ids)[i]), 8, &result_count,
      array_of_offsets, 2 /* offset count */, &offset_result_count, GDI_PROPERTY_TYPE_ID, vertex );
    assert( (status == GDI_SUCCESS) && (offset_result_count == 2) );
  }

  /**
    Start PageRank iterations. First, update the scores from previously computed
    values and synchronise to make sure all data is up to date.
   */
  for( uint32_t iteration = 1 ; iteration<num_iterations ; iteration++ ) {
    for( size_t i=0; i<local_num_verts; i++ ) {
      prev_scores[i] = curr_scores[i]/out_degrees[i];
    }

    /* ensure that the data in the windows is updated */
    MPI_Barrier( transaction->db->comm );

    for( size_t i=0 ; i<local_num_verts ; i++ ) {
      GDI_VertexHolder vertex;
      bound_memory( assoc_count );
      status = GDI_AssociateVertex( local_uids[i], transaction, &vertex );
      assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );

      if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
        fprintf( stderr, "Rank %i: vertex association was transaction-critical.\n", transaction->db->commrank );
        MPI_Abort( transaction->db->comm, -4 );
      }

      /* obtain the neighbors */
      size_t neighbors_count;
      status = GDI_GetNeighborVerticesOfVertex( neighbor_uids, adjacent_count, &neighbors_count, GDI_CONSTRAINT_NULL, GDI_EDGE_INCOMING, vertex );
      if( status == GDI_ERROR_TRUNCATE ) {
        status = GDI_GetNeighborVerticesOfVertex( NULL, 0, &adjacent_count, GDI_CONSTRAINT_NULL, GDI_EDGE_INCOMING, vertex );
        assert( status == GDI_SUCCESS );
        neighbor_uids = realloc( neighbor_uids, adjacent_count * sizeof(GDI_Vertex_uid) );
        neighbor_score = realloc( neighbor_score, adjacent_count * sizeof(double) );
        neighbor_outdegree = realloc( neighbor_outdegree, adjacent_count * sizeof(uint64_t) );
        if( neighbor_uids == NULL ) {
          fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
          MPI_Abort( transaction->db->comm, -2 );
        }
        status = GDI_GetNeighborVerticesOfVertex( neighbor_uids, adjacent_count, &neighbors_count, GDI_CONSTRAINT_NULL, GDI_EDGE_INCOMING, vertex );
      }
      assert( status == GDI_SUCCESS );

      radix_sort7( neighbor_uids, neighbors_count );

      /* go through the neighbors */
      for( size_t j=0; j<neighbors_count; j++) {
        while( ((j+1) < neighbors_count) && (neighbor_uids[j] == neighbor_uids[j+1]) ) {
          j++;
        }
        uint64_t toffset, trank;
        GDA_GetDPointer( &toffset, &trank, neighbor_uids[j] );
        toffset = toffset / transaction->db->block_size;

        RMA_Get( &neighbor_score[j], 1, MPI_DOUBLE, trank, toffset, 1, MPI_DOUBLE, scores_window );
      }

      RMA_Win_flush_all( scores_window );

      curr_scores[i] = 0.0;
      for( size_t j=0; j<neighbors_count; j++) {
        size_t repetition = 1;
        while( ((j+1) < neighbors_count) && (neighbor_uids[j] == neighbor_uids[j+1]) ) {
          repetition++;
          j++;
        }
        curr_scores[i] += repetition * neighbor_score[j];
      }
      curr_scores[i] = damping_factor * curr_scores[i] + teleport_score;
    }

    /* ensure that the round is finished by everyone */
    MPI_Barrier( transaction->db->comm );
  }

  /**
    clean up
   */
  RMA_Win_unlock_all( scores_window );

  RMA_Win_free( &scores_window );
  free( neighbor_uids );
  free( local_uids );
  free( neighbor_outdegree );
  free( neighbor_score );
  free( out_degrees );

  *scores = curr_scores;
  *elem_cnt = local_num_verts;
  return 0;
}


/**
  based on wcc_pull code by George Mitenkov
  optimized by Robert Gerstenberger
 */
int nod_wcc_pull_nonblocking_sorted( uint64_t global_num_verts, GDI_Transaction transaction, uint32_t max_num_iterations, uint64_t** components, uint64_t** v_ids, size_t* elem_cnt ) {
  int status;

  size_t assoc_count = 0; /* track the number of vertices associated with the collective transaction */

  /**
    next step is a bit iffy: we compute the vertex UIDs of the vertices
    local to this process

    equivalent would be a GDI_GetLocalVerticesOfIndex call

    normally afterwards, we would need to extract the highest block
    offset to use the memory allocations of the windows; because of
    how the database is created, local_num_verts is that maximum
   */
  uint64_t local_num_verts = global_num_verts / transaction->db->commsize;
  if( (uint64_t)(transaction->db->commrank) < (global_num_verts - local_num_verts * transaction->db->commsize) ) {
    local_num_verts++;
  }

  GDI_Vertex_uid* local_uids = malloc( local_num_verts * sizeof(GDI_Vertex_uid) );
  if( local_uids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
    GDA_SetDPointer( i * transaction->db->block_size, transaction->db->commrank, &local_uids[i] );
  }

  /**
    data allocations
   */

  *v_ids = malloc( local_num_verts * sizeof(uint64_t) );
  if( *v_ids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 ); /* a bit iffy */
  }

  uint64_t* curr_components = malloc( local_num_verts * sizeof(uint64_t) );
  if( curr_components == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  /**
    create window to share the components of vertices from the previous iteration
    initially all vertices are component themselves
   */
  RMA_Win components_window;
  uint64_t* prev_components;
  RMA_Win_allocate( local_num_verts * sizeof(uint64_t), sizeof(uint64_t), MPI_INFO_NULL, transaction->db->comm, &prev_components, &components_window );

  size_t max_adjacent_count = 0;
  for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
    GDI_VertexHolder vertex;
    bound_memory( assoc_count );
    status = GDI_AssociateVertex( local_uids[i], transaction, &vertex );
    assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );

    if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
      fprintf( stderr, "Rank %i: vertex association was transaction-critical.\n", transaction->db->commrank );
      MPI_Abort( transaction->db->comm, -4 );
    }

    size_t result_count, offset_result_count, array_of_offsets[2];
    uint64_t degree;
    status = GDI_GetPropertiesOfVertex( &degree, 1, &result_count, array_of_offsets, 2, &offset_result_count, GDI_PROPERTY_TYPE_DEGREE, vertex );
    assert( (status == GDI_SUCCESS) && (offset_result_count == 2) );

    if( degree > max_adjacent_count ) {
      max_adjacent_count = degree;
    }

    status = GDI_GetPropertiesOfVertex( &curr_components[i], 8, &result_count,
      array_of_offsets, 2 /* offset count */, &offset_result_count, GDI_PROPERTY_TYPE_ID, vertex );
    assert( (status == GDI_SUCCESS) && (offset_result_count == 2) );

    (*v_ids)[i] = curr_components[i];
  }

  RMA_Win_lock_all( 0, components_window );

  GDI_Vertex_uid* neighbor_uids = malloc( max_adjacent_count * sizeof(GDI_Vertex_uid) );
  if( neighbor_uids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  uint64_t* neighbor_components = malloc( max_adjacent_count * sizeof(uint64_t) );
  if( neighbor_components == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  /**
    look for connected components by propagating UIDs between vertices
    and selecting the minimum ones as the component's representative
   */
  for( uint32_t iteration = 0 ; iteration < max_num_iterations ; iteration++ ) {
    memcpy( prev_components, curr_components, local_num_verts * sizeof(uint64_t) );

    MPI_Barrier( transaction->db->comm );

    for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
      GDI_VertexHolder vertex;

      bound_memory( assoc_count );
      status = GDI_AssociateVertex( local_uids[i], transaction, &vertex );
      assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );

      if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
        fprintf( stderr, "Rank %i: vertex association was transaction-critical.\n", transaction->db->commrank );
        MPI_Abort( transaction->db->comm, -4 );
      }

      /* obtain the neighbors */
      size_t neighbors_count;
      status = GDI_GetNeighborVerticesOfVertex( neighbor_uids, max_adjacent_count, &neighbors_count, GDI_CONSTRAINT_NULL, GDI_EDGE_UNDIRECTED, vertex );
      assert( status == GDI_SUCCESS );

      radix_sort7( neighbor_uids, neighbors_count );

      /* go through the neighbors */
      for( size_t j=0 ; j<neighbors_count ; j++) {
        while( ((j+1) < neighbors_count) && (neighbor_uids[j] == neighbor_uids[j+1]) ) {
          j++;
        }

        uint64_t toffset, trank;
        GDA_GetDPointer( &toffset, &trank, neighbor_uids[j] );
        toffset = toffset / transaction->db->block_size;

        RMA_Get( &neighbor_components[j], 1, MPI_UINT64_T, trank, toffset,  1, MPI_UINT64_T, components_window );
      }

      RMA_Win_flush_all( components_window );

      for( size_t j=0 ; j<neighbors_count ; j++) {
        while( ((j+1) < neighbors_count) && (neighbor_uids[j] == neighbor_uids[j+1]) ) {
          j++;
        }

        if( neighbor_components[j] < curr_components[i] ) {
          curr_components[i] = neighbor_components[j];
        }
      }
    }

    MPI_Barrier( transaction->db->comm );
  }

  /**
    clean up
   */
  RMA_Win_unlock_all( components_window );

  RMA_Win_free( &components_window );

  free( local_uids );
  free( neighbor_uids );
  free( neighbor_components );

  *components = curr_components;
  *elem_cnt = local_num_verts;
  return 0;
}


/**
  based on gnn_forward code by George Mitenkov
  optimized by Robert Gerstenberger

  use of GDI_GetNeighborVerticesOfVertex works with the current
  implementation, which is just a shorthand for GDI_GetEdgesOfVertex
  instead of only returning the unique vertices of the neighborhood like
  the changed version in the specification suggests
 */
int nod_gnn_blocking_sorted( uint64_t global_num_verts, GDI_Transaction transaction, uint32_t num_layers, uint32_t num_features, double* weights, double* bias, double** y_pred, uint64_t** v_ids, size_t* elem_cnt ) {
  int status;

  size_t assoc_count = 0; /* track the number of vertices associated with the collective transaction */

  /**
    next step is a bit iffy: we compute the vertex UIDs of the vertices
    local to this process

    equivalent would be a GDI_GetLocalVerticesOfIndex call

    normally afterwards, we would need to extract the highest block
    offset to use the memory allocations of the windows; because of
    how the database is created, local_num_verts is that maximum
   */
  uint64_t local_num_verts = global_num_verts / transaction->db->commsize;
  if( (uint64_t)(transaction->db->commrank) < (global_num_verts - local_num_verts * transaction->db->commsize) ) {
    local_num_verts++;
  }

  GDI_Vertex_uid* local_uids = malloc( local_num_verts * sizeof(GDI_Vertex_uid) );
  if( local_uids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
    GDA_SetDPointer( i * transaction->db->block_size, transaction->db->commrank, &local_uids[i] );
  }

  /**
    data allocations
   */

  *v_ids = malloc( local_num_verts * sizeof(uint64_t) );
  if( *v_ids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 ); /* a bit iffy */
  }

  /* initial layer */
  double* input = malloc( local_num_verts * num_features * sizeof(double) );
  if( input == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  double* message = malloc( num_features * sizeof(double) );
  if( message == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  double* neighbor_input = malloc( num_features * sizeof(double) );
  if( neighbor_input == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  // TODO: just for testing, check with Maciej what to actually use
  {
    double increment = 1.0 / num_features;
    for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
      input[i*num_features] = 0.0;
      for( uint32_t j=1 ; j<num_features ; j++ ) {
        input[i*num_features+j] = input[i*num_features+j-1] + increment;
      }
    }
  }

  RMA_Win output_window;
  double* output = NULL;
  RMA_Win_allocate( local_num_verts * num_features * sizeof(double), num_features * sizeof(double), MPI_INFO_NULL, transaction->db->comm, &output, &output_window );

  uint64_t* in_degrees = malloc( local_num_verts * sizeof(uint64_t) );
  if( in_degrees == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  /**
    retrieve application-level ID and indegree of all local vertices
   */
  for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
    GDI_VertexHolder vertex;
    bound_memory( assoc_count );
    status = GDI_AssociateVertex( local_uids[i], transaction, &vertex );
    assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );

    if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
      fprintf( stderr, "Rank %i: vertex association was transaction-critical.\n", transaction->db->commrank );
      MPI_Abort( transaction->db->comm, -4 );
    }

    size_t result_count, offset_result_count, array_of_offsets[2];
    status = GDI_GetPropertiesOfVertex( &in_degrees[i], 1, &result_count, array_of_offsets, 2, &offset_result_count, GDI_PROPERTY_TYPE_INDEGREE, vertex );
    assert( (status == GDI_SUCCESS) && (offset_result_count == 2) );

    status = GDI_GetPropertiesOfVertex( &((*v_ids)[i]), 8, &result_count,
      array_of_offsets, 2 /* offset count */, &offset_result_count, GDI_PROPERTY_TYPE_ID, vertex );
    assert( (status == GDI_SUCCESS) && (offset_result_count == 2) );
  }

  RMA_Win_lock_all( 0, output_window );

  size_t max_adjacent_count = 0;
  for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
    if( in_degrees[i] > max_adjacent_count ) {
      max_adjacent_count = in_degrees[i];
    }
  }

  GDI_Vertex_uid* neighbor_uids = malloc( max_adjacent_count * sizeof(GDI_Vertex_uid) );
  if( neighbor_uids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  /**
    start inference, advancing by one layer each time
   */
  for( uint32_t layer = 0 ; layer < num_layers ; layer++ ) {
    /**
      move results from previous layer into the output window and
      initialize the subsequent layer
     */
    for( uint64_t i=0; i<local_num_verts; i++ ) {
      for( uint32_t j=0 ; j<num_features ; j++ ) {
        output[i * num_features + j] = input[i * num_features + j]/sqrt(in_degrees[i]);
      }
      memcpy( &input[i * num_features], &bias[layer * num_features], num_features * sizeof(double) );
    }

    MPI_Barrier( transaction->db->comm );

    /**
      for every local vertex, we pull messages from the incoming neighbors,
      then these messages are aggregated and non-linearity applied
     */
    for( size_t i=0 ; i<local_num_verts ; i++ ) {
      GDI_VertexHolder vertex;

      bound_memory( assoc_count );
      status = GDI_AssociateVertex( local_uids[i], transaction, &vertex );
      assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );

      if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
        fprintf( stderr, "Rank %i: vertex association was transaction-critical.\n", transaction->db->commrank );
        MPI_Abort( transaction->db->comm, -4 );
      }

      /* obtain the neighbors */
      size_t neighbors_count;
      status = GDI_GetNeighborVerticesOfVertex( neighbor_uids, max_adjacent_count, &neighbors_count, GDI_CONSTRAINT_NULL, GDI_EDGE_INCOMING, vertex );
      assert( status == GDI_SUCCESS );

      for( uint32_t j=0 ; j<num_features ; j++ ) {
        message[j] = 0.0;
      }

      radix_sort7( neighbor_uids, neighbors_count );

      /* go through the neighbors */
      for( size_t j=0; j<neighbors_count; j++) {
        size_t repetition = 1;
        while( ((j+1) < neighbors_count) && (neighbor_uids[j] == neighbor_uids[j+1]) ) {
          repetition++;
          j++;
        }
        uint64_t toffset, trank;
        GDA_GetDPointer( &toffset, &trank, neighbor_uids[j] );
        toffset = toffset / transaction->db->block_size;

        RMA_Get( neighbor_input, num_features, MPI_DOUBLE, trank, toffset, num_features, MPI_DOUBLE, output_window );
        RMA_Win_flush( trank, output_window );

        /* aggregate input with a message using symmetric-normalised aggregation */
        for( uint32_t k=0 ; k<num_features ; k++ ) {
          message[k] += repetition * neighbor_input[k] / sqrt(in_degrees[i]);
        }
      }

      /**
        Once all inputs are aggreagted into a message, the network can calculate
        the output by multiplying the message with the weight matrix and adding bias.
        After, apply non-linearity (ReLU).

        Note: bias is already added implicitly when `input` is initialized.
       */
      double* W = &weights[layer * num_features * num_features];
      for( uint32_t ii=0 ; ii<num_features ; ii++ ) {
        for( uint32_t jj=0 ; jj<num_features ; jj++ ) {
          input[i*num_features + ii] += W[ii * num_features + jj] * message[jj];
        }
        input[i*num_features + ii] = fmax(0.0, input[i*num_features + ii]);
      }
    }

    MPI_Barrier( transaction->db->comm );
  }

  /**
    clean up
   */
  RMA_Win_unlock_all( output_window );

  RMA_Win_free( &output_window );

  free( local_uids );
  free( neighbor_uids );
  free( message );
  free( neighbor_input );
  free( in_degrees );

  *y_pred = input;
  *elem_cnt = local_num_verts;
  return 0;
}


/**
  based on lcc code by George Mitenkov
  optimized by Robert Gerstenberger
 */
int nod_lcc( uint64_t global_num_verts, GDI_Transaction transaction, double** coefficients, uint64_t** v_ids, size_t* elem_cnt ) {
  int status;

  size_t assoc_count = 0; /* track the number of vertices associated with the collective transaction */

  /**
    next step is a bit iffy: we compute the vertex UIDs of the vertices
    local to this process

    equivalent would be a GDI_GetLocalVerticesOfIndex call

    normally afterwards, we would need to extract the highest block
    offset to use the memory allocations of the windows; because of
    how the database is created, local_num_verts is that maximum
   */
  uint64_t local_num_verts = global_num_verts / transaction->db->commsize;
  if( (uint64_t)(transaction->db->commrank) < (global_num_verts - local_num_verts * transaction->db->commsize) ) {
    local_num_verts++;
  }

  GDI_Vertex_uid* local_uids = malloc( local_num_verts * sizeof(GDI_Vertex_uid) );
  if( local_uids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
    GDA_SetDPointer( i * transaction->db->block_size, transaction->db->commrank, &local_uids[i] );
  }

  /**
    data allocations
   */
  *v_ids = malloc( local_num_verts * sizeof(uint64_t) );
  if( *v_ids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  double* tc = malloc( local_num_verts * sizeof(double) );
  if( tc == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  size_t nneighbor_max_count = 32;
  GDI_Vertex_uid* nneighbor_uids = malloc( nneighbor_max_count * sizeof(GDI_Vertex_uid) );
  if( nneighbor_uids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  size_t max_adjacent_count = 0;
  for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
    GDI_VertexHolder vertex;
    bound_memory( assoc_count );
    status = GDI_AssociateVertex( local_uids[i], transaction, &vertex );
    assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );

    if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
      fprintf( stderr, "Rank %i: vertex association was transaction-critical.\n", transaction->db->commrank );
      MPI_Abort( transaction->db->comm, -4 );
    }

    size_t result_count, offset_result_count, array_of_offsets[2];

    uint64_t degree;
    status = GDI_GetPropertiesOfVertex( &degree, 1, &result_count, array_of_offsets, 2, &offset_result_count, GDI_PROPERTY_TYPE_DEGREE, vertex );
    assert( (status == GDI_SUCCESS) && (offset_result_count == 2) );

    if( degree > max_adjacent_count ) {
      max_adjacent_count = degree;
    }

    status = GDI_GetPropertiesOfVertex( &((*v_ids)[i]), 8, &result_count,
      array_of_offsets, 2 /* offset count */, &offset_result_count, GDI_PROPERTY_TYPE_ID, vertex );
    assert( (status == GDI_SUCCESS) && (offset_result_count == 2) );
  }

  GDI_Vertex_uid* neighbor_uids = malloc( max_adjacent_count * sizeof(GDI_Vertex_uid) );
  if( neighbor_uids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
    MPI_Abort( transaction->db->comm, -2 );
  }

  /**
    compute the number of triangles each local vertex is part of
   */
  for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
    GDI_VertexHolder vertex;
    bound_memory( assoc_count );
    status = GDI_AssociateVertex( local_uids[i], transaction, &vertex );
    assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );

    if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
      fprintf( stderr, "Rank %i: vertex association was transaction-critical.\n", transaction->db->commrank );
      MPI_Abort( transaction->db->comm, -4 );
    }

    /* obtain the neighbors */
    size_t neighbors_count;
    status = GDI_GetNeighborVerticesOfVertex( neighbor_uids, max_adjacent_count, &neighbors_count, GDI_CONSTRAINT_NULL, GDI_EDGE_UNDIRECTED, vertex );
    assert( status == GDI_SUCCESS );

    /**
      use a hashmap to store the number of times a vertex is a neighbor
     */
    radix_sort7( neighbor_uids, neighbors_count );

    GDA_HashMap* neighbor_hm;
    GDA_hashmap_create( &neighbor_hm, sizeof(GDA_DPointer) /* key size */, 2 * neighbors_count /* capacity: 50% filled */, sizeof(uint64_t) /* value size */, &GDA_int64_to_int );

    tc[i] = 0.0;
    uint64_t degree = neighbors_count;
    for( uint64_t j=0 ; j<neighbors_count ; j++ ) {
      uint64_t repetition = 1;
      while( ((j+1) < neighbors_count) && (neighbor_uids[j] == neighbor_uids[j+1]) ) {
        j++;
        repetition++;
      }

      if( local_uids[i] != neighbor_uids[j] ) { /* eliminate self loops */
        GDA_hashmap_insert( neighbor_hm, &neighbor_uids[j], &repetition );
      } else {
        degree -= repetition; /* remove edges from self loops */
      }
    }

    /* go through neighbors */
    for( size_t j=0 ; j<neighbors_count ; j++ ) {
      uint64_t repetition = 1;
      while( ((j+1) < neighbors_count) && (neighbor_uids[j] == neighbor_uids[j+1]) ) {
        j++;
        repetition++;
      }

      if( local_uids[i] != neighbor_uids[j] ) { /* eliminate self loops */
        bound_memory( assoc_count );
        status = GDI_AssociateVertex( neighbor_uids[j], transaction, &vertex );
        assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );

        if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
          fprintf( stderr, "Rank %i: vertex association was transaction-critical.\n", transaction->db->commrank  );
          MPI_Abort( transaction->db->comm, -4 );
        }

        size_t nneighbors_count;
        status = GDI_GetNeighborVerticesOfVertex( nneighbor_uids, nneighbor_max_count, &nneighbors_count, GDI_CONSTRAINT_NULL, GDI_EDGE_UNDIRECTED, vertex );
        if( status == GDI_ERROR_TRUNCATE ) {
          status = GDI_GetNeighborVerticesOfVertex( NULL, 0, &nneighbor_max_count, GDI_CONSTRAINT_NULL, GDI_EDGE_UNDIRECTED, vertex );
          assert( status == GDI_SUCCESS );
          nneighbor_uids = realloc( nneighbor_uids, nneighbor_max_count * sizeof(GDI_Vertex_uid) );
          if( nneighbor_uids == NULL ) {
            fprintf( stderr, "Not enough memory on rank %i.\n", transaction->db->commrank );
            MPI_Abort( transaction->db->comm, -2 );
          }
          status = GDI_GetNeighborVerticesOfVertex( nneighbor_uids, nneighbor_max_count, &nneighbors_count, GDI_CONSTRAINT_NULL, GDI_EDGE_UNDIRECTED, vertex );
        }
        assert( status == GDI_SUCCESS );

        radix_sort7( nneighbor_uids, nneighbors_count );

        for( size_t k=0 ; k<nneighbors_count ; k++ ) {
          uint64_t nrepetition = 1;
          while( ((k+1) < nneighbors_count) && (nneighbor_uids[k] == nneighbor_uids[k+1]) ) {
            k++;
            nrepetition++;
          }

          if( neighbor_uids[j] != nneighbor_uids[k] ) { /* eliminate self loops */
            uint64_t* nfactor = GDA_hashmap_get( neighbor_hm, &nneighbor_uids[k] );
            if( nfactor != NULL ) {
              tc[i] += *nfactor * nrepetition * repetition;
            }
          }
        }
      }
    }

    GDA_hashmap_free( &neighbor_hm );

    /* correct overcounting */
    tc[i] /= 2;

    /* compute LCC */
    if( degree > 1 ) { /* avoid diving through zero */
      tc[i] /= (degree * (degree - 1));
    }
  }

  /**
    clean up
   */
  free( nneighbor_uids );
  free( neighbor_uids );
  free( local_uids );

  *coefficients = tc;
  *elem_cnt = local_num_verts;
  return 0;
}

/**
  retrieve the resource vertices that are used by projects whose name
  starts with a character identical to the letter argument and the number
  of projects for a given resource vertex that fulfill that constraint

  letter should be in the range of 'a' to 'z' or 'A' to 'Z' and the same
  across all ranks.

  limit should be at least 1.

  return codes:
  2: argument letter is not a..z or A..Z
  3: limit is equal to zero

  abort codes:
  -2: not enough memory
  -4: transaction critical error occured
 */
int business_intelligence( GDI_Label* vlabels, GDI_Label* elabels, GDI_PropertyType* ptypes, uint64_t nglobal_verts,
  GDI_Transaction transaction, GDI_Database db, MPI_Comm comm, size_t limit, char letter, char** res_name, size_t** v_count,
  size_t* elem_cnt ) {

  int rank;

  MPI_Comm_rank( comm, &rank );

  if( ((letter < 'a') || (letter > 'z')) && ((letter < 'A') || (letter > 'Z')) ) {
    if( rank == 0 ) {
      fprintf( stderr, "Letter %c is out of range.\n", letter );
    }
    return 2;
  }

  if( limit == 0 ) {
    if( rank == 0 ) {
      fprintf( stderr, "Limit should be at least 1.\n" );
    }
    return 3;
  }

  char lletter = tolower( letter );
  char uletter = toupper( letter );

  int status;

  size_t assoc_count = 0; /* track the number of vertices associated with the collective transaction */

  /**
    next step is a bit iffy: we compute the vertex UIDs of the vertices
    local to this process

    equivalent would be a GDI_GetLocalVerticesOfIndex call
   */
  uint64_t local_num_verts = nglobal_verts / transaction->db->commsize;
  if( (uint64_t)(transaction->db->commrank) < (nglobal_verts - local_num_verts * transaction->db->commsize) ) {
    local_num_verts++;
  }

  GDI_Vertex_uid* local_uids = malloc( local_num_verts * sizeof(GDI_Vertex_uid) );
  if( local_uids == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", rank );
    MPI_Abort( comm, -2 );
  }

  for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
    GDA_SetDPointer( i * transaction->db->block_size, transaction->db->commrank, &local_uids[i] );
  }

  /**
    data allocations
   */
  size_t num_resources = 32;
  if( limit < num_resources ) {
    num_resources = limit;
  }
  size_t cnt_resources = 0;
  size_t result_minimum = 0;
  int stype;
  size_t num_chars_name;
  status = GDI_GetSizeLimitOfPropertyType( &stype, &num_chars_name, ptypes[0] );
  assert( status == GDI_SUCCESS );
  num_chars_name++; /* for zero Byte at the end */
  *res_name = malloc( num_resources * num_chars_name * sizeof(char) );
  if( *res_name == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", rank );
    MPI_Abort( comm, -2 );
  }
  *v_count = malloc( num_resources * sizeof(size_t) );
  if( *v_count == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", rank );
    MPI_Abort( comm, -2 );
  }

  size_t max_adjacent_count = 32;
  GDI_Vertex_uid* v_projects = malloc( max_adjacent_count * sizeof(GDI_Vertex_uid) );
  if( v_projects == NULL ) {
    fprintf( stderr, "Not enough memory on rank %i.\n", rank );
    MPI_Abort( comm, -2 );
  }

  /* create constraint */
  GDI_Constraint constraint;
  GDI_Subconstraint subconstraint;

  status = GDI_CreateConstraint( db, &constraint );
  assert( status == GDI_SUCCESS );
  status = GDI_CreateSubconstraint( db, &subconstraint );
  assert( status == GDI_SUCCESS );
  status = GDI_AddLabelConditionToSubconstraint( elabels[10], GDI_EQUAL, subconstraint );
  assert( status == GDI_SUCCESS );
  status = GDI_AddSubconstraintToConstraint( subconstraint, constraint );
  assert( status == GDI_SUCCESS );
  status = GDI_FreeSubconstraint( &subconstraint );
  assert( status == GDI_SUCCESS );

  /**
    find all vertices with the resource label
   */
  for( uint64_t i=0 ; i<local_num_verts ; i++ ) {
    GDI_VertexHolder vertex;
    bound_memory( assoc_count );
    status = GDI_AssociateVertex( local_uids[i], transaction, &vertex );
    assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );

    if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
      fprintf( stderr, "Rank %i: vertex association was transaction-critical.\n", rank );
      MPI_Abort( comm, -4 );
    }

    GDI_Label label;
    size_t result_count;
    status = GDI_GetAllLabelsOfVertex( &label, 1, &result_count, vertex );
    assert( (status == GDI_SUCCESS) && (result_count == 1) );

    if( label == vlabels[4] ) {
      /* vertex with label resource */

      /* obtain all neighbors that are projects */
      size_t neighbors_count;
      status = GDI_GetNeighborVerticesOfVertex( v_projects, max_adjacent_count, &neighbors_count, constraint, GDI_EDGE_UNDIRECTED, vertex );
      if( status == GDI_ERROR_TRUNCATE ) {
        status = GDI_GetNeighborVerticesOfVertex( NULL, 0, &max_adjacent_count, constraint, GDI_EDGE_UNDIRECTED, vertex );
        assert( status == GDI_SUCCESS );
        v_projects = realloc( v_projects, max_adjacent_count * sizeof(GDI_Vertex_uid) );
        if( v_projects == NULL ) {
          fprintf( stderr, "Not enough memory on rank %i.\n", rank );
          MPI_Abort( comm, -2 );
        }
        status = GDI_GetNeighborVerticesOfVertex( v_projects, max_adjacent_count, &neighbors_count, constraint, GDI_EDGE_UNDIRECTED, vertex );
      }
      assert( status == GDI_SUCCESS );

      if( neighbors_count > result_minimum ) { /* prune unnecessary operations */
        char name[num_chars_name];
        size_t name_size;
        size_t offset_result_count, array_of_offsets[2];
        status = GDI_GetPropertiesOfVertex( name, num_chars_name, &name_size, array_of_offsets, 2, &offset_result_count, ptypes[0], vertex );
        assert( (status == GDI_SUCCESS) && (offset_result_count == 2) );
        name[name_size] = '\0';
        name_size++;

        size_t count = 0;
        for( size_t j=0 ; j<neighbors_count ; j++ ) {
          GDI_VertexHolder neighbor;
          bound_memory( assoc_count );
          status = GDI_AssociateVertex( v_projects[j], transaction, &neighbor );
          assert( (status == GDI_SUCCESS) || (status == GDI_ERROR_TRANSACTION_CRITICAL) );

          if( status == GDI_ERROR_TRANSACTION_CRITICAL ) {
            fprintf( stderr, "Rank %i: vertex association was transaction-critical.\n", rank );
            MPI_Abort( comm, -4 );
          }

#if 0
          status = GDI_GetAllLabelsOfVertex( &label, 1, &result_count, neighbor );
          assert( (status == GDI_SUCCESS) && (result_count == 1) );
          assert( label == vlabels[3] );
#endif

          char buf[100];
          size_t offset_result_count, array_of_offsets[2];
          status = GDI_GetPropertiesOfVertex( buf, num_chars_name, &result_count, array_of_offsets, 2, &offset_result_count, ptypes[0], neighbor );
          assert( (status == GDI_SUCCESS) && (offset_result_count == 2) );

          if( (buf[0] == lletter) || (buf[0] == uletter) ) {
            count++;
          }
        }

        /* add resource vertex to results if warranted */
        if( count > 0 ) {
          if( cnt_resources == limit ) {
            if( count > result_minimum ) {
              size_t idx = cnt_resources; /* initialize to avoid compiler warnings */
              for( size_t j=0 ; j<cnt_resources ; j++ ) {
                if( (*v_count)[j] == result_minimum ) {
                  idx = j;
                  break;
                }
              }

              assert( idx < cnt_resources );

              memcpy( *res_name+idx*num_chars_name, name, name_size );
              (*v_count)[idx] = count;

              result_minimum = (*v_count)[0];
              for( size_t j=1 ; j<cnt_resources ; j++ ) {
                if( (*v_count)[j] < result_minimum ) {
                  result_minimum = (*v_count)[j];
                }
              }
            }
          } else {
            if( cnt_resources == num_resources ) {
              /* arrays are full, have to increase */
              num_resources *= 2;
              if( limit < num_resources ) {
                num_resources = limit;
              }
              *res_name = realloc( *res_name, num_resources * num_chars_name * sizeof(char) );
              if( *res_name == NULL ) {
                fprintf( stderr, "Not enough memory on rank %i.\n", rank );
                MPI_Abort( comm, -2 );
              }
              *v_count = realloc( *v_count, num_resources * sizeof(size_t) );
              if( *v_count == NULL ) {
                fprintf( stderr, "Not enough memory on rank %i.\n", rank );
                MPI_Abort( comm, -2 );
              }
            }

            memcpy( *res_name+cnt_resources*num_chars_name, name, name_size );
            (*v_count)[cnt_resources++] = count;

            if( cnt_resources == limit ) {
              result_minimum = (*v_count)[0];
              for( size_t j=1 ; j<cnt_resources ; j++ ) {
                if( (*v_count)[j] < result_minimum ) {
                  result_minimum = (*v_count)[j];
                }
              }
            }
          }
        }
      }
    }
  }

  /**
    clean up
   */
  free( local_uids );
  free( v_projects );
  status = GDI_FreeConstraint( &constraint );
  assert( status == GDI_SUCCESS );

  *elem_cnt = cnt_resources;

  return 0;
}
