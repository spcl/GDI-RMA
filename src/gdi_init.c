// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#include <assert.h>
#include <string.h>

#include "gdi.h"

int GDI_Init( int *argc, char ***argv ) {
  // predefined label: GDI_LABEL_NONE
  GDI_LABEL_NONE = malloc(sizeof( GDI_Label_desc_t ));
  assert(GDI_LABEL_NONE != NULL);
  GDI_LABEL_NONE->name = calloc(15,  sizeof(char) );
  strcpy(GDI_LABEL_NONE->name, "GDI_LABEL_NONE");
  GDI_LABEL_NONE->db = NULL;
  GDI_LABEL_NONE->int_handle = 0;

  // predefined property type: GDI_PROPERTY_TYPE_ID
  GDI_PROPERTY_TYPE_ID = malloc(sizeof(GDI_PropertyType_desc_t));
  GDI_PROPERTY_TYPE_ID->db = NULL;
  GDI_PROPERTY_TYPE_ID->name = calloc(21, sizeof (char));
  strcpy(GDI_PROPERTY_TYPE_ID->name, "GDI_PROPERTY_TYPE_ID");
  GDI_PROPERTY_TYPE_ID->etype = GDI_SINGLE_ENTITY;
  GDI_PROPERTY_TYPE_ID->dtype = GDI_BYTE;
  GDI_PROPERTY_TYPE_ID->stype = GDI_NO_SIZE_LIMIT;
  GDI_PROPERTY_TYPE_ID->count = 0;
  GDI_PROPERTY_TYPE_ID->int_handle = 3; /* value is fixed due to the label/property handling */

  // predefined property type: GDI_PROPERTY_TYPE_DEGREE
  GDI_PROPERTY_TYPE_DEGREE = malloc(sizeof(GDI_PropertyType_desc_t));
  GDI_PROPERTY_TYPE_DEGREE->db = NULL;
  GDI_PROPERTY_TYPE_DEGREE->name = calloc(25, sizeof (char));
  strcpy(GDI_PROPERTY_TYPE_DEGREE->name, "GDI_PROPERTY_TYPE_DEGREE");
  GDI_PROPERTY_TYPE_DEGREE->etype = GDI_SINGLE_ENTITY;
  GDI_PROPERTY_TYPE_DEGREE->dtype = GDI_UINT64_T;
  GDI_PROPERTY_TYPE_DEGREE->stype = GDI_FIXED_SIZE;
  GDI_PROPERTY_TYPE_DEGREE->count = 1;
  GDI_PROPERTY_TYPE_DEGREE->int_handle = 0;

  // predefined property type: GDI_PROPERTY_TYPE_INDEGREE
  GDI_PROPERTY_TYPE_INDEGREE = malloc(sizeof(GDI_PropertyType_desc_t));
  GDI_PROPERTY_TYPE_INDEGREE->db = NULL;
  GDI_PROPERTY_TYPE_INDEGREE->name = calloc(27, sizeof (char));
  strcpy(GDI_PROPERTY_TYPE_INDEGREE->name, "GDI_PROPERTY_TYPE_INDEGREE");
  GDI_PROPERTY_TYPE_INDEGREE->etype = GDI_SINGLE_ENTITY;
  GDI_PROPERTY_TYPE_INDEGREE->dtype = GDI_UINT64_T;
  GDI_PROPERTY_TYPE_INDEGREE->stype = GDI_FIXED_SIZE;
  GDI_PROPERTY_TYPE_INDEGREE->count = 1;
  GDI_PROPERTY_TYPE_INDEGREE->int_handle = 1;

  // predefined property type: GDI_PROPERTY_TYPE_OUTDEGREE
  GDI_PROPERTY_TYPE_OUTDEGREE = malloc(sizeof(GDI_PropertyType_desc_t));
  GDI_PROPERTY_TYPE_OUTDEGREE->db = NULL;
  GDI_PROPERTY_TYPE_OUTDEGREE->name = calloc(28, sizeof (char));
  strcpy(GDI_PROPERTY_TYPE_OUTDEGREE->name, "GDI_PROPERTY_TYPE_OUTDEGREE");
  GDI_PROPERTY_TYPE_OUTDEGREE->etype = GDI_SINGLE_ENTITY;
  GDI_PROPERTY_TYPE_OUTDEGREE->dtype = GDI_UINT64_T;
  GDI_PROPERTY_TYPE_OUTDEGREE->stype = GDI_FIXED_SIZE;
  GDI_PROPERTY_TYPE_OUTDEGREE->count = 1;
  GDI_PROPERTY_TYPE_OUTDEGREE->int_handle = 2;

  return GDI_SUCCESS;
}

int GDI_Finalize() {
  // predefined label: GDI_LABEL_NONE
  free(GDI_LABEL_NONE->name);
  free(GDI_LABEL_NONE);

  // predefined property type: GDI_PROPERTY_TYPE_ID
  free(GDI_PROPERTY_TYPE_ID->name);
  free(GDI_PROPERTY_TYPE_ID);

  // predefined property type: GDI_PROPERTY_TYPE_DEGREE
  free(GDI_PROPERTY_TYPE_DEGREE->name);
  free(GDI_PROPERTY_TYPE_DEGREE);

  // predefined property type: GDI_PROPERTY_TYPE_INDEGREE
  free(GDI_PROPERTY_TYPE_INDEGREE->name);
  free(GDI_PROPERTY_TYPE_INDEGREE);

  // predefined property type: GDI_PROPERTY_TYPE_OUTDEGREE
  free(GDI_PROPERTY_TYPE_OUTDEGREE->name);
  free(GDI_PROPERTY_TYPE_OUTDEGREE);

  return GDI_SUCCESS;
}
