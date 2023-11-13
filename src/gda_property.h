// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#ifndef __GDA_PROPERTY_H
#define __GDA_PROPERTY_H

/**
  a check for the maximum size of those meta data fields is included in the
  code of GDA_LinearScanningAddProperty,
  GDA_LinearScanningRemoveSpecificProperty,
  GDA_LinearScanningUpdateSingleEntityProperty,
  GDA_LinearScanningUpdateSpecificProperty and
  GDA_LinearScanningSetSingleEntityProperty, so any changes to
  GDA_PropertyHandle and GDA_PropertyRecordSize should update that code as well
 */
typedef uint8_t   GDA_PropertyHandle;
/**
  TODO: change this again to uint16_t
 */
typedef uint64_t  GDA_PropertyRecordSize;

#define GDA_PROPERTY_HANDLE_SIZE      sizeof(GDA_PropertyHandle)
#define GDA_PROPERTY_RECORD_SIZE      sizeof(GDA_PropertyRecordSize)
#define GDA_PROPERTY_METADATA_SIZE    (GDA_PROPERTY_HANDLE_SIZE + GDA_PROPERTY_RECORD_SIZE)
#define GDA_PROPERTY_OFFSET_PRIMARY   0
#define GDA_PROPERTY_OFFSET_BLOCK     0

#define GDA_PROPERTY_EMPTY    0
#define GDA_PROPERTY_LAST     1
#define GDA_PROPERTY_LABEL    2

/**
  TODO: Also use a return type, where appropriate, for the label functions?
 */
int GDA_LinearScanningAddProperty( GDI_PropertyType ptype, const void* value, size_t count, GDI_VertexHolder vertex );
int GDA_LinearScanningFindAllProperties( void* buf, size_t buf_count, size_t* buf_resultcount, size_t* array_of_offsets, size_t offset_count, size_t* offset_resultcount, GDI_PropertyType ptype, GDI_VertexHolder vertex );
int GDA_LinearScanningFindAllPropertyTypes( GDI_VertexHolder vertex, GDI_PropertyType* ptypes, size_t count, size_t* resultcount );
void GDA_LinearScanningNumProperties( GDI_VertexHolder vertex, GDI_PropertyType ptype, size_t* resultcount, size_t* element_resultcount );
void GDA_LinearScanningNumPropertyTypes( GDI_VertexHolder vertex, size_t* resultcount );
void GDA_LinearScanningRemoveProperties( GDI_PropertyType ptype, GDI_VertexHolder vertex, bool* found_flag );
void GDA_LinearScanningRemoveSpecificProperty( GDI_PropertyType ptype, const void* value, size_t count, GDI_VertexHolder vertex, bool* found_flag );
void GDA_LinearScanningSetSingleEntityProperty( GDI_PropertyType ptype, const void* value, size_t count, GDI_VertexHolder vertex, bool* found_flag );
int GDA_LinearScanningUpdateSingleEntityProperty( GDI_PropertyType ptype, const void* value, size_t count, GDI_VertexHolder vertex );
int GDA_LinearScanningUpdateSpecificProperty( GDI_PropertyType ptype, const void* old_value, size_t old_count, const void* new_value, size_t new_count, GDI_VertexHolder vertex );

int GDA_LinearScanningFindAllLabels( GDI_VertexHolder vertex, GDI_Label* labels, size_t count, size_t* resultcount );
void GDA_LinearScanningInitPropertyList( GDI_VertexHolder vertex );
void GDA_LinearScanningInsertLabel( GDI_Label label, GDI_VertexHolder vertex, bool* found_flag );
void GDA_LinearScanningNumLabels( GDI_VertexHolder vertex, size_t* resultcount );
void GDA_LinearScanningRemoveLabel( GDI_Label label, GDI_VertexHolder vertex, bool* found_flag );

#endif // #ifndef __GDA_PROPERTY_H
