// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#ifndef __GDA_VERTEX_H
#define __GDA_VERTEX_H

#define GDA_OFFSET_NUM_BLOCKS             0
#define GDA_OFFSET_NUM_LIGHTWEIGHT_EDGES  (GDA_OFFSET_NUM_BLOCKS+4)
#define GDA_OFFSET_SIZE_PROPERTY_DATA     (GDA_OFFSET_NUM_LIGHTWEIGHT_EDGES+4)
#define GDA_OFFSET_SIZE_UNUSED_SPACE      (GDA_OFFSET_SIZE_PROPERTY_DATA+8)

/**
  should be 24 Bytes
 */
#define GDA_VERTEX_METADATA_SIZE          (GDA_OFFSET_SIZE_UNUSED_SPACE+8)

void GDA_AssociateVertex( GDI_Vertex_uid internal_uid, GDI_Transaction transaction, GDI_VertexHolder vertex );

#endif // #ifndef __GDA_VERTEX_H
