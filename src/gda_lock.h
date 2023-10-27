// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Wojciech Chlapek

#ifndef __GDA_LOCK_H
#define __GDA_LOCK_H

#include "gdi.h"

/**
  constant definitions
 */
#define GDA_NO_LOCK     190
#define GDA_READ_LOCK   191
#define GDA_WRITE_LOCK  192


/**
  function prototypes
 */
void GDA_AcquireVertexReadLock( GDI_VertexHolder vertex );
void GDA_UpdateToVertexWriteLock( GDI_VertexHolder vertex );
/**
  GDA_SetVertexWriteLock should only be called from GDI_CreateVertex,
  after the primary block is acquired from the free memory management,
  so we already know, that we are the only process with access to that
  block
 */
void GDA_SetVertexWriteLock( GDI_VertexHolder vertex );
void GDA_ReleaseVertexLock( GDI_VertexHolder vertex );

#endif // __GDA_LOCK_H
