// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#ifndef __GDI_OPERATION_H_
#define __GDI_OPERATION_H_

/**
  header file for GDI operations
 */


/**
  constant defintions
 */

/**
  GDI operations

  TODO: move them, so they are distinct from the error code definitions?
 */
#define GDI_EQUAL       0
#define GDI_NOTEQUAL    1
#define GDI_GREATER     2
#define GDI_EQGREATER   3
#define GDI_SMALLER     4
#define GDI_EQSMALLER   5
#define GDI_OP_END      5


/**
  data type defintions
 */
typedef uint8_t GDI_Op;

#endif // __GDI_OPERATION_H_
