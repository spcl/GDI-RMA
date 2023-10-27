// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#ifndef __GDA_OPERATION_H_
#define __GDA_OPERATION_H_

/**
  header file for additional NOD functionality for
  GDI operations

  assumes that this header file is included after gdi.h
 */


/**
  function definitions
 */

/**
  Convenience function that checks if a given operation handle is valid.
 */
static inline bool GDA_IsOpValid(GDI_Op op) {
  /**
    note: lower bound check is not required since it is an unsigned type
   */
  return (op <= GDI_OP_END);
}

/**
  Convenience function that checks if an operation is allowed on a data type.
  The function assumes that the operation and data type are valid.
 */
inline bool GDA_IsOpAllowedOnDatatype(GDI_Op op, GDI_Datatype dtype) {
  /**
    this code only outlines the full decision - the shortened version is below

  if(dtype == GDI_INT8_T || dtype == GDI_UINT8_T || dtype == GDI_INT16_T ||
     dtype == GDI_UINT16_T || dtype == GDI_INT32_T || dtype == GDI_UINT32_T ||
     dtype == GDI_INT64_T || dtype == GDI_UINT64_T || dtype == GDI_BOOL ||
     dtype == GDI_TIME || dtype == GDI_DATE || dtype == GDI_DATETIME || dtype == GDI_DECIMAL) {
    return true;
  }

  if(dtype == GDI_CHAR || dtype == GDI_BYTE) {
    if(op == GDI_EQUAL || op == GDI_NOTEQUAL) {
      return true;
    } else {
      return false;
    }
  }

  if(dtype == GDI_FLOAT || dtype == GDI_DOUBLE) {
    if(op == GDI_GREATER || op == GDI_SMALLER) {
      return true;
    } else {
      return false;
    }
  }
  */
  if( ((dtype == GDI_CHAR || dtype == GDI_BYTE) && !(op == GDI_EQUAL || op == GDI_NOTEQUAL)) ||
      ((dtype == GDI_FLOAT || dtype == GDI_DOUBLE) && !(op == GDI_GREATER || op == GDI_SMALLER)) ) {
    return false;
  }
  return true;
}

#endif // __GDA_OPERATION_H_
