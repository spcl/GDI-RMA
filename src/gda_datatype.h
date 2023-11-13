// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef __GDA_DATATYPE_H
#define __GDA_DATATYPE_H

#include <stdbool.h>

/**
  header file for additional NOD functionality for
  GDI data types

  assumes that this header file is included after gdi.h
 */


/**
  function prototypes
 */

/**
  Function to convert a GDI_Datetime value into a GDI_Date value.

  Only the date portion of the GDI_Datetime value is relevant,
  and the time zone information will be ignored.
 */
GDI_Date GDA_ConvertDatetimeToDate( GDI_Datetime source );

/**
  Function to convert a GDI_Datetime value into a GDI_Time value.

  Only the time portion of the GDI_Datetime value is relevant,
  and the time zone information will be ignored.
 */
GDI_Time GDA_ConvertDatetimeToTime( GDI_Datetime source );

/**
  Function to convert a GDI_Date value to a GDI_Datetime value.

  The time fields of the GDI_Datetime value are set to 00:00:00.000
  and the time zone is set to 0000.
 */
GDI_Datetime GDA_ConvertDateToDatetime( GDI_Date source );

/**
  Function to convert a GDI_Time value to a GDI_Datetime value.

  The date fields of the GDI_Datetime value are set to 01.01.0000
  and the time zone is set to 0000.
 */
GDI_Datetime GDA_ConvertTimeToDatetime( GDI_Time source );

/**
  Function to test whether the time zone fields of two GDI_Datetime
  values are equal.

  Returns true, if they are equal and false, if they are not.
 */
bool GDA_TestEqualityOfDatetimeTimeZones( GDI_Datetime operand1, GDI_Datetime operand2 );


/**
  function definitions
 */

/**
  Convenience function to check if the given data type is a valid data type handle.

  Returns true if the data type is valid, false otherwise.
 */
static inline bool GDA_IsDatatypeValid(GDI_Datatype dtype) {
  return (dtype >= GDI_CHAR && dtype <= GDI_DECIMAL);
}

/**
  Convenience function that returns true when the given data type is a
  C integer, false otherwise.

  The function is not meant for external use, so it has not the GDA_
  prefix.
 */
static inline bool is_cinteger(GDI_Datatype dtype) {
  return dtype == GDI_INT8_T || dtype == GDI_UINT8_T|| dtype == GDI_INT16_T || dtype == GDI_UINT16_T ||
    dtype == GDI_INT32_T || dtype == GDI_UINT32_T || dtype == GDI_INT64_T || dtype == GDI_UINT64_T ||
    dtype == GDI_BOOL;
}

/**
  Convenience function that returns true when the given data type is a
  floating point, false otherwise.

  The function is not meant for external use, so it has not the GDA_
  prefix.
 */
static inline bool is_floatingpoint(GDI_Datatype dtype) {
  return dtype == GDI_FLOAT || dtype == GDI_DOUBLE;
}

/**
  Convenience function that determines if it is valid to convert the
  data type from to the given data type to.

  If it is valid, the function returns true
  and if invalid, the function returns false.
 */
static inline bool GDA_CanConvertDatatypes(GDI_Datatype from, GDI_Datatype to) {
  if((from == to) || (to == GDI_BYTE)) {
    return true;;
  }

  if(is_cinteger(from) || is_floatingpoint(from)) {
    if(is_cinteger(to) || is_floatingpoint(to) || to == GDI_DECIMAL) {
      return true;
    } else {
      return false;
    }
  }

  if(from == GDI_DECIMAL) {
    if(is_cinteger(to) || is_floatingpoint(to)) {
      return true;
    } else {
      return false;
    }
  }

  if(from == GDI_TIME) {
    if(to == GDI_DATETIME) {
      return true;
    } else {
      return false;
    }
  }

  if(from == GDI_DATE) {
    if(to == GDI_DATETIME) {
      return true;
    } else {
      return false;
    }
  }

  if(from == GDI_DATETIME) {
    if(to == GDI_DATE || to == GDI_TIME) {
      return true;
    } else {
      return false;
    }
  }

  return false;
}

#endif // __GDA_DATATYPE_H
