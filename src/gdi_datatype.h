// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef __GDI_DATATYPE_H
#define __GDI_DATATYPE_H

#include <inttypes.h>

/**
  header file for GDI data types
 */


/**
  constant definitions
 */

/**
  defintion of the GDI_Datatype constants
  some functions rely on the order below, so the order shouldn't be changed
 */
/**
  the following data types occupy 1 Byte (100 to 104)
 */
#define GDI_CHAR     100
#define GDI_INT8_T   101
#define GDI_UINT8_T  102
#define GDI_BOOL     103
#define GDI_BYTE     104
/**
  the following data types occupy 2 Bytes (105 to 106)
 */
#define GDI_INT16_T  105
#define GDI_UINT16_T 106
/**
  the following data types occupy 4 Bytes (107 to 111)
 */
#define GDI_INT32_T  107
#define GDI_UINT32_T 108
#define GDI_FLOAT    109
#define GDI_DATE     110
#define GDI_TIME     111
/**
  the following data types occupy 8 Bytes (112 to 115)
 */
#define GDI_INT64_T  112
#define GDI_UINT64_T 113
#define GDI_DOUBLE   114
#define GDI_DATETIME 115
/**
  the following data types occupy a variable number of bytes (116)
 */
#define GDI_DECIMAL  116

// TODO: might not be the final amount
#define GDI_DECIMAL_NBYTES 67


/**
  data type definitions
 */

typedef uint8_t  GDI_Datatype;
typedef uint32_t GDI_Date;
typedef uint32_t GDI_Time;
typedef uint64_t GDI_Datetime;

typedef struct GDI_Decimal
{
  char x[GDI_DECIMAL_NBYTES];
} GDI_Decimal;


/**
  function prototypes
 */

int GDI_GetDate( uint16_t* year, uint8_t* month, uint8_t* day, const GDI_Date* date );
int GDI_SetDate( uint16_t year, uint8_t month, uint8_t day, GDI_Date* date );
int GDI_GetTime( uint8_t* hour, uint8_t* minute, uint8_t* second, uint16_t* fraction, const GDI_Time* time );
int GDI_SetTime( uint8_t hour, uint8_t minute, uint8_t second, uint16_t fraction, GDI_Time* time );
int GDI_GetDatetime( uint16_t* year, uint8_t* month, uint8_t* day, uint8_t* hour, uint8_t* minute, uint8_t* second, uint16_t* fraction, int16_t* timezone, const GDI_Datetime* datetime );
int GDI_SetDatetime( uint16_t year, uint8_t month, uint8_t day, uint8_t hour, uint8_t minute, uint8_t second, uint16_t fraction, int16_t timezone, GDI_Datetime* datetime );
int GDI_GetSizeOfDatatype( size_t* size, GDI_Datatype dtype );

#endif // #ifndef __GDI_DATATYPE_H
