// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#include "gdi.h"

/**
  GDI_Date, compressed format as uint32_t, has the following format:
  bits 31..25: unused (set to zero)
  bits 24..09: year
  bits 08..05: month
  bits 04..00: day
 */

int GDI_SetDate( uint16_t year, uint8_t month, uint8_t day, GDI_Date* date ) {

  /**
    check whether input is valid

    no check for year necessary, since it is a non-negative integer by design
   */

  if( (month > 12) || (month == 0) || (day > 31) || (day == 0) ) {
    return GDI_ERROR_RANGE;
  }

  /**
    check whether the date is valid
   */
  if( (month == 2) || (month == 4) || (month == 6) || (month == 9) || (month == 11) ) {
    if( day == 31 ) {
      return GDI_ERROR_INVALID_DATE;
    }
    if( month == 2 ) {
      if( day == 30 ) {
        return GDI_ERROR_INVALID_DATE;
      }
      /**
        check for leap year
       */
      if( (year % 4 != 0) || ((year % 100 == 0) && (year % 400 != 0)) ) {
        /**
          common year
         */
        if( day == 29 ) {
          return GDI_ERROR_INVALID_DATE;
        }
      }
    }
  }

  if( date == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  *date = year;
  *date = (*date << 4) | month;
  *date = (*date << 5) | day;

  return GDI_SUCCESS;
}

int GDI_GetDate( uint16_t* year, uint8_t* month, uint8_t* day, const GDI_Date* date ) {

  /**
    check whether all pointers are valid
   */

  if( (year == NULL) || (month == NULL) || (day == NULL) ) {
    return GDI_ERROR_BUFFER;
  }

  if( date == NULL ) {
    return GDI_ERROR_DATE;
  }

  /**
    check whether input is valid
   */

  uint8_t temp_day = (*date) & 0x1F;
  uint8_t temp_month = (*date >> 5) & 0x0F;

  if( (temp_month > 12) || (temp_month == 0) || (temp_day > 31) || (temp_day == 0) ) {
    return GDI_ERROR_DATE;
  }

  /**
    only do this now, so we obey the error handling class definition and
    only write to the output buffers, once everything is validated
   */

  *year = *date >> 9;
  *month = temp_month;
  *day = temp_day;

  return GDI_SUCCESS;
}

/**
  GDI_Time, compressed format as uint32_t, has the following format:
  bits 31..27: unused (set to zero)
  bits 26..22: hour
  bits 21..16: minute
  bits 15..10: second
  bits 09..00: fraction
 */

int GDI_SetTime( uint8_t hour, uint8_t minute, uint8_t second, uint16_t fraction, GDI_Time* time ) {

  /**
    check whether input is valid
   */

  if( (hour > 23) || (minute > 59) || (second > 59) || (fraction > 999) ) {
    return GDI_ERROR_RANGE;
  }

  if( time == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  *time = hour;
  *time = (*time << 6) | minute;
  *time = (*time << 6) | second;
  *time = (*time << 10) | fraction;

  return GDI_SUCCESS;
}

int GDI_GetTime( uint8_t* hour, uint8_t* minute, uint8_t* second, uint16_t* fraction, const GDI_Time* time ) {

  /**
    check whether all pointers are valid
   */

  if( (hour == NULL) || (minute == NULL) || (second == NULL) || (fraction == NULL) ) {
    return GDI_ERROR_BUFFER;
  }

  if( time == NULL ) {
    return GDI_ERROR_TIME;
  }

  /**
    check whether input is valid
   */

  uint16_t temp_fraction = *time & 0x3FF;
  uint8_t temp_second = (*time >> 10) & 0x3F;
  uint8_t temp_minute = (*time >> 16) & 0x3F;
  uint8_t temp_hour = (*time >> 22) & 0x1F;

  if( (temp_hour > 23) || (temp_minute > 59) || (temp_second > 59) || (temp_fraction > 999) ) {
    return GDI_ERROR_TIME;
  }

  /**
    only do this now, so we obey the error handling class definition and
    only write to the output buffers, once everythin is validated
   */

  *hour = temp_hour;
  *minute = temp_minute;
  *second = temp_second;
  *fraction = temp_fraction;

  return GDI_SUCCESS;
}

/**
  GDI_Datetime, compressed format as uint64_t, has the following format:
  bits 63..48: year
  bits 47..44: month
  bits 43..39: day
  bits 38..27: timezone (bit 38 indicates whether timezone is negative (1) or not (0))
  bits 26..22: hour
  bits 21..16: minute
  bits 15..10: second
  bits 09..00: fraction

  timezone is placed in the middle, so that the GDI_Date/GDI_Time can be
  derived by a simple shift operation/bit masking
 */

int GDI_SetDatetime( uint16_t year, uint8_t month, uint8_t day, uint8_t hour, uint8_t minute, uint8_t second, uint16_t fraction, int16_t timezone, GDI_Datetime* datetime ) {

  /**
    check whether input is valid

    no check for year necessary, since it is a non-negative integer by design
   */

  if( (month > 12) || (month == 0) || (day > 31) || (day == 0) || (hour > 23) || (minute > 59) || (second > 59) || (fraction > 999) || (timezone < -1200) || (timezone > 1400) ) {
    return GDI_ERROR_RANGE;
  }

  /**
    check whether the date is valid
   */
  if( (month == 2) || (month == 4) || (month == 6) || (month == 9) || (month == 11) ) {
    if( day == 31 ) {
      return GDI_ERROR_INVALID_DATE;
    }
    if( month == 2 ) {
      if( day == 30 ) {
        return GDI_ERROR_INVALID_DATE;
      }
      /**
        check for leap year
       */
      if( (year % 4 != 0) || ((year % 100 == 0) && (year % 400 != 0)) ) {
        /**
          common year
         */
        if( day == 29 ) {
          return GDI_ERROR_INVALID_DATE;
        }
      }
    }
  }

  if( datetime == NULL ) {
    return GDI_ERROR_BUFFER;
  }

  *datetime = year;
  *datetime = (*datetime << 4) | month;
  *datetime = (*datetime << 5) | day;
  *datetime = *datetime << 1;
  if( timezone < 0 ) {
    *datetime = *datetime | 0x1;
  }
  *datetime = (*datetime << 11) | (timezone & 0x7FF);
  *datetime = (*datetime << 5) | hour;
  *datetime = (*datetime << 6) | minute;
  *datetime = (*datetime << 6) | second;
  *datetime = (*datetime << 10) | fraction;

  return GDI_SUCCESS;
}

int GDI_GetDatetime( uint16_t* year, uint8_t* month, uint8_t* day, uint8_t* hour, uint8_t* minute, uint8_t* second, uint16_t* fraction, int16_t* timezone, const GDI_Datetime* datetime ) {

  /**
    check whether all pointers are valid
   */

  if( (year == NULL) || (month == NULL) || (day == NULL) || (hour == NULL) || (minute == NULL) || (second == NULL) || (fraction == NULL) || (timezone == NULL) ) {
    return GDI_ERROR_BUFFER;
  }

  if( datetime == NULL ) {
    return GDI_ERROR_DATETIME;
  }

  /**
    check whether input is valid
   */

  uint16_t temp_fraction = *datetime & 0x3FF;
  uint8_t temp_second = (*datetime >> 10) & 0x3F;
  uint8_t temp_minute = (*datetime >> 16) & 0x3F;
  uint8_t temp_hour = (*datetime >> 22) & 0x1F;
  int16_t temp_timezone = (*datetime >> 27) & 0x7FF;
  if( *datetime & 0x4000000000 ) {
    temp_timezone = -temp_timezone;
  }
  uint8_t temp_day = (*datetime >> 39) & 0x1F;
  uint8_t temp_month = (*datetime >> 44) & 0x0F;

  if( (temp_hour > 23) || (temp_minute > 59) || (temp_second > 59) || (temp_fraction > 999) || (temp_timezone < -1200) || (temp_timezone > 1400)
    || (temp_month > 12) || (temp_month == 0) || (temp_day > 31) || (temp_day == 0) ) {
    return GDI_ERROR_DATETIME;
  }

  /**
    only do this now, so we obey the error handling class definition and
    only write to the output buffers, once everything is validated
   */

  *year = *datetime >> 48;
  *month = temp_month;
  *day = temp_day;
  *timezone = temp_timezone;
  *hour = temp_hour;
  *minute = temp_minute;
  *second = temp_second;
  *fraction = temp_fraction;

  return GDI_SUCCESS;
}

int GDI_GetSizeOfDatatype( size_t* size, GDI_Datatype dtype ) {
  if(size == NULL) {
    return GDI_ERROR_BUFFER;
  }

  if(dtype >= GDI_CHAR && dtype <= GDI_BYTE) {
    *size = 1;
    return GDI_SUCCESS;
  }
  else if((dtype == GDI_INT16_T) || (dtype == GDI_UINT16_T)) {
    *size = 2;
    return GDI_SUCCESS;
  }
  else if(dtype >= GDI_INT32_T && dtype <= GDI_TIME) {
    *size = 4;
    return GDI_SUCCESS;
  }
  else if(dtype >= GDI_INT64_T && dtype <= GDI_DATETIME) {
    *size = 8;
    return GDI_SUCCESS;
  }
  else if(dtype == GDI_DECIMAL) {
    *size = sizeof(GDI_Decimal);
    return GDI_SUCCESS;
  }

  return GDI_ERROR_DATATYPE;
}
