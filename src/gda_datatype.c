// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

#include <stdbool.h>

#include "gdi.h"

/**
  Function to convert a GDI_Datetime value into a GDI_Date value.

  Only the date portion of the GDI_Datetime value is relevant,
  and the time zone information will be ignored.
 */
// TODO: make this function static inline?
GDI_Date GDA_ConvertDatetimeToDate( GDI_Datetime source ) {

  GDI_Date* temp = (GDI_Date*) &source;

  return temp[1] >> 7;
}


/**
  Function to convert a GDI_Datetime value into a GDI_Time value.

  Only the time portion of the GDI_Datetime value is relevant,
  and the time zone information will be ignored.
 */
// TODO: make this function static inline?
GDI_Time GDA_ConvertDatetimeToTime( GDI_Datetime source ) {

  return (source & 0x7FFFFFF);
}


/**
  Function to convert a GDI_Date value to a GDI_Datetime value.

  The time fields of the GDI_Datetime value are set to 00:00:00.000
  and the time zone is set to 0000.
 */
// TODO: make this function static inline?
GDI_Datetime GDA_ConvertDateToDatetime( GDI_Date source ) {

  GDI_Datetime temp = source;
  return temp << 39;
}


/**
  Function to convert a GDI_Time value to a GDI_Datetime value.

  The date fields of the GDI_Datetime value are set to 01.01.0000
  and the time zone is set to 0000.
 */
// TODO: make this function static inline?
GDI_Datetime GDA_ConvertTimeToDatetime( GDI_Time source ) {

  GDI_Datetime temp = source;
  /**
    day and month fields need to be set to one, so that a valid
    GDI_Datetime object is returned
   */
  return (temp | 0x108000000000);
}


/**
  Function to test whether the time zone fields of two GDI_Datetime
  values are equal.

  Returns true, if they are equal and false, if they are not.
 */
// TODO: make this function static inline?
bool GDA_TestEqualityOfDatetimeTimeZones( GDI_Datetime operand1, GDI_Datetime operand2 ) {

  return (operand1 & 0x3FFC000000) == (operand2 & 0x3FFC000000);
}

/**
  function to apply the timezone to the other fields, so that an absolute datetime
  value is generated, where the timezone field is zero

  Two such normalized datetime values can be compared in their compressed state,
  even if their original datetime values contained different timezone information.

  Since this is an internal function, the function assumes that the input datetime
  value is valid and that a real target buffer is supplied.

  The function returns
  GDI_SUCCESS if the normalization was successful
  GDI_ERROR_RANGE, if an overflow happens

  // TODO: function doesn't cover all use cases, maybe it is more inituitive to
  // unpack the fields, with year field being an uint32_t, so overflow cases are
  // not a problem
 */
// TODO: removed function, so I can commit the file, since there were other changes
// necessary while newly arranging the headers
#if 0
int GDA_NormalizeDatetime( GDI_Datetime source, GDI_Datetime* target ) {

  uint8_t source_minute = (source >> 16) & 0x3F;
  uint8_t source_hour = (source >> 22) & 0x1F;
  uint16_t source_timezone = (source >> 27) & 0x7FF;
  uint8_t source_day = (source >> 39) & 0x1F;
  uint8_t source_month = (source >> 44) & 0x0F;
  uint16_t source_year = source >> 48;

  uint8_t timezone_minute = source_timezone & 0x3F;
  uint8_t timezone_hour = source_timezone >> 6;

  uint8_t target_minute, target_hour, target_day, target_month, target_year;

  if( source & 0x4000000000 ) {
    source_timezone = -source_timezone;
    // TODO

  } else {
    /* positive timezone */

    target_day = source_day;
    target_month = source_month;
    target_year = source_year;

    if( timezone_minute <= source_minute ) {
      target_minute = source_minute - timezone_minute;
    } else {
      /* overflow */
      target_minute = timezone_minute - source_minute;
      timezone_hour++;
    }

    if( timezone_hour <= source_hour ) {
      target_hour = source_hour - timezone_hour;
      /* finished, no need to look at the other fields */
    } else {
      target_hour = timezone_hour - source_hour;

      if( source_day == 1 ) {
        /* overflow */
        if( (source_month == 1) || (source_month == 2) || (source_month == 4) || (source_month == 6) || (source_month == 8) || (source_month == 9) || (source_month == 11) ) {
          target_day = 31;
        } else {
          if( source_month == 3 ) {
            target_day = 28;
            /* check for leap year */
            if( (source_year & 0x3) == 0 /* divisible by four */) {
              if( (source_year % 100) != 0 /* not the start of a new century */) {
                target_day = 29;
              } else {
                if( (source_year % 400) == 0 ) {
                  target_day = 29;
                }
              }
            }
          } else {
            target_day = 30;
          }
        }
        if( source_month == 1 ) {
          /* overflow */
          target_month = 12;
          if( source_year == 0 ) {
            return GDI_ERROR_RANGE;
          } else {
            target_year--;
          }
        } else {
          target_month--;
        }
      } else {
        target_day--;
      }
    }
  }

  *target = target_year;
  *target = (*target << 4) | target_month;
  *target = (*target << 5) | target_day;
  /* timezone is implicitely set to zero */
  *target = (*target << 17) | target_hour;
  *target = (*target << 6) | target_minute;
  *target = (*target << 16) | (source & 0xFFFF);

  return GDI_SUCCESS;
}
#endif
