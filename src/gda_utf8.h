// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#ifndef __GDA_UTF8_H
#define __GDA_UTF8_H

#include <stdlib.h>

/**
  header file for NOD functionality surrounding UTF-8 strings
 */


/**
  function prototypes
 */

/**
  The function places the null terminator at a position such that the
  character string remains a valid UTF-8 string. Further, the function
  truncates all trailing spaces. The function returns the resulting
  length of the string (strlen). The parameter str denotes a pointer to
  the start of the string, pos denotes the position where the string
  should be cut and is an integer from 0 to strlen(str) (including
  strlen(str)).
 */
size_t GDA_truncate_string(char* str, size_t pos);

/**
 Returns a copy of str that is truncated (removed trailing spaces and
 valid UTF-8) with at most max_size characters in length (strlen).
 */
char* GDA_copy_truncate_string(const char* str, size_t max_size);

#endif // __GDA_UTF8_H
