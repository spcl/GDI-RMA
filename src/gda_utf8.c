// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Marc Fischer

#include <string.h>

#include "gda_utf8.h"

#define MIN(a,b) (((a)<(b))?(a):(b))


/**
  Stolen from https://gist.github.com/w-vi/67fe49106c62421992a2
  The parameter buf points to the start of a string,
  len denotes the position in the range from 0 to strlen(buf)
  (including strlen(buf)) at which the string should be cut off.
 */
static inline size_t fix_utf8(char *buf, size_t len) {
  if(len == 0) {
    return 0;
  }

  /**
    Might truncate up to 3 bytes
   */
  char *b = buf + len - 3;

  /**
    Is the last byte part of multi-byte sequence?
   */
  if (b[2] & 0x80) {
    /**
      Is the last byte in buffer the first byte in a new
      multi-byte sequence?
     */
    if (b[2] & 0x40) return len - 1;
    /**
      Is it a 3 byte sequence?
     */
    else if (len > 1 && (b[1] & 0xe0) == 0xe0) return len - 2;
    /**
      Is it a 4 byte sequence?
     */
    else if (len > 2 && (b[0] & 0xf0) == 0xf0) return len - 3;
  }
  return len;
}


size_t GDA_truncate_string(char* str, size_t pos) {
  if(pos == 0) {
    str[0] = '\0';
    return 0;
  }

  size_t zero_pos = fix_utf8(str, pos);

  while(zero_pos > 0 && str[zero_pos-1] == ' ') {
    zero_pos--;
  }

  str[zero_pos] = '\0';
  return zero_pos;
}


char* GDA_copy_truncate_string(const char* str, size_t max_size) {
  size_t len = MIN(strlen(str), max_size);
  char* copy = malloc((len+1) * sizeof(char));
  strncpy(copy, str, len);
  GDA_truncate_string(copy, len);
  return copy;
}
