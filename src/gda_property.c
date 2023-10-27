// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger
//
// The source code of the function GDA_LinearScanningFindAllProperties
// was written by Wojciech Chlapek.

#include <assert.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include "gdi.h"
#include "gda_property.h"

/**
  linear scanning:
  organizes property and label as a linked list of records

  a record consists of the meta data and the actual data:
   -------- ---------------- --------   -------- --------
  | handle |      size      |   data ...        | handle |...
   -------- ---------------- --------   -------- --------

  handle = handle to indicate the property type of the record.
  A label will be treated as a property type.
  possible values:
  0 = empty record
  1 = last record
  2 = label
  3 = GDI_PROPERTY_TYPE_ID
  4... = user-defined property types

  GDI_PROPERTY_TYPE_DEGREE, GDI_PROPERTY_TYPE_INDEGREE and GDI_PROPERTY_TYPE_OUTDEGREE have to be handled differently

  size = size of the data in Bytes, 0 is a valid size

  The position of the next record is position of the current record + meta data size + size.
 */


/**
  assumes that pos points to the beginning of the meta data of a record

  TODO: function will be more complex, once the block layout is considered
 */
static inline char* GDA_NextPropertyRecord( char* pos ) {
  return pos + GDA_PROPERTY_METADATA_SIZE + *(GDA_PropertyRecordSize*)(pos+GDA_PROPERTY_HANDLE_SIZE);
}


void GDA_LinearScanningInitPropertyList( GDI_VertexHolder vertex ) {
  vertex->property_size = 32;
  vertex->property_data = malloc( 32 );
  vertex->unused_space = vertex->property_size - GDA_PROPERTY_OFFSET_PRIMARY - GDA_PROPERTY_METADATA_SIZE /* for the last record */;
  *(GDA_PropertyHandle*) ((vertex->property_data) + GDA_PROPERTY_OFFSET_PRIMARY) = GDA_PROPERTY_LAST;
}


/**
  assumes that label is not GDI_LABEL_NONE and GDI_LABEL_NULL

  found_flag is returned, in case the caller wants to keep skip updating indexes and indicates
  whether the label was already present on the vertex:
  true  = label was already present
  false = label was added by this function call

  updates the unused_space counter
 */
void GDA_LinearScanningInsertLabel( GDI_Label label, GDI_VertexHolder vertex, bool* found_flag ) {
  char* pos = vertex->property_data;
  GDA_PropertyHandle phandle = *(GDA_PropertyHandle*) pos;
  GDA_PropertyRecordSize label_entry_size = sizeof(uint32_t);
  char* insert_pos = NULL;

  /**
    first check whether the label is already present on the vertex
   */
  while( phandle != GDA_PROPERTY_LAST ) {
    if( phandle == GDA_PROPERTY_LABEL ) {
      uint32_t record_entry = *(uint32_t*) (pos+GDA_PROPERTY_METADATA_SIZE);
      if( record_entry == label->int_handle ) {
        *found_flag = true;
        return;
      }
    } else {
      if( phandle == GDA_PROPERTY_EMPTY ) {
        GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
        if( (record_size == label_entry_size) || (record_size >= (label_entry_size + GDA_PROPERTY_METADATA_SIZE)) ) {
          /**
            found a record entry that is empty and big enough
           */
          insert_pos = pos;
          /**
            check the remaining list to ensure that the label is not already there
           */
          pos = GDA_NextPropertyRecord( pos );
          phandle = *(GDA_PropertyHandle*) pos;
          while( phandle != GDA_PROPERTY_LAST ) {
            if( phandle == GDA_PROPERTY_LABEL ) {
              uint32_t record_entry = *(uint32_t*) (pos+GDA_PROPERTY_METADATA_SIZE);
              if( record_entry == label->int_handle ) {
                *found_flag = true;
                return;
              }
            }
            pos = GDA_NextPropertyRecord( pos );
            phandle = *(GDA_PropertyHandle*) pos;
          }
          /**
            we are already finished with traversing the list,
            so we break from the outer loop
           */
          break;
        }
      }
    }
    /**
      advance to the next record in the list
     */
    pos = GDA_NextPropertyRecord( pos );
    phandle = *(GDA_PropertyHandle*) pos;
  }

  /**
    didn't find the label, so we insert it now
   */
  *found_flag = false;

  if( insert_pos != NULL ) {
    /**
      already found an sufficient sized empty record entry during the find operation
     */
    GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE);
    if( record_size == label_entry_size ) {
      /**
        empty record fits perfectly, so we don't have to update the size part of the meta data
       */
      *(GDA_PropertyHandle*) insert_pos = GDA_PROPERTY_LABEL;
      *(uint32_t*) (insert_pos+GDA_PROPERTY_METADATA_SIZE) = label->int_handle;
    } else {
      /**
        we already know that the empty record is at least big enough to hold the new
        label record and an empty record, so no need to check again

        first set the new label record
       */
      *(GDA_PropertyHandle*) insert_pos = GDA_PROPERTY_LABEL;
      *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = label_entry_size;
      *(uint32_t*) (insert_pos+GDA_PROPERTY_METADATA_SIZE) = label->int_handle;
      /**
        set a new empty record
       */
      insert_pos = insert_pos + GDA_PROPERTY_METADATA_SIZE + label_entry_size;
      *(GDA_PropertyHandle*) insert_pos = GDA_PROPERTY_EMPTY;
      *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = record_size - (label_entry_size + GDA_PROPERTY_METADATA_SIZE);
    }
  } else {
    assert( *(GDA_PropertyHandle*) pos == GDA_PROPERTY_LAST );
    /**
      have to insert a record at the end

      pos points to the last record (= special record at the end of the list)
     */
    size_t new_list_size = pos + GDA_PROPERTY_METADATA_SIZE - vertex->property_data /* absolute size of the used memory */
      + GDA_PROPERTY_METADATA_SIZE + sizeof(uint32_t) /* size of the new record (including meta data) */;
    if( new_list_size > vertex->property_size ) {
      /**
        allocated memory is not sufficient
       */
      size_t temp_pos = pos - vertex->property_data;
      do {
        vertex->unused_space += vertex->property_size;
        vertex->property_size = vertex->property_size << 1;
      } while( new_list_size > vertex->property_size );
      vertex->property_data = realloc( vertex->property_data, vertex->property_size );
      pos = vertex->property_data + temp_pos;
    }
    *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LABEL;
    *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) = label_entry_size;
    *(uint32_t*) (pos+GDA_PROPERTY_METADATA_SIZE) = label->int_handle;
    /**
      set a new last record
     */
    pos = pos + GDA_PROPERTY_METADATA_SIZE + label_entry_size;
    *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
  }

  /**
    update the unused space counter
   */
  assert( vertex->unused_space >= (label_entry_size + GDA_PROPERTY_METADATA_SIZE) );
  vertex->unused_space -= label_entry_size + GDA_PROPERTY_METADATA_SIZE;
}


/**
  assumes that label is not GDI_LABEL_NONE and GDI_LABEL_NULL

  found_flag is returned, in case the caller wants to keep skip updating indexes and indicates
  whether the label was present on the vertex:
  true  = label was found and removed
  false = label wasn't found

  updates the unused_space counter
 */
void GDA_LinearScanningRemoveLabel( GDI_Label label, GDI_VertexHolder vertex, bool* found_flag ) {
  char* pos = vertex->property_data;
  char* previous_record = NULL;
  GDA_PropertyHandle phandle = *(GDA_PropertyHandle*) pos;

  while( phandle != GDA_PROPERTY_LAST ) {
    if( phandle == GDA_PROPERTY_LABEL ) {
      uint32_t record_entry = *(uint32_t*) (pos+GDA_PROPERTY_METADATA_SIZE);
      if( record_entry == label->int_handle ) {
        /**
          found the label that we are looking for
         */
        *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
        /**
          introduced a new empty record, so update the appropriate counter
         */
        vertex->unused_space += sizeof(uint32_t) + GDA_PROPERTY_METADATA_SIZE;

        if( previous_record != NULL ) {
          /**
            check whether the previous record is also empty, and if so, merge them
           */
          phandle = *(GDA_PropertyHandle*) previous_record;
          if( phandle == GDA_PROPERTY_EMPTY ) {
            *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += sizeof(uint32_t) + GDA_PROPERTY_METADATA_SIZE;
            pos = previous_record;
          }
        }
        /**
          check whether the next record is also empty, and if so, merge them
         */
        char* next_record = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) next_record;
        if( phandle == GDA_PROPERTY_EMPTY ) {
          *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
        } else {
          if( phandle == GDA_PROPERTY_LAST ) {
            *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
          }
        }
        *found_flag = true;
        return;
      }
    }
    /**
      advance to the next record in the list
     */
    previous_record = pos;
    pos = GDA_NextPropertyRecord( pos );
    phandle = *(GDA_PropertyHandle*) pos;
  }

  /**
    reached the end of the list, so the label was not found
   */
  *found_flag = false;
}


/**
  assumes that label is not GDI_LABEL_NONE and GDI_LABEL_NULL
 */
void GDA_LinearScanningNumLabels( GDI_VertexHolder vertex, size_t* resultcount ) {
  size_t num_labels = 0;
  char* pos = vertex->property_data;
  GDA_PropertyHandle phandle = *(GDA_PropertyHandle*) pos;

  while( phandle != GDA_PROPERTY_LAST ) {
    if( phandle == GDA_PROPERTY_LABEL ) {
      num_labels++;
    }
    pos = GDA_NextPropertyRecord( pos );
    phandle = *(GDA_PropertyHandle*) pos;
  }

  *resultcount = num_labels;
}


/**
  assumes that label is not GDI_LABEL_NONE and GDI_LABEL_NULL

  assumes that count is greater than zero, since otherwise GDA_LinearScanningNumLabels should be called

  return code is either GDI_SUCCESS or GDI_ERROR_TRUNCATE
 */
int GDA_LinearScanningFindAllLabels( GDI_VertexHolder vertex, GDI_Label* labels, size_t count, size_t* resultcount ) {
  size_t num_labels = 0;
  char* pos = vertex->property_data;
  GDA_PropertyHandle phandle = *(GDA_PropertyHandle*) pos;

  while( phandle != GDA_PROPERTY_LAST ) {
    if( phandle == GDA_PROPERTY_LABEL ) {
      if( num_labels == count ) {
        /**
          found more labels than can be returned
         */
        *resultcount = num_labels;
        return GDI_ERROR_TRUNCATE;
      }
      /**
        simplification of GDA_IntHandleToLabel
        since we don't need the additional input validation
       */
      GDA_Node** node = GDA_hashmap_get( vertex->transaction->db->labels->handle_to_address, pos+GDA_PROPERTY_METADATA_SIZE );
      assert( node != NULL );
      labels[num_labels++] = *(GDI_Label*)((*node)->value);
    }
    pos = GDA_NextPropertyRecord( pos );
    phandle = *(GDA_PropertyHandle*) pos;
  }

  *resultcount = num_labels;
  return GDI_SUCCESS;
}


/**
  should only be used for GDI AddPropertyToVertex
  returns three possible values:
  GDI_SUCCESS, GDI_ERROR_PROPERTY_EXISTS, GDI_ERROR_PROPERTY_TYPE_EXISTS

  assumes that ptype is not GDI_PROPERTY_TYPE_NULL, GDI_PROPERTY_TYPE_DEGREE, GDI_PROPERTY_TYPE_INDEGREE and GDI_PROPERTY_TYPE_OUTDEGREE

  updates the unused_space counter
 */
int GDA_LinearScanningAddProperty( GDI_PropertyType ptype, const void* value, size_t count, GDI_VertexHolder vertex ) {
  char* pos = vertex->property_data;
  GDA_PropertyHandle phandle = *(GDA_PropertyHandle*) pos;
  /**
    determine the buffer size for the property value
   */
  size_t dsize;
  GDI_GetSizeOfDatatype( &dsize, ptype->dtype );
  size_t property_entry_size = count * dsize;

  /**
    input checking
   */
  assert( ptype->int_handle < 256 );
  /**
    TODO: enable this again, once we move back to the default of 16 Bits for the record size field
    assert( property_entry_size < 65536 );
   */

  char* insert_pos = NULL;

  if( ptype->etype == GDI_SINGLE_ENTITY ) {
    if( ptype->stype == GDI_FIXED_SIZE ) {
      /**
        first check whether the property (type) is already present on the vertex
       */
      while( phandle != GDA_PROPERTY_LAST ) {
        if( phandle == ptype->int_handle ) {
          if( memcmp( value, (pos + GDA_PROPERTY_METADATA_SIZE), property_entry_size ) == 0 ) {
            return GDI_ERROR_PROPERTY_EXISTS;
          }
          return GDI_ERROR_PROPERTY_TYPE_EXISTS;
        } else {
          if( phandle == GDA_PROPERTY_EMPTY ) {
            GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
            if( (record_size == property_entry_size) || (record_size >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
              /**
                found a record entry that is empty and big enough
               */
              insert_pos = pos;
              /**
                check the remaining list to ensure that the property (type) is not already present on the vertex
               */
              pos = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) pos;
              while( phandle != GDA_PROPERTY_LAST ) {
                if( phandle == ptype->int_handle ) {
                  if( memcmp( value, (pos + GDA_PROPERTY_METADATA_SIZE), property_entry_size ) == 0 ) {
                    return GDI_ERROR_PROPERTY_EXISTS;
                  }
                  return GDI_ERROR_PROPERTY_TYPE_EXISTS;
                }
                pos = GDA_NextPropertyRecord( pos );
                phandle = *(GDA_PropertyHandle*) pos;
              }
              /**
                we are already finished with traversing the list,
                so we break from the outer loop
               */
              break;
            }
          }
        }
        /**
          advance to the next record in the list
         */
        pos = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) pos;
      }
    } else {
      /**
        single entity type which is a maximum sized type or has no size limit
       */
      /**
        first check whether the property (type) is already present on the vertex
       */
      while( phandle != GDA_PROPERTY_LAST ) {
        if( phandle == ptype->int_handle ) {
          GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
          if( record_size == property_entry_size ) {
            /**
              found an entry of type in question, which also has the right size
             */
            if( memcmp( value, (pos + GDA_PROPERTY_METADATA_SIZE), property_entry_size ) == 0 ) {
              return GDI_ERROR_PROPERTY_EXISTS;
            }
          }
          return GDI_ERROR_PROPERTY_TYPE_EXISTS;
        } else {
          if( phandle == GDA_PROPERTY_EMPTY ) {
            GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
            if( (record_size == property_entry_size) || (record_size >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
              /**
                found a record entry that is empty and big enough
               */
              insert_pos = pos;
              /**
                check the remaining list to ensure that the property (type) is not already present on the vertex
               */
              pos = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) pos;
              while( phandle != GDA_PROPERTY_LAST ) {
                if( phandle == ptype->int_handle ) {
                  GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
                  if( record_size == property_entry_size ) {
                    if( memcmp( value, (pos + GDA_PROPERTY_METADATA_SIZE), property_entry_size ) == 0 ) {
                      return GDI_ERROR_PROPERTY_EXISTS;
                    }
                    return GDI_ERROR_PROPERTY_TYPE_EXISTS;
                  }
                }
                pos = GDA_NextPropertyRecord( pos );
                phandle = *(GDA_PropertyHandle*) pos;
              }
              /**
                we are already finished with traversing the list,
                so we break from the outer loop
               */
              break;
            }
          }
        }
        /**
          advance to the next record in the list
         */
        pos = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) pos;
      }
    }
  } else {
    /**
      multi entity type, so we also have to check the whole list, unless we find the property in question
     */
    if( ptype->stype == GDI_FIXED_SIZE ) {
      /**
        first check whether the property (type) is already present on the vertex
       */
      while( phandle != GDA_PROPERTY_LAST ) {
        if( phandle == ptype->int_handle ) {
          if( memcmp( value, (pos + GDA_PROPERTY_METADATA_SIZE), property_entry_size ) == 0 ) {
            return GDI_ERROR_PROPERTY_EXISTS;
          }
        } else {
          if( phandle == GDA_PROPERTY_EMPTY ) {
            GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
            if( (record_size == property_entry_size) || (record_size >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
              /**
                found a record entry that is empty and big enough
               */
              insert_pos = pos;
              /**
                check the remaining list to ensure that the property (type) is not already present on the vertex
               */
              pos = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) pos;
              while( phandle != GDA_PROPERTY_LAST ) {
                if( phandle == ptype->int_handle ) {
                  if( memcmp( value, (pos + GDA_PROPERTY_METADATA_SIZE), property_entry_size ) == 0 ) {
                    return GDI_ERROR_PROPERTY_EXISTS;
                  }
                }
                pos = GDA_NextPropertyRecord( pos );
                phandle = *(GDA_PropertyHandle*) pos;
              }
              /**
                we are already finished with traversing the list,
                so we break from the outer loop
               */
              break;
            }
          }
        }
        /**
          advance to the next record in the list
         */
        pos = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) pos;
      }
    } else {
      /**
        multi entity type which is a maximum sized type or has no size limit
       */
      while( phandle != GDA_PROPERTY_LAST ) {
        if( phandle == ptype->int_handle ) {
          GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
          if( record_size == property_entry_size ) {
            /**
              found an entry of type in question, which also has the right size
             */
            if( memcmp( value, (pos + GDA_PROPERTY_METADATA_SIZE), property_entry_size ) == 0 ) {
              return GDI_ERROR_PROPERTY_EXISTS;
            }
          }
        } else {
          if( phandle == GDA_PROPERTY_EMPTY ) {
            GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
            if( (record_size == property_entry_size) || (record_size >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
              /**
                found a record entry that is empty and big enough
               */
              insert_pos = pos;
              /**
                check the remaining list to ensure that the property (type) is not already present on the vertex
               */
              pos = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) pos;
              while( phandle != GDA_PROPERTY_LAST ) {
                if( phandle == ptype->int_handle ) {
                  GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
                  if( record_size == property_entry_size ) {
                    if( memcmp( value, (pos + GDA_PROPERTY_METADATA_SIZE), property_entry_size ) == 0 ) {
                      return GDI_ERROR_PROPERTY_EXISTS;
                    }
                  }
                }
                pos = GDA_NextPropertyRecord( pos );
                phandle = *(GDA_PropertyHandle*) pos;
              }
              /**
                we are already finished with traversing the list,
                so we break from the outer loop
               */
              break;
            }
          }
        }
        /**
          advance to the next record in the list
         */
        pos = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) pos;
      }
    }
  }
  /**
    We didn't find a property of that type with the same value (or another property of that type,
    in case of a single entity type), so we are good to start the actual insertion operation.
   */
  if( insert_pos != NULL ) {
    /**
      already found an sufficient sized empty record entry during the find operation
     */
    GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE);
    if( record_size == property_entry_size ) {
      /**
        empty record fits perfectly, so we don't have to update the size part of the meta data
       */
      *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
      memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, value, property_entry_size );
    } else {
      /**
        we already know that the empty record is at least big enough to hold the new
        property record and an empty record, so no need to check again

        first insert the new property record
       */
      *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
      *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = property_entry_size;
      memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, value, property_entry_size );
      /**
        set a new empty record
       */
      insert_pos = insert_pos + GDA_PROPERTY_METADATA_SIZE + property_entry_size;
      *(GDA_PropertyHandle*) insert_pos = GDA_PROPERTY_EMPTY;
      *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = record_size - (property_entry_size + GDA_PROPERTY_METADATA_SIZE);
    }
  } else {
    assert( *(GDA_PropertyHandle*) pos == GDA_PROPERTY_LAST );
    /**
      have to insert a record at the end

      pos points to the last record (= special record at the end of the list)
     */
    size_t new_list_size = pos + GDA_PROPERTY_METADATA_SIZE - vertex->property_data /* absolute size of the used memory */
      + GDA_PROPERTY_METADATA_SIZE + property_entry_size /* size of the new record (including meta data) */;
    if( new_list_size > vertex->property_size ) {
      /**
        allocated memory is not sufficient
       */
      size_t temp_pos = pos - vertex->property_data;
      do {
        vertex->unused_space += vertex->property_size;
        vertex->property_size = vertex->property_size << 1;
      } while( new_list_size > vertex->property_size );
      vertex->property_data = realloc( vertex->property_data, vertex->property_size );
      pos = vertex->property_data + temp_pos;
    }
    *(GDA_PropertyHandle*) pos = ptype->int_handle;
    *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) = property_entry_size;
    memcpy( pos+GDA_PROPERTY_METADATA_SIZE, value, property_entry_size );
    /**
      set a new last record
     */
    pos = pos + GDA_PROPERTY_METADATA_SIZE + property_entry_size;
    *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
  }

  /**
    update the unused space counter
   */
  assert( vertex->unused_space >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) );
  vertex->unused_space -= property_entry_size + GDA_PROPERTY_METADATA_SIZE;

  return GDI_SUCCESS;
}


/**
  Counts how many different property types are used in (property type, value) pairs assigned to the vertex.

  Note that multiple occurrences of the same property type with different values are counted like
  a single occurrence, as we are counting only property types.
 */
void GDA_LinearScanningNumPropertyTypes( GDI_VertexHolder vertex, size_t* resultcount ) {

  uint32_t ptype_max = vertex->transaction->db->ptypes->ptype_max;

  bool ptypes[ptype_max];
  for( uint32_t i=0 ; i<ptype_max; i++ ) {
    ptypes[i] = false;
  }

  char* pos = vertex->property_data;
  GDA_PropertyHandle phandle = *(GDA_PropertyHandle*) pos;

  while( phandle != GDA_PROPERTY_LAST ) {
    assert( phandle < ptype_max );
    /**
      ignores GDA_PROPERTY_EMPTY, GDA_PROPERTY_LAST and GDA_PROPERTY_LABEL
      assumes that these values occupy the first three spots
     */
    if( phandle > GDA_PROPERTY_LABEL ) {
      ptypes[phandle] = true;
    }

    pos = GDA_NextPropertyRecord( pos );
    phandle = *(GDA_PropertyHandle*) pos;
  }

  uint32_t num_ptypes = 0;
  for( uint32_t i=3 /* start with GDI_PROPERTY_TYPE_ID */ ; i<ptype_max; i++ ) {
    if( ptypes[i] ) {
      num_ptypes++;
    }
  }

  *resultcount = num_ptypes;
}


/**
  assumes that ptype is not GDI_PROPERTY_TYPE_NULL, GDI_PROPERTY_TYPE_DEGREE, GDI_PROPERTY_TYPE_INDEGREE and GDI_PROPERTY_TYPE_OUTDEGREE

  assumes that count is greater than zero, since otherwise GDA_LinearScanningNumPropertyTypes should be called

  return code is either GDI_SUCCESS or GDI_ERROR_TRUNCATE
 */
int GDA_LinearScanningFindAllPropertyTypes( GDI_VertexHolder vertex, GDI_PropertyType* ptypes, size_t count, size_t* resultcount ) {
  GDI_Database graph_db = vertex->transaction->db;

  /**
    input checking
   */
  assert( count > 0 );

  /**
    first part uses the same algorithm as GDA_LinearScanningNumPropertyTypes
   */
  uint32_t ptype_max = graph_db->ptypes->ptype_max;

  bool present_ptypes[ptype_max];
  for( uint32_t i=0 ; i<ptype_max; i++ ) {
    present_ptypes[i] = false;
  }

  char* pos = vertex->property_data;
  GDA_PropertyHandle phandle = *(GDA_PropertyHandle*) pos;

  while( phandle != GDA_PROPERTY_LAST ) {
    assert( phandle < ptype_max );
    /**
      ignores GDA_PROPERTY_EMPTY, GDA_PROPERTY_LAST and GDA_PROPERTY_LABEL
      assumes that these values occupy the first three spots
     */
    if( phandle > GDA_PROPERTY_LABEL ) {
      present_ptypes[phandle] = true;
    }

    pos = GDA_NextPropertyRecord( pos );
    phandle = *(GDA_PropertyHandle*) pos;
  }

  /**
    second part: fill the array
   */
  uint32_t num_ptypes = 0;
  if( present_ptypes[3 /* = GDI_PROPERTY_TYPE_ID */] ) {
    ptypes[num_ptypes++] = GDI_PROPERTY_TYPE_ID;
  }
  for( uint32_t i=4 /* first user-defined property type */ ; i<ptype_max ; i++ ) {
    if( present_ptypes[i] ) {
      if( num_ptypes == count ) {
        /**
          found more property types than can be returned
         */
        *resultcount = num_ptypes;
        return GDI_ERROR_TRUNCATE;
      }
      /**
        simplification of GDA_IntHandleToPropertyType
        since we don't need the additional input validation
       */
      GDA_Node** node = GDA_hashmap_get( graph_db->ptypes->handle_to_address, &i );
      assert( node != NULL );
      ptypes[num_ptypes++] = *(GDI_PropertyType*)((*node)->value);
    }
  }

  *resultcount = num_ptypes;
  return GDI_SUCCESS;
}


/**
  assumes that ptype is not GDI_PROPERTY_TYPE_NULL, GDI_PROPERTY_TYPE_DEGREE, GDI_PROPERTY_TYPE_INDEGREE and GDI_PROPERTY_TYPE_OUTDEGREE
 */
void GDA_LinearScanningNumProperties( GDI_VertexHolder vertex, GDI_PropertyType ptype, size_t* resultcount, size_t* element_resultcount ) {
  /**
    input checking
   */
  assert( ptype->int_handle < 256 );

  uint8_t int_handle = ptype->int_handle;

  size_t dsize; /* size of one element in Bytes */
  GDI_GetSizeOfDatatype( &dsize, ptype->dtype );

  size_t num_properties = 0;
  size_t num_bytes = 0;

  char* pos = vertex->property_data;
  GDA_PropertyHandle phandle = *(GDA_PropertyHandle*) pos;

  if( ptype->etype == GDI_SINGLE_ENTITY ) {
    while( phandle != GDA_PROPERTY_LAST ) {
      if( phandle == int_handle ) {
        *resultcount = 1;
        *element_resultcount = (*(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE)) /* size in Bytes */ / dsize; /* determine number of elements */
        return;
      }
      pos = GDA_NextPropertyRecord( pos );
      phandle = *(GDA_PropertyHandle*) pos;
    }
  } else {
    /**
      multi entity type, so we also have to check the whole list
     */
    while( phandle != GDA_PROPERTY_LAST ) {
      if( phandle == int_handle ) {
        num_properties++;
        num_bytes += (*(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE)) /* size in Bytes */;
      }
      pos = GDA_NextPropertyRecord( pos );
      phandle = *(GDA_PropertyHandle*) pos;
    }
  }

  *resultcount = num_properties;
  *element_resultcount = num_bytes / dsize; /* determine number of elements */
}


/**
  assumes that ptype is not GDI_PROPERTY_TYPE_NULL, GDI_PROPERTY_TYPE_DEGREE, GDI_PROPERTY_TYPE_INDEGREE and GDI_PROPERTY_TYPE_OUTDEGREE

  assumes that count is greater than zero, since otherwise GDA_LinearScanningNumPropertyTypes should be called

  return code is either GDI_SUCCESS or GDI_ERROR_TRUNCATE
 */
int GDA_LinearScanningFindAllProperties( void* buf, size_t buf_count, size_t* buf_resultcount, size_t* array_of_offsets,
                                         size_t offset_count, size_t* offset_resultcount, GDI_PropertyType ptype,
                                         GDI_VertexHolder vertex ) {
  /**
    assertion which checks if the property handle is in valid range
   */
  assert( ( ptype->int_handle < 256 ) );
  uint8_t int_handle = ptype->int_handle;

  size_t dsize; /* size of one element in Bytes */
  GDI_GetSizeOfDatatype( &dsize, ptype->dtype );

  bool buf_overflow = false;
  bool offset_overflow = false;

  /**
    The numbers of elements written - initially 0
   */
  *buf_resultcount = 0;
  *offset_resultcount = 0;

  size_t val_size;

  char* pos = vertex->property_data;
  GDA_PropertyHandle phandle = *(GDA_PropertyHandle*)pos;

  if( ptype->etype == GDI_SINGLE_ENTITY ) {
    while( phandle != GDA_PROPERTY_LAST ) {
      if( phandle == int_handle ) {
        val_size = ( *(GDA_PropertyRecordSize*)( pos + GDA_PROPERTY_HANDLE_SIZE ) ) / dsize;

        if( val_size <= buf_count ) {
          memcpy( buf, pos + GDA_PROPERTY_METADATA_SIZE, val_size * dsize );
          *buf_resultcount = val_size;
        } else {
          buf_overflow = true;
        }

        array_of_offsets[0] = 0;
        ++*offset_resultcount;
        if( offset_count >= 2 ) {
          array_of_offsets[1] = val_size;
          ++*offset_resultcount;
        } else {
          offset_overflow = true;
        }
        break;
      }

      pos = GDA_NextPropertyRecord( pos );
      phandle = *(GDA_PropertyHandle*)pos;
    }
  } else {
    /**
      multi entity type, so we also have to check the whole list
     */
    while( phandle != GDA_PROPERTY_LAST ) {
      if( phandle == int_handle ) {
        val_size = ( *(GDA_PropertyRecordSize*)( pos + GDA_PROPERTY_HANDLE_SIZE ) ) /* size in Bytes */
                   / dsize;

        if( !buf_overflow ) {
          if( *buf_resultcount + val_size <= buf_count ) {
            memcpy( (char*)buf + ( *buf_resultcount ) * dsize, pos + GDA_PROPERTY_METADATA_SIZE, val_size * dsize );
            *buf_resultcount += val_size;
          } else {
            buf_overflow = true;
          }
        }
        if( !offset_overflow ) {
          /**
            Write the first offset (only once per function call).
            It is always safe, as offset_count must be >= 1 (otherwise size trick would be used and this function
            wouldn't be called at all).
           */
          if( *offset_resultcount == 0 ) {
            array_of_offsets[0] = 0;
            ++*offset_resultcount;
          }
          if( *offset_resultcount + 1 <= offset_count ) {
            array_of_offsets[*offset_resultcount] = array_of_offsets[*offset_resultcount - 1] + val_size;
            ++*offset_resultcount;
          } else {
            offset_overflow = true;
          }
        }
        /**
          If there is an overflow on both buffers we can stop immediately
         */
        if( buf_overflow && offset_overflow ) {
          break;
        }
      }

      pos = GDA_NextPropertyRecord( pos );
      phandle = *(GDA_PropertyHandle*)pos;
    }
  }

  if( buf_overflow || offset_overflow ) {
    return GDI_ERROR_TRUNCATE;
  }
  return GDI_SUCCESS;
}


/**
  assumes that ptype is not GDI_PROPERTY_TYPE_NULL, GDI_PROPERTY_TYPE_DEGREE, GDI_PROPERTY_TYPE_INDEGREE and GDI_PROPERTY_TYPE_OUTDEGREE

  found_flag is returned, in case the caller wants to keep skip updating indexes and indicates
  whether the property was present on the vertex:
  true  = all properties of that type were removed, and there was at least one present
  false = no property of that type was found

  TODO: not sure that we can do this without retrieving the values first, so that we can update the indexes later

  updates the unused_space counter
 */
void GDA_LinearScanningRemoveProperties( GDI_PropertyType ptype, GDI_VertexHolder vertex, bool* found_flag ) {
  /**
    input checking
   */
  assert( ptype->int_handle < 256 );

  char* pos = vertex->property_data;
  GDA_PropertyHandle phandle = *(GDA_PropertyHandle*) pos;

  char* previous_record = NULL;

  if( ptype->etype == GDI_SINGLE_ENTITY ) {
    while( phandle != GDA_PROPERTY_LAST ) {
      if( phandle == ptype->int_handle ) {
        /**
          found a property of the type that we are looking for
         */
        *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
        /**
          introduced a new empty record, so update the appropriate counter
         */
        vertex->unused_space += *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;

        if( previous_record != NULL ) {
          /**
            check whether the previous record is also empty, and if so, merge them
           */
          phandle = *(GDA_PropertyHandle*) previous_record;
          if( phandle == GDA_PROPERTY_EMPTY ) {
            *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
            pos = previous_record;
          }
        }
        /**
          check whether the next record is also empty, and if so, merge them
         */
        char* next_record = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) next_record;
        if( phandle == GDA_PROPERTY_EMPTY ) {
          *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
        } else {
          if( phandle == GDA_PROPERTY_LAST ) {
            *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
          }
        }
        *found_flag = true;
        return;
      }
      /**
        advance to the next record in the list
       */
      previous_record = pos;
      pos = GDA_NextPropertyRecord( pos );
      phandle = *(GDA_PropertyHandle*) pos;
    }

    /**
      reached the end of the list, so the label was not found
     */
    *found_flag = false;
  } else {
    /**
      multi entity type, so we also have to check the whole list
     */
    *found_flag = false;

    while( phandle != GDA_PROPERTY_LAST ) {
      if( phandle == ptype->int_handle ) {
        /**
          found a property of the type that we are looking for
         */
        *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
        /**
          introduced a new empty record, so update the appropriate counter
         */
        vertex->unused_space += *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
        *found_flag = true;

        if( previous_record != NULL ) {
          /**
            check whether the previous record is also empty, and if so, merge them
           */
          phandle = *(GDA_PropertyHandle*) previous_record;
          if( phandle == GDA_PROPERTY_EMPTY ) {
            *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
            pos = previous_record;
          }
        }
        /**
          check whether the next record is also empty, and if so, merge them
         */
        char* next_record = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) next_record;
        if( phandle == GDA_PROPERTY_EMPTY ) {
          *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
        } else {
          if( phandle == GDA_PROPERTY_LAST ) {
            *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
            /**
              already reached the last record, so return immediately, because
              GDA_NextPropertyRecord doesn't guard against pointing to the last record
             */
            return;
          }
        }
      }
      /**
        advance to the next record in the list
       */
      previous_record = pos;
      pos = GDA_NextPropertyRecord( pos );
      phandle = *(GDA_PropertyHandle*) pos;
    }
  }
}


/**
  assumes that ptype is not GDI_PROPERTY_TYPE_NULL, GDI_PROPERTY_TYPE_DEGREE, GDI_PROPERTY_TYPE_INDEGREE and GDI_PROPERTY_TYPE_OUTDEGREE

  found_flag is returned, in case the caller wants to keep skip updating indexes and indicates
  whether the property was present on the vertex:
  true  = property was removed
  false = property wasn't found

  TODO: not sure that we can do this without retrieving the original value first, so that we can update the indexes later

  updates the unused_space counter
 */
void GDA_LinearScanningRemoveSpecificProperty( GDI_PropertyType ptype, const void* value, size_t count, GDI_VertexHolder vertex, bool* found_flag ) {
  char* pos = vertex->property_data;
  GDA_PropertyHandle phandle = *(GDA_PropertyHandle*) pos;

  /**
    determine the buffer size for the property value
   */
  size_t dsize;
  GDI_GetSizeOfDatatype( &dsize, ptype->dtype );
  size_t property_entry_size = count * dsize;

  /**
    input checking
   */
  assert( ptype->int_handle < 256 );
  /**
    TODO: enable this again, once we move back to the default of 16 Bits for the record size field
    assert( property_entry_size < 65536 );
   */

  char* previous_record = NULL;

  if( ptype->etype == GDI_SINGLE_ENTITY ) {
    if( ptype->stype == GDI_FIXED_SIZE ) {
      while( phandle != GDA_PROPERTY_LAST ) {
        if( phandle == ptype->int_handle ) {
          if( memcmp( value, (pos + GDA_PROPERTY_METADATA_SIZE), property_entry_size ) == 0 ) {
            /**
              found the property
             */
            *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
            /**
              introduced a new empty record, so update the appropriate counter
             */
            vertex->unused_space += property_entry_size + GDA_PROPERTY_METADATA_SIZE;

            if( previous_record != NULL ) {
              /**
                check whether the previous record is also empty, and if so, merge them
               */
              phandle = *(GDA_PropertyHandle*) previous_record;
              if( phandle == GDA_PROPERTY_EMPTY ) {
                *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += property_entry_size + GDA_PROPERTY_METADATA_SIZE;
                pos = previous_record;
              }
            }
            /**
              check whether the next record is also empty, and if so, merge them
             */
            char* next_record = GDA_NextPropertyRecord( pos );
            phandle = *(GDA_PropertyHandle*) next_record;
            if( phandle == GDA_PROPERTY_EMPTY ) {
              *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
            } else {
              if( phandle == GDA_PROPERTY_LAST ) {
                *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
              }
            }
            *found_flag = true;
            return;
          }
          *found_flag = false;
          return;
        }
        /**
          advance to the next record in the list
         */
        previous_record = pos;
        pos = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) pos;
      }
    } else {
      /**
        single entity type which is a maximum sized type or has no size limit
       */
      while( phandle != GDA_PROPERTY_LAST ) {
        if( phandle == ptype->int_handle ) {
          GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
          if( record_size == property_entry_size ) {
            /**
              found a property of the type in question, which also has the right size
             */
            if( memcmp( value, (pos + GDA_PROPERTY_METADATA_SIZE), property_entry_size ) == 0 ) {
              /**
                found the property
               */
              *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
              /**
                introduced a new empty record, so update the appropriate counter
               */
              vertex->unused_space += property_entry_size + GDA_PROPERTY_METADATA_SIZE;

              if( previous_record != NULL ) {
                /**
                  check whether the previous record is also empty, and if so, merge them
                 */
                phandle = *(GDA_PropertyHandle*) previous_record;
                if( phandle == GDA_PROPERTY_EMPTY ) {
                  *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += property_entry_size + GDA_PROPERTY_METADATA_SIZE;
                  pos = previous_record;
                }
              }
              /**
                check whether the next record is also empty, and if so, merge them
               */
              char* next_record = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) next_record;
              if( phandle == GDA_PROPERTY_EMPTY ) {
                *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
              } else {
                if( phandle == GDA_PROPERTY_LAST ) {
                  *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
                }
              }
              *found_flag = true;
              return;
            }
          }
          *found_flag = false;
          return;
        }
        /**
          advance to the next record in the list
         */
        previous_record = pos;
        pos = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) pos;
      }
    }
  } else {
    /**
      multi entity type, so we also have to check the whole list, unless we find the property in question
     */
    if( ptype->stype == GDI_FIXED_SIZE ) {
      while( phandle != GDA_PROPERTY_LAST ) {
        if( phandle == ptype->int_handle ) {
          if( memcmp( value, (pos + GDA_PROPERTY_METADATA_SIZE), property_entry_size ) == 0 ) {
            /**
              found the property
             */
            *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
            /**
              introduced a new empty record, so update the appropriate counter
             */
            vertex->unused_space += property_entry_size + GDA_PROPERTY_METADATA_SIZE;

            if( previous_record != NULL ) {
              /**
                check whether the previous record is also empty, and if so, merge them
               */
              phandle = *(GDA_PropertyHandle*) previous_record;
              if( phandle == GDA_PROPERTY_EMPTY ) {
                *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += property_entry_size + GDA_PROPERTY_METADATA_SIZE;
                pos = previous_record;
              }
            }
            /**
              check whether the next record is also empty, and if so, merge them
             */
            char* next_record = GDA_NextPropertyRecord( pos );
            phandle = *(GDA_PropertyHandle*) next_record;
            if( phandle == GDA_PROPERTY_EMPTY ) {
              *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
            } else {
              if( phandle == GDA_PROPERTY_LAST ) {
                *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
              }
            }
            *found_flag = true;
            return;
          }
        }
        /**
          advance to the next record in the list
         */
        previous_record = pos;
        pos = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) pos;
      }
    } else {
      /**
        multi entity type which is a maximum sized type or has no size limit
       */
      while( phandle != GDA_PROPERTY_LAST ) {
        if( phandle == ptype->int_handle ) {
          GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
          if( record_size == property_entry_size ) {
            /**
              found a property of the type in question, which also has the right size
             */
            if( memcmp( value, (pos + GDA_PROPERTY_METADATA_SIZE), property_entry_size ) == 0 ) {
              /**
                found the property
               */
              *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
              /**
                introduced a new empty record, so update the appropriate counter
               */
              vertex->unused_space += property_entry_size + GDA_PROPERTY_METADATA_SIZE;

              if( previous_record != NULL ) {
                /**
                  check whether the previous record is also empty, and if so, merge them
                 */
                phandle = *(GDA_PropertyHandle*) previous_record;
                if( phandle == GDA_PROPERTY_EMPTY ) {
                  *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += property_entry_size + GDA_PROPERTY_METADATA_SIZE;
                  pos = previous_record;
                }
              }
              /**
                check whether the next record is also empty, and if so, merge them
               */
              char* next_record = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) next_record;
              if( phandle == GDA_PROPERTY_EMPTY ) {
                *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
              } else {
                if( phandle == GDA_PROPERTY_LAST ) {
                  *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
                }
              }
              *found_flag = true;
              return;
            }
          }
        }
        /**
          advance to the next record in the list
         */
        previous_record = pos;
        pos = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) pos;
      }
    }
  }

  *found_flag = false;
}


/**
  assumes that ptype is not GDI_PROPERTY_TYPE_NULL, GDI_PROPERTY_TYPE_DEGREE, GDI_PROPERTY_TYPE_INDEGREE and GDI_PROPERTY_TYPE_OUTDEGREE

  assumes that ptype is a single entity type

  returns either GDI_SUCCESS or GDI_ERROR_NO_PROPERTY

  GDA_LinearScanningSetSingleEntityProperty uses exactly the same implementation (minus a check), so any updates
  to this function should be made there as well

  TODO: not sure that we can do this without retrieving the original value first, so that we can update the indexes later

  updates the unused_space counter
 */
int GDA_LinearScanningUpdateSingleEntityProperty( GDI_PropertyType ptype, const void* value, size_t count, GDI_VertexHolder vertex ) {
  char* pos = vertex->property_data;
  GDA_PropertyHandle phandle = *(GDA_PropertyHandle*) pos;

  /**
    determine the buffer size for the property value
   */
  size_t dsize;
  GDI_GetSizeOfDatatype( &dsize, ptype->dtype );
  size_t property_entry_size = count * dsize;

  bool found_flag = false;

  /**
    input checking
   */
  assert( ptype->int_handle < 256 );
  /**
    TODO: enable this again, once we move back to the default of 16 Bits for the record size field
    assert( property_entry_size < 65536 );
   */

  char* previous_record = NULL;
  char* insert_pos = NULL; /* remove compiler warning */

  if( ptype->stype == GDI_FIXED_SIZE ) {
    /**
      look for the property to be updated (and also a place for insertion)
     */
    while( phandle != GDA_PROPERTY_LAST ) {
      if( phandle == ptype->int_handle ) {
        /**
          found the property of the type that we are looking for

          first we remove that property and do merging, if necessary
         */
        found_flag = true;
        *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
        /**
          introduced a new empty record, so update the appropriate counter
         */
        vertex->unused_space += property_entry_size + GDA_PROPERTY_METADATA_SIZE;

        if( previous_record != NULL ) {
          /**
            check whether the previous record is also empty, and if so, merge them
           */
          phandle = *(GDA_PropertyHandle*) previous_record;
          if( phandle == GDA_PROPERTY_EMPTY ) {
            *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += property_entry_size + GDA_PROPERTY_METADATA_SIZE;
            pos = previous_record;
          }
        }

        /**
          check whether the next record is also empty, and if so, merge them
         */
        char* next_record = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) next_record;
        if( phandle == GDA_PROPERTY_EMPTY ) {
          *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
        } else {
          if( phandle == GDA_PROPERTY_LAST ) {
            *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
           }
         }

        /**
          we only reach this code path, if we haven't found an insertion position
          since it is a fixed sized property type, the record is definitely big enough
         */
        insert_pos = pos;

        /**
          we have everything we need, so we can stop traversing the list
         */
        break;
      } else {
        /**
          property is not of the type, that we are looking for
         */
        if( phandle == GDA_PROPERTY_EMPTY ) {
          GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
          if( (record_size == property_entry_size) || (record_size >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
            /**
              found a record entry that is empty and big enough
             */
            insert_pos = pos;

            /**
              check the remaining list to remove the existing property
             */
            previous_record = pos;
            pos = GDA_NextPropertyRecord( pos );
            phandle = *(GDA_PropertyHandle*) pos;
            while( phandle != GDA_PROPERTY_LAST ) {
              if( phandle == ptype->int_handle ) {
                /**
                  found the property of the type that we are looking for

                  we remove that property and do merging, if necessary
                 */
                found_flag = true;
                *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
                /**
                  introduced a new empty record, so update the appropriate counter
                 */
                vertex->unused_space += property_entry_size + GDA_PROPERTY_METADATA_SIZE;

                /**
                  check whether the previous record is also empty, and if so, merge them

                  guaranteed to have a previous record
                 */
                phandle = *(GDA_PropertyHandle*) previous_record;
                if( phandle == GDA_PROPERTY_EMPTY ) {
                  *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += property_entry_size + GDA_PROPERTY_METADATA_SIZE;
                  pos = previous_record;
                }

                /**
                  check whether the next record is also empty, and if so, merge them
                 */
                char* next_record = GDA_NextPropertyRecord( pos );
                phandle = *(GDA_PropertyHandle*) next_record;
                if( phandle == GDA_PROPERTY_EMPTY ) {
                  *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
                } else {
                  if( phandle == GDA_PROPERTY_LAST ) {
                    *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
                  }
                }

                /**
                  we have everything we need, so we can stop traversing the list
                 */
                break;
              }
              /**
                advance to the next record in the list
               */
              previous_record = pos;
              pos = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) pos;
            }
            /**
              we are already finished with traversing the list,
              so we break from the outer loop
             */
            break;
          }
        }
      }
      /**
        advance to the next record in the list
       */
      previous_record = pos;
      pos = GDA_NextPropertyRecord( pos );
      phandle = *(GDA_PropertyHandle*) pos;
    }

    if( !found_flag ) {
      return GDI_ERROR_NO_PROPERTY;
    }

    assert( insert_pos != NULL );
    /**
      insertion of the new property
     */
    if( *(GDA_PropertyHandle*) insert_pos == GDA_PROPERTY_LAST ) {
      /**
        can always be sure, that we have enough space, because we just removed a property of the same size
       */
      *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
      *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = property_entry_size;
      memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, value, property_entry_size );
      /**
        set a new last record
       */
      insert_pos = insert_pos + GDA_PROPERTY_METADATA_SIZE + property_entry_size;
      *(GDA_PropertyHandle*) insert_pos = GDA_PROPERTY_LAST;
    } else {
      assert( *(GDA_PropertyHandle*) insert_pos == GDA_PROPERTY_EMPTY );
      GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE);
      if( record_size == property_entry_size ) {
        /**
          empty record fits perfectly, so we don't have to update the size part of the meta data
         */
        *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
        memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, value, property_entry_size );
      } else {
        /**
          we already know that the empty record is at least big enough to hold the new
          property record and an empty record, so no need to check again

          first insert the new property record
         */
        *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
        *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = property_entry_size;
        memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, value, property_entry_size );
        /**
          set a new empty record
         */
        insert_pos = insert_pos + GDA_PROPERTY_METADATA_SIZE + property_entry_size;
        *(GDA_PropertyHandle*) insert_pos = GDA_PROPERTY_EMPTY;
        *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = record_size - (property_entry_size + GDA_PROPERTY_METADATA_SIZE);
      }
    }

    /**
      update the unused space counter
     */
    assert( vertex->unused_space >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) );
    vertex->unused_space -= property_entry_size + GDA_PROPERTY_METADATA_SIZE;
  } else {
    /**
      property type which is a maximum sized type or has no size limit

      first look for the property to be updated (and also a place for insertion)
     */
    while( phandle != GDA_PROPERTY_LAST ) {
      if( phandle == ptype->int_handle ) {
        /**
          found the property of the type that we are looking for

          first we remove that property and do merging, if necessary
         */
        found_flag = true;
        *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
        /**
          introduced a new empty record, so update the appropriate counter
         */
        vertex->unused_space += *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;

        if( previous_record != NULL ) {
          /**
            check whether the previous record is also empty, and if so, merge them
           */
          phandle = *(GDA_PropertyHandle*) previous_record;
          if( phandle == GDA_PROPERTY_EMPTY ) {
            *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
            pos = previous_record;
          }
        }

        /**
          check whether the next record is also empty, and if so, merge them
         */
        char* next_record = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) next_record;
        if( phandle == GDA_PROPERTY_EMPTY ) {
          *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
        } else {
          if( phandle == GDA_PROPERTY_LAST ) {
            *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
            break;
          }
        }

        /**
          only reach this code path, if we haven't found an insertion position
         */
        GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
        if( (record_size == property_entry_size) || (record_size >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
          /**
            the just created empty record is big enough
           */
          insert_pos = pos;
        } else {
          /**
            check the remaining list for an empty record that is big enough to hold the new value
           */
          pos = GDA_NextPropertyRecord( pos );
          phandle = *(GDA_PropertyHandle*) pos;
          // TODO: jump ahead two records
          while( phandle != GDA_PROPERTY_LAST ) {
            if( phandle == GDA_PROPERTY_EMPTY ) {
              GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
              if( (record_size == property_entry_size) || (record_size >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
                /**
                  found a record entry that is empty and big enough
                 */
                insert_pos = pos;
                /**
                  we have everything we need, so we can stop traversing the list
                  and break from the inner loop
                 */
                break;
              }
            }
            /**
              advance to the next record
             */
            pos = GDA_NextPropertyRecord( pos );
            phandle = *(GDA_PropertyHandle*) pos;
          }
        }
        /**
          we have everything we need, so we can stop traversing the list
         */
        break;
      } else {
        /**
          property is not of the type, that we are looking for
         */
        if( phandle == GDA_PROPERTY_EMPTY ) {
          GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
          if( (record_size == property_entry_size) || (record_size >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
            /**
              found a record entry that is empty and big enough
             */
            insert_pos = pos;

            /**
              check the remaining list to remove the existing property
             */
            previous_record = pos;
            pos = GDA_NextPropertyRecord( pos );
            phandle = *(GDA_PropertyHandle*) pos;
            while( phandle != GDA_PROPERTY_LAST ) {
              if( phandle == ptype->int_handle ) {
                /**
                  found the property of the type that we are looking for

                  we remove that property and do merging, if necessary
                 */
                found_flag = true;
                *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
                /**
                  introduced a new empty record, so update the appropriate counter
                 */
                vertex->unused_space += *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;

                /**
                  check whether the previous record is also empty, and if so, merge them

                  guaranteed to have a previous record
                 */
                phandle = *(GDA_PropertyHandle*) previous_record;
                if( phandle == GDA_PROPERTY_EMPTY ) {
                  *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
                  pos = previous_record;
                }

                /**
                  check whether the next record is also empty, and if so, merge them
                 */
                char* next_record = GDA_NextPropertyRecord( pos );
                phandle = *(GDA_PropertyHandle*) next_record;
                if( phandle == GDA_PROPERTY_EMPTY ) {
                  *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
                } else {
                  if( phandle == GDA_PROPERTY_LAST ) {
                    *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
                  }
                }

                /**
                  we have everything we need, so we can stop traversing the list
                 */
                break;
              }
              /**
                advance to the next record in the list
               */
              previous_record = pos;
              pos = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) pos;
            }
            /**
              we are already finished with traversing the list,
              so we break from the outer loop
             */
            break;
          }
        }
      }
      /**
        advance to the next record in the list
       */
      previous_record = pos;
      pos = GDA_NextPropertyRecord( pos );
      phandle = *(GDA_PropertyHandle*) pos;
    }

    if( !found_flag ) {
      return GDI_ERROR_NO_PROPERTY;
    }

    /**
      insertion of the new property
     */
    if( insert_pos != NULL ) {
      /**
        already found an sufficient sized empty record entry
       */
      GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE);
      if( record_size == property_entry_size ) {
        /**
          empty record fits perfectly, so we don't have to update the size part of the meta data
         */
        *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
        memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, value, property_entry_size );
      } else {
        /**
          we already know that the empty record is at least big enough to hold the new
          property record and an empty record, so no need to check again

          first insert the new property record
         */
        *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
        *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = property_entry_size;
        memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, value, property_entry_size );
        /**
          set a new empty record
         */
        insert_pos = insert_pos + GDA_PROPERTY_METADATA_SIZE + property_entry_size;
        *(GDA_PropertyHandle*) insert_pos = GDA_PROPERTY_EMPTY;
        *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = record_size - (property_entry_size + GDA_PROPERTY_METADATA_SIZE);
      }
    } else {
      assert( *(GDA_PropertyHandle*) pos == GDA_PROPERTY_LAST );
      /**
        have to insert a record at the end

        pos points to the last record (= special record at the end of the list)
       */
      size_t new_list_size = pos + GDA_PROPERTY_METADATA_SIZE - vertex->property_data /* absolute size of the used memory */
        + GDA_PROPERTY_METADATA_SIZE + property_entry_size /* size of the new record (including meta data) */;
      if( new_list_size > vertex->property_size ) {
        /**
          allocated memory is not sufficient
         */
        size_t temp_pos = pos - vertex->property_data;
        do {
          vertex->unused_space += vertex->property_size;
          vertex->property_size = vertex->property_size << 1;
        } while( new_list_size > vertex->property_size );
        vertex->property_data = realloc( vertex->property_data, vertex->property_size );
        pos = vertex->property_data + temp_pos;
      }
      *(GDA_PropertyHandle*) pos = ptype->int_handle;
      *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) = property_entry_size;
      memcpy( pos+GDA_PROPERTY_METADATA_SIZE, value, property_entry_size );
      /**
        set a new last record
       */
      pos = pos + GDA_PROPERTY_METADATA_SIZE + property_entry_size;
      *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
    }

    /**
      update the unused space counter
     */
    assert( vertex->unused_space >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) );
    vertex->unused_space -= property_entry_size + GDA_PROPERTY_METADATA_SIZE;
  }
  return GDI_SUCCESS;
}


/**
  assumes that ptype is not GDI_PROPERTY_TYPE_NULL, GDI_PROPERTY_TYPE_DEGREE, GDI_PROPERTY_TYPE_INDEGREE and GDI_PROPERTY_TYPE_OUTDEGREE

  returns three possible values: GDI_SUCCESS, GDI_ERROR_PROPERTY_EXISTS, GDI_ERROR_NO_PROPERTY

  updates the unused_space counter
 */
int GDA_LinearScanningUpdateSpecificProperty( GDI_PropertyType ptype, const void* old_value, size_t old_count, const void* new_value, size_t new_count, GDI_VertexHolder vertex ) {
  char* pos = vertex->property_data;
  GDA_PropertyHandle phandle = *(GDA_PropertyHandle*) pos;

  /**
    determine the buffer size for both values
   */
  size_t dsize;
  GDI_GetSizeOfDatatype( &dsize, ptype->dtype );
  size_t old_property_entry_size = old_count * dsize;
  size_t new_property_entry_size = new_count * dsize;

  /**
    input checking
   */
  assert( ptype->int_handle < 256 );
  /**
    TODO: enable this again, once we move back to the default of 16 Bits for the record size field
    assert( old_property_entry_size < 65536 );
    assert( new_property_entry_size < 65536 );
   */

  char* previous_record = NULL;
  char* insert_pos = NULL;

  if( ptype->etype == GDI_SINGLE_ENTITY ) {
    bool removed_property = false;
    if( ptype->stype == GDI_FIXED_SIZE ) {
      /**
        look for the property to be updated (and also a place for insertion)
       */
      while( phandle != GDA_PROPERTY_LAST ) {
        if( phandle == ptype->int_handle ) {
          /**
            found a property of the type we are looking for
           */
          if( memcmp( old_value, (pos + GDA_PROPERTY_METADATA_SIZE), old_property_entry_size ) == 0 ) {
            /**
              found the property that we were looking for

              first we remove that property and do merging, if necessary
             */
            *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
            removed_property = true;
            /**
              introduced a new empty record, so update the appropriate counter
             */
            vertex->unused_space += old_property_entry_size + GDA_PROPERTY_METADATA_SIZE;

            if( previous_record != NULL ) {
              /**
                check whether the previous record is also empty, and if so, merge them
               */
              phandle = *(GDA_PropertyHandle*) previous_record;
              if( phandle == GDA_PROPERTY_EMPTY ) {
                *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += old_property_entry_size + GDA_PROPERTY_METADATA_SIZE;
                pos = previous_record;
              }
            }

            /**
              check whether the next record is also empty, and if so, merge them
             */
            char* next_record = GDA_NextPropertyRecord( pos );
            phandle = *(GDA_PropertyHandle*) next_record;
            if( phandle == GDA_PROPERTY_EMPTY ) {
              *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
            } else {
              if( phandle == GDA_PROPERTY_LAST ) {
                *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
              }
            }

            /**
              we only reach this code path, if we haven't found an insertion position
              since it is a fixed sized property type, the record is definitely big enough
             */
            insert_pos = pos;

            /**
              we have everything we need, so we can stop traversing the list
             */
            break;
          } else {
            return GDI_ERROR_NO_PROPERTY;
          }
        } else {
          /**
            property is not of the type, that we are looking for
           */
          if( phandle == GDA_PROPERTY_EMPTY ) {
            GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
            if( (record_size == new_property_entry_size) || (record_size >= (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
              /**
                found a record entry that is empty and big enough
               */
              insert_pos = pos;

              /**
                check the remaining list to remove the existing property
               */
              previous_record = pos;
              pos = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) pos;
              while( phandle != GDA_PROPERTY_LAST ) {
                if( phandle == ptype->int_handle ) {
                  /**
                    found a property of the type that we are look for
                   */
                  if( memcmp( old_value, (pos + GDA_PROPERTY_METADATA_SIZE), old_property_entry_size ) == 0 ) {
                    /**
                      found the property that we were looking for

                      we remove that property and do merging, if necessary
                     */
                    *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
                    removed_property = true;
                    /**
                      introduced a new empty record, so update the appropriate counter
                     */
                    vertex->unused_space += old_property_entry_size + GDA_PROPERTY_METADATA_SIZE;

                    /**
                      check whether the previous record is also empty, and if so, merge them

                      guaranteed to have a previous record
                     */
                    phandle = *(GDA_PropertyHandle*) previous_record;
                    if( phandle == GDA_PROPERTY_EMPTY ) {
                      *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += old_property_entry_size + GDA_PROPERTY_METADATA_SIZE;
                      pos = previous_record;
                    }

                    /**
                      check whether the next record is also empty, and if so, merge them
                     */
                    char* next_record = GDA_NextPropertyRecord( pos );
                    phandle = *(GDA_PropertyHandle*) next_record;
                    if( phandle == GDA_PROPERTY_EMPTY ) {
                      *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
                    } else {
                      if( phandle == GDA_PROPERTY_LAST ) {
                        *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
                      }
                    }

                    /**
                      we have everything we need, so we can stop traversing the list
                     */
                    break;
                  } else {
                    return GDI_ERROR_NO_PROPERTY;
                  }
                }
                /**
                  advance to the next record in the list
                 */
                previous_record = pos;
                pos = GDA_NextPropertyRecord( pos );
                phandle = *(GDA_PropertyHandle*) pos;
              }
              /**
                we are already finished with traversing the list,
                so we break from the outer loop
               */
              break;
            }
          }
        }
        /**
          advance to the next record in the list
         */
        previous_record = pos;
        pos = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) pos;
      }

      if( !removed_property ) {
        return GDI_ERROR_NO_PROPERTY;
      }

      assert( insert_pos != NULL );
      /**
        insertion of the new property
       */
      if( *(GDA_PropertyHandle*) insert_pos == GDA_PROPERTY_LAST ) {
        /**
          can always be sure, that we have enough space, because we just removed a property of the same size
         */
        *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
        *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = new_property_entry_size;
        memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, new_value, new_property_entry_size );
        /**
          set a new last record
         */
        insert_pos = insert_pos + GDA_PROPERTY_METADATA_SIZE + new_property_entry_size;
        *(GDA_PropertyHandle*) insert_pos = GDA_PROPERTY_LAST;
      } else {
        assert( *(GDA_PropertyHandle*) insert_pos == GDA_PROPERTY_EMPTY );
        GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE);
        if( record_size == new_property_entry_size ) {
          /**
            empty record fits perfectly, so we don't have to update the size part of the meta data
           */
          *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
          memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, new_value, new_property_entry_size );
        } else {
          /**
            we already know that the empty record is at least big enough to hold the new
            property record and an empty record, so no need to check again

            first insert the new property record
           */
          *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
          *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = new_property_entry_size;
          memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, new_value, new_property_entry_size );
          /**
            set a new empty record
           */
          insert_pos = insert_pos + GDA_PROPERTY_METADATA_SIZE + new_property_entry_size;
          *(GDA_PropertyHandle*) insert_pos = GDA_PROPERTY_EMPTY;
          *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = record_size - (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE);
        }
      }

      /**
        update the unused space counter
       */
      assert( vertex->unused_space >= (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE) );
      vertex->unused_space -= new_property_entry_size + GDA_PROPERTY_METADATA_SIZE;
    } else {
      /**
        single entity property type which is a maximum sized type or has no size limit

        first look for the property to be updated (and also a place for insertion)
       */
      while( phandle != GDA_PROPERTY_LAST ) {
        if( phandle == ptype->int_handle ) {
          /**
            found a property of the type that we are looking for
           */
          GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
          if( record_size == old_property_entry_size ) {
            if( memcmp( old_value, (pos + GDA_PROPERTY_METADATA_SIZE), old_property_entry_size ) == 0 ) {
              /**
                found the property that we were looking for

                first we remove that property and do merging, if necessary
               */
              *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
              removed_property = true;
              /**
                introduced a new empty record, so update the appropriate counter
               */
              vertex->unused_space += old_property_entry_size + GDA_PROPERTY_METADATA_SIZE;

              if( previous_record != NULL ) {
                /**
                  check whether the previous record is also empty, and if so, merge them
                 */
                phandle = *(GDA_PropertyHandle*) previous_record;
                if( phandle == GDA_PROPERTY_EMPTY ) {
                  *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += old_property_entry_size + GDA_PROPERTY_METADATA_SIZE;
                  pos = previous_record;
                }
              }

              /**
                check whether the next record is also empty, and if so, merge them
               */
              char* next_record = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) next_record;
              if( phandle == GDA_PROPERTY_EMPTY ) {
                *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
              } else {
                if( phandle == GDA_PROPERTY_LAST ) {
                  *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
                }
              }

              /**
                only reach this code path, if we haven't found an insertion position
               */
              phandle = *(GDA_PropertyHandle*) pos;
              if( phandle != GDA_PROPERTY_LAST ) {
                GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
                if( (record_size == new_property_entry_size) || (record_size >= (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
                  /**
                    the just created empty record is big enough
                   */
                  insert_pos = pos;
                } else {
                  /**
                    check the remaining list for an empty record that is big enough to hold the new value
                   */
                  pos = GDA_NextPropertyRecord( pos );
                  phandle = *(GDA_PropertyHandle*) pos;
                  // TODO: jump ahead two records
                  while( phandle != GDA_PROPERTY_LAST ) {
                    if( phandle == GDA_PROPERTY_EMPTY ) {
                      GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
                      if( (record_size == new_property_entry_size) || (record_size >= (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
                        /**
                          found a record entry that is empty and big enough
                         */
                        insert_pos = pos;
                        /**
                          we have everything we need, so we can stop traversing the list
                          and break from the inner loop
                         */
                        break;
                      }
                    }
                    /**
                      advance to the next record
                     */
                    pos = GDA_NextPropertyRecord( pos );
                    phandle = *(GDA_PropertyHandle*) pos;
                  }
                }
              }
              /**
                we have everything we need, so we can stop traversing the list
               */
              break;
            } else {
              return GDI_ERROR_NO_PROPERTY;
            }
          } else {
            return GDI_ERROR_NO_PROPERTY;
          }
        } else {
          /**
            property is not of the type, that we are looking for
           */
          if( phandle == GDA_PROPERTY_EMPTY ) {
            GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
            if( (record_size == new_property_entry_size) || (record_size >= (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
              /**
                found a record entry that is empty and big enough
               */
              insert_pos = pos;

              /**
                check the remaining list for the old property value
               */
              previous_record = pos;
              pos = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) pos;
              while( phandle != GDA_PROPERTY_LAST ) {
                if( phandle == ptype->int_handle ) {
                  /**
                    found a property of the type that we are looking for
                   */
                  GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
                  if( record_size == old_property_entry_size ) {
                    if( memcmp( old_value, (pos + GDA_PROPERTY_METADATA_SIZE), old_property_entry_size ) == 0 ) {
                      /**
                        found the property that we were looking for

                        first we remove that property and do merging, if necessary
                       */
                      *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
                      removed_property = true;
                      /**
                        introduced a new empty record, so update the appropriate counter
                       */
                      vertex->unused_space += old_property_entry_size + GDA_PROPERTY_METADATA_SIZE;

                      /**
                        check whether the previous record is also empty, and if so, merge them

                        always have a previous record
                       */
                      phandle = *(GDA_PropertyHandle*) previous_record;
                      if( phandle == GDA_PROPERTY_EMPTY ) {
                        *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += old_property_entry_size + GDA_PROPERTY_METADATA_SIZE;
                        pos = previous_record;
                      }

                      /**
                        check whether the next record is also empty, and if so, merge them
                       */
                      char* next_record = GDA_NextPropertyRecord( pos );
                      phandle = *(GDA_PropertyHandle*) next_record;
                      if( phandle == GDA_PROPERTY_EMPTY ) {
                        *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
                      } else {
                        if( phandle == GDA_PROPERTY_LAST ) {
                          *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
                        }
                      }

                      /**
                        we have everything we need, so we can stop traversing the list
                        and break from the inner loop
                       */
                      break;
                    } else {
                      return GDI_ERROR_NO_PROPERTY;
                    }
                  } else {
                    return GDI_ERROR_NO_PROPERTY;
                  }
                }
                /**
                  advance to the next record in the list
                 */
                previous_record = pos;
                pos = GDA_NextPropertyRecord( pos );
                phandle = *(GDA_PropertyHandle*) pos;
              }
              /**
                we have everything we need, so we can stop traversing the list
                and break from the outer loop
               */
              break;
            }
          }
        }
        /**
          advance to the next record in the list
         */
        previous_record = pos;
        pos = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) pos;
      }

      if( !removed_property ) {
        return GDI_ERROR_NO_PROPERTY;
      }

      /**
        insertion of the new property
       */
      if( insert_pos != NULL ) {
        /**
          already found an sufficient sized empty record entry
         */
        GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE);
        if( record_size == new_property_entry_size ) {
          /**
            empty record fits perfectly, so we don't have to update the size part of the meta data
           */
          *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
          memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, new_value, new_property_entry_size );
        } else {
          /**
            we already know that the empty record is at least big enough to hold the new
            property record and an empty record, so no need to check again

            first insert the new property record
           */
          *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
          *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = new_property_entry_size;
          memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, new_value, new_property_entry_size );
          /**
            set a new empty record
           */
          insert_pos = insert_pos + GDA_PROPERTY_METADATA_SIZE + new_property_entry_size;
          *(GDA_PropertyHandle*) insert_pos = GDA_PROPERTY_EMPTY;
          *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = record_size - (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE);
        }
      } else {
        assert( *(GDA_PropertyHandle*) pos == GDA_PROPERTY_LAST );
        /**
          have to insert a record at the end

          pos points to the last record (= special record at the end of the list)
         */
        size_t new_list_size = pos + GDA_PROPERTY_METADATA_SIZE - vertex->property_data /* absolute size of the used memory */
          + GDA_PROPERTY_METADATA_SIZE + new_property_entry_size /* size of the new record (including meta data) */;
        if( new_list_size > vertex->property_size ) {
          /**
            allocated memory is not sufficient
           */
          size_t temp_pos = pos - vertex->property_data;
          do {
            vertex->unused_space += vertex->property_size;
            vertex->property_size = vertex->property_size << 1;
          } while( new_list_size > vertex->property_size );
          vertex->property_data = realloc( vertex->property_data, vertex->property_size );
          pos = vertex->property_data + temp_pos;
        }
        *(GDA_PropertyHandle*) pos = ptype->int_handle;
        *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) = new_property_entry_size;
        memcpy( pos+GDA_PROPERTY_METADATA_SIZE, new_value, new_property_entry_size );
        /**
          set a new last record
         */
        pos = pos + GDA_PROPERTY_METADATA_SIZE + new_property_entry_size;
        *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
      }

      /**
        update the unused space counter
       */
      assert( vertex->unused_space >= (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE) );
      vertex->unused_space -= new_property_entry_size + GDA_PROPERTY_METADATA_SIZE;
    }
  } else {
    /**
      multi entity type, so we also have to check the whole list, unless the new property is already present

      we have three conditions to meet:
      * insert position (unless we insert at the end)
      * find the old property value
      * make sure that the new property value is not already present
     */
    /**
      works together with previous_record and the assumption that previous_record
      is not updated anymore, once we found the old property value
     */
    char* remove_pos = NULL;
    if( ptype->stype == GDI_FIXED_SIZE ) {
      while( phandle != GDA_PROPERTY_LAST ) {
        if( phandle == ptype->int_handle ) {
          /**
            found a property of the type we are looking for
           */
          if( memcmp( old_value, (pos + GDA_PROPERTY_METADATA_SIZE), old_property_entry_size ) == 0 ) {
            /**
              found the property that we were looking for
             */
            remove_pos = pos;

            /**
              we only reach this code path, if we haven't found an insertion position
              since it is a fixed sized property type, the record is definitely big enough
             */
            insert_pos = pos;

            /**
              only thing left is to make sure that the new property value is not already
              present on the object
             */
            pos = GDA_NextPropertyRecord( pos );
            phandle = *(GDA_PropertyHandle*) pos;
            while( phandle != GDA_PROPERTY_LAST ) {
              if( phandle == ptype->int_handle ) {
                if( memcmp( new_value, (pos + GDA_PROPERTY_METADATA_SIZE), new_property_entry_size ) == 0 ) {
                  return GDI_ERROR_PROPERTY_EXISTS;
                }
              }
              /**
                advance to the next record in the list
               */
              pos = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) pos;
            }

            /**
              already finished traversing the list
             */
            break;
          } else {
            if( memcmp( new_value, (pos + GDA_PROPERTY_METADATA_SIZE), new_property_entry_size ) == 0 ) {
              return GDI_ERROR_PROPERTY_EXISTS;
            }
          }
        } else {
          /**
            property has not the type that we are looking for
           */
          if( phandle == GDA_PROPERTY_EMPTY ) {
            GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
            if( (record_size == new_property_entry_size) || (record_size >= (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
              /**
                found a record entry that is empty and big enough
               */
              insert_pos = pos;

              /**
                check the remaining list for the old and the new property value
               */
              previous_record = pos;
              pos = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) pos;
              while( phandle != GDA_PROPERTY_LAST ) {
                if( phandle == ptype->int_handle ) {
                  /**
                    found a property of the type we are looking for
                   */
                  if( memcmp( old_value, (pos + GDA_PROPERTY_METADATA_SIZE), old_property_entry_size ) == 0 ) {
                    /**
                      found the property that we were looking for
                     */
                    remove_pos = pos;

                    /**
                      only thing left is to make sure that the new property value is not already
                      present on the object
                     */
                    pos = GDA_NextPropertyRecord( pos );
                    phandle = *(GDA_PropertyHandle*) pos;
                    while( phandle != GDA_PROPERTY_LAST ) {
                      if( phandle == ptype->int_handle ) {
                        if( memcmp( new_value, (pos + GDA_PROPERTY_METADATA_SIZE), new_property_entry_size ) == 0 ) {
                          return GDI_ERROR_PROPERTY_EXISTS;
                        }
                      }
                      /**
                        advance to the next record in the list
                       */
                      pos = GDA_NextPropertyRecord( pos );
                      phandle = *(GDA_PropertyHandle*) pos;
                    }

                    /**
                      already finished traversing the list
                     */
                    break;
                  } else {
                    if( memcmp( new_value, (pos + GDA_PROPERTY_METADATA_SIZE), new_property_entry_size ) == 0 ) {
                      return GDI_ERROR_PROPERTY_EXISTS;
                    }
                  }
                }
                /**
                  advance to the next record in the list
                 */
                previous_record = pos;
                pos = GDA_NextPropertyRecord( pos );
                phandle = *(GDA_PropertyHandle*) pos;
              }

              /**
                we are already finished with traversing the list,
                so we break from the outer loop
               */
              break;
            }
          }
        }
        /**
          advance to the next record in the list
         */
        previous_record = pos;
        pos = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) pos;
      }

      /**
        removal of the old property
       */
      if( remove_pos == NULL ) {
        return GDI_ERROR_NO_PROPERTY;
      } else {
        /**
          found the old property value and at this point we are sure
          that we didn't meet any error condition

          first we remove the old property value record and do merging, if necessary
         */
        *(GDA_PropertyHandle*) remove_pos = GDA_PROPERTY_EMPTY;
        /**
          introduced a new empty record, so update the appropriate counter
         */
        vertex->unused_space += old_property_entry_size + GDA_PROPERTY_METADATA_SIZE;

        if( previous_record != NULL ) {
          /**
            check whether the previous record is also empty, and if so, merge them
           */
          phandle = *(GDA_PropertyHandle*) previous_record;
          if( phandle == GDA_PROPERTY_EMPTY ) {
            *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += old_property_entry_size + GDA_PROPERTY_METADATA_SIZE;
            if( insert_pos == remove_pos ) {
              insert_pos = previous_record;
            }
            remove_pos = previous_record;
          }
        }

        /**
          check whether the next record is also empty, and if so, merge them
         */
        char* next_record = GDA_NextPropertyRecord( remove_pos );
        phandle = *(GDA_PropertyHandle*) next_record;
        if( phandle == GDA_PROPERTY_EMPTY ) {
          *(GDA_PropertyRecordSize*) (remove_pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
        } else {
          if( phandle == GDA_PROPERTY_LAST ) {
            *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
          }
        }
      }

      assert( insert_pos != NULL );
      /**
        insertion of the new property
       */
      if( *(GDA_PropertyHandle*) insert_pos == GDA_PROPERTY_LAST ) {
        /**
          can always be sure, that we have enough space, because we just removed a property of the same size
         */
        *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
        *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = new_property_entry_size;
        memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, new_value, new_property_entry_size );
        /**
          set a new last record
         */
        insert_pos = insert_pos + GDA_PROPERTY_METADATA_SIZE + new_property_entry_size;
        *(GDA_PropertyHandle*) insert_pos = GDA_PROPERTY_LAST;
      } else {
        assert( *(GDA_PropertyHandle*) insert_pos == GDA_PROPERTY_EMPTY );
        GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE);
        if( record_size == new_property_entry_size ) {
          /**
            empty record fits perfectly, so we don't have to update the size part of the meta data
           */
          *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
          memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, new_value, new_property_entry_size );
        } else {
          /**
            we already know that the empty record is at least big enough to hold the new
            property record and an empty record, so no need to check again

            first insert the new property record
           */
          *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
          *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = new_property_entry_size;
          memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, new_value, new_property_entry_size );
          /**
            set a new empty record
           */
          insert_pos = insert_pos + GDA_PROPERTY_METADATA_SIZE + new_property_entry_size;
          *(GDA_PropertyHandle*) insert_pos = GDA_PROPERTY_EMPTY;
          *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = record_size - (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE);
        }
      }

      /**
        update the unused space counter
       */
      assert( vertex->unused_space >= (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE) );
      vertex->unused_space -= new_property_entry_size + GDA_PROPERTY_METADATA_SIZE;
    } else {
      /**
        multi entity type which is a maximum sized type or has no size limit

        we have three conditions to meet:
        * insert position (unless we insert at the end)
        * find the old property value
        * make sure that the new property value is not already present
       */
      /**
        works together with previous_record and the assumption that previous_record
        is not updated anymore, once we found the old property value
       */
      char* remove_pos = NULL;
      while( phandle != GDA_PROPERTY_LAST ) {
        if( phandle == ptype->int_handle ) {
          GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
          if( record_size == new_property_entry_size ) {
            if( memcmp( new_value, (pos + GDA_PROPERTY_METADATA_SIZE), new_property_entry_size ) == 0 ) {
              return GDI_ERROR_PROPERTY_EXISTS;
            }
          }
          if( record_size == old_property_entry_size ) {
            if( memcmp( old_value, (pos + GDA_PROPERTY_METADATA_SIZE), old_property_entry_size ) == 0 ) {
              /**
                found the property that we were looking for
               */
              remove_pos = pos;

              /**
                haven't found a position to insert so far

                so check, whether this record (with the neighboring records, if empty) is big
                enough to accommodate the new property value

                we do this right here, because later (with the block design) it might not be that
                easy to check whether a pointer points to a later record in the list
               */
              size_t emptyBytes = old_property_entry_size;
              if( previous_record != NULL ) {
                phandle = *(GDA_PropertyHandle*) previous_record;
                if( phandle == GDA_PROPERTY_EMPTY ) {
                  emptyBytes += *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
                }
              }

              char* next_record = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) next_record;
              if( phandle == GDA_PROPERTY_EMPTY ) {
                emptyBytes += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
              }

              if( (emptyBytes == new_property_entry_size) || (emptyBytes >= (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE)) ) {
                insert_pos = pos; /* equal to remove_pos */

                /**
                  check the remaining list for the new property value
                 */
                pos = GDA_NextPropertyRecord( pos );
                phandle = *(GDA_PropertyHandle*) pos;
                while( phandle != GDA_PROPERTY_LAST ) {
                  if( phandle == ptype->int_handle ) {
                    GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
                    if( record_size == new_property_entry_size ) {
                      if( memcmp( new_value, (pos + GDA_PROPERTY_METADATA_SIZE), new_property_entry_size ) == 0 ) {
                        return GDI_ERROR_PROPERTY_EXISTS;
                      }
                    }
                  }
                  /**
                    advance to the next record in the list
                   */
                  pos = GDA_NextPropertyRecord( pos );
                  phandle = *(GDA_PropertyHandle*) pos;
                }

                /**
                  finished with traversing the list
                 */
                break;
              }

              /**
                still have two conditions to meet:
                * find a position to insert
                * make sure that the new property value is not already present on the object
               */
              pos = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) pos;
              while( phandle != GDA_PROPERTY_LAST ) {
                if( phandle == ptype->int_handle ) {
                  GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
                  if( record_size == new_property_entry_size ) {
                    if( memcmp( new_value, (pos + GDA_PROPERTY_METADATA_SIZE), new_property_entry_size ) == 0 ) {
                      return GDI_ERROR_PROPERTY_EXISTS;
                    }
                  }
                } else {
                  /**
                    property has not the type that we are looking for
                   */
                  if( phandle == GDA_PROPERTY_EMPTY ) {
                    GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
                    if( (record_size == new_property_entry_size) || (record_size >= (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
                      /**
                        found a record entry that is empty and big enough
                       */
                      insert_pos = pos;

                      /**
                        check the remaining list for the new property value
                       */
                      pos = GDA_NextPropertyRecord( pos );
                      phandle = *(GDA_PropertyHandle*) pos;
                      while( phandle != GDA_PROPERTY_LAST ) {
                        if( phandle == ptype->int_handle ) {
                          GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
                          if( record_size == new_property_entry_size ) {
                            if( memcmp( new_value, (pos + GDA_PROPERTY_METADATA_SIZE), new_property_entry_size ) == 0 ) {
                              return GDI_ERROR_PROPERTY_EXISTS;
                            }
                          }
                        }
                        /**
                          advance to the next record in the list
                         */
                        pos = GDA_NextPropertyRecord( pos );
                        phandle = *(GDA_PropertyHandle*) pos;
                      }

                      /**
                        finished with traversing the list
                       */
                      break;
                    }
                  }
                }
                /**
                  advance to the next record in the list
                 */
                pos = GDA_NextPropertyRecord( pos );
                phandle = *(GDA_PropertyHandle*) pos;
              }

              /**
                already finished traversing the list
               */
              break;
            }
          }
        } else {
          /**
            property is not of the type that we are looking for
           */
          if( phandle == GDA_PROPERTY_EMPTY ) {
            GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
            if( (record_size == new_property_entry_size) || (record_size >= (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
              /**
                 found a record entry that is empty and big enough
               */
              insert_pos = pos;

              /**
                check the remaining list for the old and the new property value
               */
              previous_record = pos;
              pos = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) pos;
              while( phandle != GDA_PROPERTY_LAST ) {
                if( phandle == ptype->int_handle ) {
                  /**
                    found a property of the type we are looking for
                   */
                  GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
                  if( record_size == new_property_entry_size ) {
                    if( memcmp( new_value, (pos + GDA_PROPERTY_METADATA_SIZE), new_property_entry_size ) == 0 ) {
                      return GDI_ERROR_PROPERTY_EXISTS;
                    }
                  }
                  if( record_size == old_property_entry_size ) {
                    if( memcmp( old_value, (pos + GDA_PROPERTY_METADATA_SIZE), old_property_entry_size ) == 0 ) {
                      /**
                        found the old property value that we were looking for
                       */
                      remove_pos = pos;

                      /**
                        only thing left is to make sure that the new property value is not already
                        present on the object
                       */
                      pos = GDA_NextPropertyRecord( pos );
                      phandle = *(GDA_PropertyHandle*) pos;
                      while( phandle != GDA_PROPERTY_LAST ) {
                        GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
                        if( record_size == new_property_entry_size ) {
                          if( memcmp( new_value, (pos + GDA_PROPERTY_METADATA_SIZE), new_property_entry_size ) == 0 ) {
                            return GDI_ERROR_PROPERTY_EXISTS;
                          }
                        }
                        /**
                          advance to the next record in the list
                         */
                        pos = GDA_NextPropertyRecord( pos );
                        phandle = *(GDA_PropertyHandle*) pos;
                      }
                    }
                  }
                }
                /**
                  advance to the next record in the list
                 */
                previous_record = pos;
                pos = GDA_NextPropertyRecord( pos );
                phandle = *(GDA_PropertyHandle*) pos;
              }

              /**
                already finished traversing the list
               */
              break;
            }
          }
        }
        /**
          advance to the next record in the list
         */
        previous_record = pos;
        pos = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) pos;
      }

      /**
        removal of the old property
       */
      if( remove_pos == NULL ) {
        return GDI_ERROR_NO_PROPERTY;
      } else {
        /**
          found the old property value and at this point we are sure
          that we didn't meet any error condition (TODO: What about ressource exhaustion errors?)

          first we remove the old property value record and do merging, if necessary
         */
        *(GDA_PropertyHandle*) remove_pos = GDA_PROPERTY_EMPTY;
        /**
          introduced a new empty record, so update the appropriate counter
         */
        vertex->unused_space += old_property_entry_size + GDA_PROPERTY_METADATA_SIZE;

        if( previous_record != NULL ) {
          /**
            check whether the previous record is also empty, and if so, merge them
           */
          phandle = *(GDA_PropertyHandle*) previous_record;
          if( phandle == GDA_PROPERTY_EMPTY ) {
            *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += old_property_entry_size + GDA_PROPERTY_METADATA_SIZE;
            if( insert_pos == remove_pos ) {
              insert_pos = previous_record;
            }
            remove_pos = previous_record;
          }
        }

        /**
          check whether the next record is also empty, and if so, merge them
         */
        char* next_record = GDA_NextPropertyRecord( remove_pos );
        phandle = *(GDA_PropertyHandle*) next_record;
        if( phandle == GDA_PROPERTY_EMPTY ) {
          *(GDA_PropertyRecordSize*) (remove_pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
        } else {
          if( phandle == GDA_PROPERTY_LAST ) {
            *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
          }
        }
      }

      /**
        insertion of the new property
       */
      if( insert_pos != NULL ) {
        /**
          already found an sufficient sized empty record entry
         */
        GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE);
        if( record_size == new_property_entry_size ) {
          /**
            empty record fits perfectly, so we don't have to update the size part of the meta data
           */
          *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
          memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, new_value, new_property_entry_size );
        } else {
          /**
            we already know that the empty record is at least big enough to hold the new
            property record and an empty record, so no need to check again

            first insert the new property record
           */
          *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
          *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = new_property_entry_size;
          memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, new_value, new_property_entry_size );
          /**
            set a new empty record
           */
          insert_pos = insert_pos + GDA_PROPERTY_METADATA_SIZE + new_property_entry_size;
          *(GDA_PropertyHandle*) insert_pos = GDA_PROPERTY_EMPTY;
          *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = record_size - (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE);
        }
      } else {
        assert( *(GDA_PropertyHandle*) pos == GDA_PROPERTY_LAST );
        /**
          have to insert a record at the end
          pos points to the last record (= special record at the end of the list)
         */
        size_t new_list_size = pos + GDA_PROPERTY_METADATA_SIZE - vertex->property_data /* absolute size of the used memory */
          + GDA_PROPERTY_METADATA_SIZE + new_property_entry_size /* size of the new record (including meta data) */;
        if( new_list_size > vertex->property_size ) {
          /**
            allocated memory is not sufficient
           */
          size_t temp_pos = pos - vertex->property_data;
          do {
            vertex->unused_space += vertex->property_size;
            vertex->property_size = vertex->property_size << 1;
          } while( new_list_size > vertex->property_size );
          vertex->property_data = realloc( vertex->property_data, vertex->property_size );
          pos = vertex->property_data + temp_pos;
        }
        *(GDA_PropertyHandle*) pos = ptype->int_handle;
        *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) = new_property_entry_size;
        memcpy( pos+GDA_PROPERTY_METADATA_SIZE, new_value, new_property_entry_size );
        /**
          set a new last record
         */
        pos = pos + GDA_PROPERTY_METADATA_SIZE + new_property_entry_size;
        *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
      }

      /**
        update the unused space counter
       */
      assert( vertex->unused_space >= (new_property_entry_size + GDA_PROPERTY_METADATA_SIZE) );
      vertex->unused_space -= new_property_entry_size + GDA_PROPERTY_METADATA_SIZE;
    }
  }

  return GDI_SUCCESS;
}


/**
  assumes that ptype is not GDI_PROPERTY_TYPE_NULL, GDI_PROPERTY_TYPE_DEGREE, GDI_PROPERTY_TYPE_INDEGREE and GDI_PROPERTY_TYPE_OUTDEGREE

  assumes that ptype is a single entity type

  found_flag is returned, in case the caller wants to keep skip updating indexes and indicates
  whether the property was present on the vertex:
  true  = property was updated
  false = no property of that type was found

  uses the same implementation as GDA_LinearScanningUpdateSingleEntityProperty (minus a check),
  so any changes should reflect there as well

  TODO: not sure that we can do this without retrieving the original value first, so that we can update the indexes later

  TODO: Wait with the removal until enough space for the insertion as acquired?

  updates the unused_space counter
 */
void GDA_LinearScanningSetSingleEntityProperty( GDI_PropertyType ptype, const void* value, size_t count, GDI_VertexHolder vertex, bool* found_flag ) {
  char* pos = vertex->property_data;
  GDA_PropertyHandle phandle = *(GDA_PropertyHandle*) pos;

  /**
    determine the buffer size for the property value
   */
  size_t dsize;
  GDI_GetSizeOfDatatype( &dsize, ptype->dtype );
  size_t property_entry_size = count * dsize;

  *found_flag = false;

  /**
    input checking
   */
  assert( ptype->int_handle < 256 );
  /**
    TODO: enable this again, once we move back to the default of 16 Bits for the record size field
    assert( property_entry_size < 65536 );
   */

  char* previous_record = NULL;
  char* insert_pos = NULL; /* remove compiler warning */

  if( ptype->stype == GDI_FIXED_SIZE ) {
    /**
      look for the property to be updated (and also a place for insertion)
     */
    while( phandle != GDA_PROPERTY_LAST ) {
      if( phandle == ptype->int_handle ) {
        /**
          found the property of the type that we are looking for

          first we remove that property and do merging, if necessary
         */
        *found_flag = true;
        *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
        /**
          introduced a new empty record, so update the appropriate counter
         */
        vertex->unused_space += property_entry_size + GDA_PROPERTY_METADATA_SIZE;

        if( previous_record != NULL ) {
          /**
            check whether the previous record is also empty, and if so, merge them
           */
          phandle = *(GDA_PropertyHandle*) previous_record;
          if( phandle == GDA_PROPERTY_EMPTY ) {
            *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += property_entry_size + GDA_PROPERTY_METADATA_SIZE;
            pos = previous_record;
          }
        }

        /**
          check whether the next record is also empty, and if so, merge them
         */
        char* next_record = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) next_record;
        if( phandle == GDA_PROPERTY_EMPTY ) {
          *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
        } else {
          if( phandle == GDA_PROPERTY_LAST ) {
            *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;

            /**
              haven't found an insert position, so we want to insert at the end,
              meaning we don't want to set insert_pos
             */
            break;
          }
        }

        /**
          we only reach this code path, if we haven't found an insertion position
          since it is a fixed sized property type, the record is definitely big enough
         */
        insert_pos = pos;

        /**
          we have everything we need, so we can stop traversing the list
         */
        break;
      } else {
        /**
          property is not of the type, that we are looking for
         */
        if( phandle == GDA_PROPERTY_EMPTY ) {
          GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
          if( (record_size == property_entry_size) || (record_size >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
            /**
              found a record entry that is empty and big enough
             */
            insert_pos = pos;

            /**
              check the remaining list to remove the existing property
             */
            previous_record = pos;
            pos = GDA_NextPropertyRecord( pos );
            phandle = *(GDA_PropertyHandle*) pos;
            while( phandle != GDA_PROPERTY_LAST ) {
              if( phandle == ptype->int_handle ) {
                /**
                  found the property of the type that we are looking for

                  we remove that property and do merging, if necessary
                 */
                *found_flag = true;
                *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
                /**
                  introduced a new empty record, so update the appropriate counter
                 */
                vertex->unused_space += property_entry_size + GDA_PROPERTY_METADATA_SIZE;

                /**
                  check whether the previous record is also empty, and if so, merge them

                  guaranteed to have a previous record
                 */
                phandle = *(GDA_PropertyHandle*) previous_record;
                if( phandle == GDA_PROPERTY_EMPTY ) {
                  *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += property_entry_size + GDA_PROPERTY_METADATA_SIZE;
                  pos = previous_record;
                }

                /**
                  check whether the next record is also empty, and if so, merge them
                 */
                char* next_record = GDA_NextPropertyRecord( pos );
                phandle = *(GDA_PropertyHandle*) next_record;
                if( phandle == GDA_PROPERTY_EMPTY ) {
                  *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
                } else {
                  if( phandle == GDA_PROPERTY_LAST ) {
                    *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
                  }
                }

                /**
                  we have everything we need, so we can stop traversing the list
                 */
                break;
              }
              /**
                advance to the next record in the list
               */
              previous_record = pos;
              pos = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) pos;
            }
            /**
              we are already finished with traversing the list,
              so we break from the outer loop
             */
            break;
          }
        }
      }
      /**
        advance to the next record in the list
       */
      previous_record = pos;
      pos = GDA_NextPropertyRecord( pos );
      phandle = *(GDA_PropertyHandle*) pos;
    }
  } else {
    /**
      property type which is a maximum sized type or has no size limit

      first look for the property to be updated (and also a place for insertion)
     */
    while( phandle != GDA_PROPERTY_LAST ) {
      if( phandle == ptype->int_handle ) {
        /**
          found the property of the type that we are looking for

          first we remove that property and do merging, if necessary
         */
        *found_flag = true;
        *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
        /**
          introduced a new empty record, so update the appropriate counter
         */
        vertex->unused_space += *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;

        if( previous_record != NULL ) {
          /**
            check whether the previous record is also empty, and if so, merge them
           */
          phandle = *(GDA_PropertyHandle*) previous_record;
          if( phandle == GDA_PROPERTY_EMPTY ) {
            *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
            pos = previous_record;
          }
        }

        /**
          check whether the next record is also empty, and if so, merge them
         */
        char* next_record = GDA_NextPropertyRecord( pos );
        phandle = *(GDA_PropertyHandle*) next_record;
        if( phandle == GDA_PROPERTY_EMPTY ) {
          *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
        } else {
          if( phandle == GDA_PROPERTY_LAST ) {
            *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
            break;
          }
        }

        /**
          only reach this code path, if we haven't found an insertion position

          current record is guaranteed to be empty
         */
        GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
        if( (record_size == property_entry_size) || (record_size >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
          /**
            the just created empty record is big enough
           */
          insert_pos = pos;
          break;
        } else {
          /**
            check the remaining list for an empty record that is big enough to hold the new value
           */
          pos = GDA_NextPropertyRecord( pos );
          phandle = *(GDA_PropertyHandle*) pos;
          while( phandle != GDA_PROPERTY_LAST ) {
            if( phandle == GDA_PROPERTY_EMPTY ) {
              GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
              if( (record_size == property_entry_size) || (record_size >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
                /**
                  found a record entry that is empty and big enough
                 */
                insert_pos = pos;
                /**
                  we have everything we need, so we can stop traversing the list
                  and break from the inner loop
                 */
                break;
              }
            }
            /**
              advance to the next record
             */
            pos = GDA_NextPropertyRecord( pos );
            phandle = *(GDA_PropertyHandle*) pos;
          }
        }
        /**
          we have everything we need, so we can stop traversing the list
         */
        break;
      } else {
        /**
          property is not of the type, that we are looking for
         */
        if( phandle == GDA_PROPERTY_EMPTY ) {
          GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE);
          if( (record_size == property_entry_size) || (record_size >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) ) ) {
            /**
              found a record entry that is empty and big enough
             */
            insert_pos = pos;

            /**
              check the remaining list to remove the existing property
             */
            previous_record = pos;
            pos = GDA_NextPropertyRecord( pos );
            phandle = *(GDA_PropertyHandle*) pos;
            while( phandle != GDA_PROPERTY_LAST ) {
              if( phandle == ptype->int_handle ) {
                /**
                  found the property of the type that we are looking for

                  we remove that property and do merging, if necessary
                 */
                *found_flag = true;
                *(GDA_PropertyHandle*) pos = GDA_PROPERTY_EMPTY;
                /**
                  introduced a new empty record, so update the appropriate counter
                 */
                vertex->unused_space += *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;

                /**
                  check whether the previous record is also empty, and if so, merge them

                  guaranteed to have a previous record
                 */
                phandle = *(GDA_PropertyHandle*) previous_record;
                if( phandle == GDA_PROPERTY_EMPTY ) {
                  *(GDA_PropertyRecordSize*) (previous_record+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
                  pos = previous_record;
                }

                /**
                  check whether the next record is also empty, and if so, merge them
                 */
                char* next_record = GDA_NextPropertyRecord( pos );
                phandle = *(GDA_PropertyHandle*) next_record;
                if( phandle == GDA_PROPERTY_EMPTY ) {
                  *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) += *(GDA_PropertyRecordSize*) (next_record+GDA_PROPERTY_HANDLE_SIZE) + GDA_PROPERTY_METADATA_SIZE;
                } else {
                  if( phandle == GDA_PROPERTY_LAST ) {
                    *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
                  }
                }

                /**
                  we have everything we need, so we can stop traversing the list
                 */
                break;
              }
              /**
                advance to the next record in the list
               */
              previous_record = pos;
              pos = GDA_NextPropertyRecord( pos );
              phandle = *(GDA_PropertyHandle*) pos;
            }
            /**
              we are already finished with traversing the list,
              so we break from the outer loop
             */
            break;
          }
        }
      }
      /**
        advance to the next record in the list
       */
      previous_record = pos;
      pos = GDA_NextPropertyRecord( pos );
      phandle = *(GDA_PropertyHandle*) pos;
    }
  }

  /**
    insertion of the new property
   */
  if( insert_pos != NULL ) {
    /**
      already found an sufficient sized empty record entry
     */
    GDA_PropertyRecordSize record_size = *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE);
    if( record_size == property_entry_size ) {
      /**
        empty record fits perfectly, so we don't have to update the size part of the meta data
       */
      *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
      memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, value, property_entry_size );
    } else {
      /**
        we already know that the empty record is at least big enough to hold the new
        property record and an empty record, so no need to check again

        first insert the new property record
       */
      *(GDA_PropertyHandle*) insert_pos = ptype->int_handle;
      *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = property_entry_size;
      memcpy( insert_pos+GDA_PROPERTY_METADATA_SIZE, value, property_entry_size );
      /**
        set a new empty record
       */
      insert_pos = insert_pos + GDA_PROPERTY_METADATA_SIZE + property_entry_size;
      *(GDA_PropertyHandle*) insert_pos = GDA_PROPERTY_EMPTY;
      *(GDA_PropertyRecordSize*) (insert_pos+GDA_PROPERTY_HANDLE_SIZE) = record_size - (property_entry_size + GDA_PROPERTY_METADATA_SIZE);
    }
  } else {
    assert( *(GDA_PropertyHandle*) pos == GDA_PROPERTY_LAST );
    /**
      have to insert a record at the end

      pos points to the last record (= special record at the end of the list)
     */
    size_t new_list_size = pos + GDA_PROPERTY_METADATA_SIZE - vertex->property_data /* absolute size of the used memory */
      + GDA_PROPERTY_METADATA_SIZE + property_entry_size /* size of the new record (including meta data) */;
    if( new_list_size > vertex->property_size ) {
      /**
        allocated memory is not sufficient
       */
      size_t temp_pos = pos - vertex->property_data;
      do {
        vertex->unused_space += vertex->property_size;
        vertex->property_size = vertex->property_size << 1;
      } while( new_list_size > vertex->property_size );
      vertex->property_data = realloc( vertex->property_data, vertex->property_size );
      pos = vertex->property_data + temp_pos;
    }
    *(GDA_PropertyHandle*) pos = ptype->int_handle;
    *(GDA_PropertyRecordSize*) (pos+GDA_PROPERTY_HANDLE_SIZE) = property_entry_size;
    memcpy( pos+GDA_PROPERTY_METADATA_SIZE, value, property_entry_size );
    /**
      set a new last record
     */
    pos = pos + GDA_PROPERTY_METADATA_SIZE + property_entry_size;
    *(GDA_PropertyHandle*) pos = GDA_PROPERTY_LAST;
  }

  /**
    used an empty record, so update the appropriate counter
   */
  assert( vertex->unused_space >= (property_entry_size + GDA_PROPERTY_METADATA_SIZE) );
  vertex->unused_space -= property_entry_size + GDA_PROPERTY_METADATA_SIZE;
}
