// Copyright (c) 2023 ETH Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef __GDI_H
#define __GDI_H

#include <inttypes.h>
#include <stdbool.h>

#include "rma.h" /* equal to #include <mpi.h> */

#include "gdi_constraint.h"
#include "gdi_datatype.h"
#include "gdi_label.h"
#include "gdi_property_type.h"
#include "gda_distributed_hashtable.h"
#include "gda_dpointer.h"
#include "gda_vector.h"
#include "gda_edge_uid.h"

/**
  general GDI header file
 */


/**
  constant definitions
 */

#define GDI_MAX_OBJECT_NAME 64
#define GDI_FALSE 0
#define GDI_TRUE 1

/**
  property type entity type (state)

  stored as uint8_t
 */
#define GDI_SINGLE_ENTITY                 198
#define GDI_MULTIPLE_ENTITY               199

/**
  property type size limit (state)

  stored as uint8_t
 */
#define GDI_FIXED_SIZE                    200
#define GDI_MAX_SIZE                      201
#define GDI_NO_SIZE_LIMIT                 202

/**
  edge direction type (state parameter for some function calls)

  when only looking at the lowest Byte, only 3 Bits are occupied and
  each constant only occupies one of those 3 Bits (apart from
  GDI_EDGE_DIRECTED, which is not important for that operation), so they
  can be used for a bitwise OR operation for GDI_GetEdgesOfVertex and
  GDI_GetNeighborVerticesOfVertex
 */
#define GDI_EDGE_INCOMING                 257
#define GDI_EDGE_OUTGOING                 258
#define GDI_EDGE_DIRECTED                 259
#define GDI_EDGE_UNDIRECTED               260

/**
  transaction commit type (state)
 */
#define GDI_TRANSACTION_COMMIT          11000
#define GDI_TRANSACTION_ABORT           11001

/**
  transaction type (state)

  stored as uint8_t
 */
#define GDI_SINGLE_PROCESS_TRANSACTION    203
#define GDI_COLLECTIVE_TRANSACTION        204

/**
  error classses

  The error classes are sorted by category, and alphabetically within
  those categories. Right now, there is no code that relies on those
  facts, so the error classes can be reordered, if necessary.
 */

#define GDI_SUCCESS                            0
/**
  warning error classes
 */
#define GDI_WARNING_NON_UNIQUE_ID              1
#define GDI_WARNING_OTHER                      2
/**
  preoperational error classes
 */
#define GDI_ERROR_ACCESS                       3
#define GDI_ERROR_ARGUMENT                     4
#define GDI_ERROR_BAD_FILE                     5
#define GDI_ERROR_BUFFER                       6
#define GDI_ERROR_CONSTRAINT                   7
#define GDI_ERROR_CONVERSION                   8
#define GDI_ERROR_COUNT                        9
#define GDI_ERROR_DATABASE                    10
#define GDI_ERROR_DATATYPE                    11
#define GDI_ERROR_DATE                        12
#define GDI_ERROR_DATETIME                    13
#define GDI_ERROR_DECIMAL                     14
#define GDI_ERROR_EDGE                        15
#define GDI_ERROR_EDGE_ORIENTATION            16
#define GDI_ERROR_EMPTY_NAME                  17
#define GDI_ERROR_ERROR_CODE                  18
#define GDI_ERROR_FILE_EXISTS                 19
#define GDI_ERROR_FILE_IN_USE                 20
#define GDI_ERROR_INCOMPATIBLE_TRANSACTIONS   21
#define GDI_ERROR_INDEX                       22
#define GDI_ERROR_LABEL                       23
#define GDI_ERROR_NAME_EXISTS                 24
#define GDI_ERROR_NO_MEMORY                   25
#define GDI_ERROR_NO_PROPERTY                 26
#define GDI_ERROR_NO_SPACE                    27
#define GDI_ERROR_NO_SUCH_FILE                28
#define GDI_ERROR_NON_UNIQUE_ID               29
#define GDI_ERROR_NOT_SAME                    30
#define GDI_ERROR_OBJECT_MISMATCH             31
#define GDI_ERROR_OP                          32
#define GDI_ERROR_OP_DATATYPE_MISMATCH        33
#define GDI_ERROR_PROPERTY_EXISTS             34
#define GDI_ERROR_PROPERTY_TYPE               35
#define GDI_ERROR_PROPERTY_TYPE_EXISTS        36
#define GDI_ERROR_RANGE                       37
#define GDI_ERROR_READ_ONLY_FILE              38
#define GDI_ERROR_READ_ONLY_PROPERTY_TYPE     39
#define GDI_ERROR_READ_ONLY_TRANSACTION       40
#define GDI_ERROR_RESOURCE                    41
#define GDI_ERROR_SIZE                        42
#define GDI_ERROR_SIZE_LIMIT                  43
#define GDI_ERROR_STALE                       44
#define GDI_ERROR_STATE                       45
#define GDI_ERROR_SUBCONSTRAINT               46
#define GDI_ERROR_TIME                        47
#define GDI_ERROR_TRANSACTION                 48
#define GDI_ERROR_UID                         49
#define GDI_ERROR_VERTEX                      50
#define GDI_ERROR_WRONG_TYPE                  51
#define GDI_ERROR_INVALID_DATE                52 // error code introduced by NOD: belongs to error class GDI_ERROR_ARGUMENT
#define GDI_ERROR_COMMUNICATOR                53 // error code introduced by NOD: belongs to error class GDI_ERROR_ARGUMENT
#define GDI_ERROR_BLOCK_SIZE                  54 // error code introduced by NOD: belongs to error class GDI_ERROR_ARGUMENT
/**
  output argument error classes
 */
#define GDI_ERROR_QUOTA                       55
#define GDI_ERROR_TRUNCATE                    56
/*
  transaction-critical error classes
 */
#define GDI_ERROR_TRANSACTION_CRITICAL        57
/**
  transaction commit failed error class
 */
#define GDI_ERROR_TRANSACTION_COMMIT_FAIL     58
/**
  undefined error classes
 */
#define GDI_ERROR_INTERN                      59
#define GDI_ERROR_IO                          60
#define GDI_ERROR_OTHER                       61
#define GDI_ERROR_UNKNOWN                     62
/**
  last error code
 */
#define GDI_ERROR_LASTCODE                    62

/**
  NULL handles
 */
// TODO: check how they do it in MPICH/OpenMPI
// definitely try to give them different values
#define GDI_CONSTRAINT_NULL     NULL
#define GDI_DATABASE_NULL       NULL
#define GDI_EDGE_NULL           NULL
#define GDI_LABEL_NULL          NULL
#define GDI_PROPERTY_TYPE_NULL  NULL
#define GDI_SUBCONSTRAINT_NULL  NULL
#define GDI_TRANSACTION_NULL    NULL
#define GDI_VERTEX_NULL         NULL


/**
  data type definitions
 */

// TODO: Move MPI related entries into a "sub-struct"?
/**
  database data structure
 */
typedef struct GDI_Database_desc {

  GDI_Label_db_t* labels;
  GDI_Constraint_db_t* constraints;
  GDI_PropertyType_db_t* ptypes;
  GDA_RMAHashMap_desc_t* internal_index;

  /**
    list of all transactions that the local process
    is part of
   */
  GDA_List* transactions;
  /**
    base pointer of the block window
    points to the local memory
   */
  void* win_blocks_baseptr;
  /**
    base pointer of the system window
    points to the local memory
   */
  uint64_t* win_system_baseptr;
  /**
    base pointer of the usage window
    points to the local memory
   */
  uint32_t* win_usage_baseptr;
  /* block window */
  RMA_Win win_blocks;
  /* system window */
  RMA_Win win_system;
  /* usage window */
  RMA_Win win_usage;
  /**
    communicator of the graph database object
    identifies the processes that are part of the graph database

    duplicate of the original communicator passed to GDI_CreateDatabase
   */
  MPI_Comm comm;
  /**
    theoretical size of the local portion of the graph database
    initial size argument to GDI_CreateDatabase
   */
  MPI_Aint memsize;
  /**
    size of the block window
    multiple of block_size
   */
  MPI_Aint win_blocks_size;
  /**
    size of the system window
    multiple of 8 Bytes (= sizeof(uint64_t))
   */
  MPI_Aint win_system_size;
  /**
   size of the usage window
   multiple of 4 Bytes (= sizeof(uint32_t))
   */
  MPI_Aint win_usage_size;
  /**
    block size of the database (in Bytes)
   */
  uint32_t block_size;
  /* rank of the process inside the communicator */
  int commrank;
  /* size of the communicator */
  unsigned int commsize;
  /**
    flag to indicate whether a collective transaction is active
    on the local process

    true  = collective transaction is active
    false = no collective transaction is underway
   */
  bool collective_flag;
} GDI_Database_desc_t;

typedef GDI_Database_desc_t* GDI_Database;


typedef struct GDI_Transaction_desc {
  /**
    pointer to the associated database
   */
  GDI_Database db;
  /**
    Database keeps track of all transactions with a list.
    Pointer to the element in the list that represents
    the current transaction, so we can remove it in O(1).
   */
  GDA_Node* db_listptr;
  /**
    hash map to check whether a vertex (identified by his vertex UID)
    is already associated (locally) with this transaction
   */
  GDA_HashMap* v_translate_d2l;
  /**
    A vector to keep track of all vertices associated with this
    transaction. It will include vertex objects, that are already
    "freed" by a call of GDI_FreeVertex. However we still have to delay
    any globally visible action until a commit is called, so those
    vertices are still part of the data structure.

    A vector can be used, because we will only add to the data
    structure and remove all elements at the end.
   */
  GDA_Vector* vertices;
  /**
    A vector to keep track of all edges associated with this
    transaction. it will include edge objects, that are already "freed"
    by a call of GDI_FreeEdge. How we still have to delay any globally
    visible action until a commit is called, so those edges are still
    part of the data structure.

    A vector can be used, because we will only add to the data
    structure and remove all elements at the end.
   */
  GDA_Vector* edges;
  /**
    TODO: some sort of list/vector for all EdgeHolder
    objects associated with this object

    a vector can probably be used, since we only add to the data structure
    and remove it all at once during the commit
   */
  /**
    transaction type

    GDI_COLLECTIVE_TRANSACTION indicates a collective transaction
    GDI_SINGLE_PROCESS_TRANSACTION indicates a single process transaction
   */
  uint8_t type;
  /**
    flag to indicate whether there were any write operations
    during the transaction

    true  = write operations
    false = only read operations

    a collective transaction should always be read only, so the write
    flag should always be false
   */
  bool write_flag;
  /**
    flag to indicate whether a transaction critical error occured
    during the transaction

    true  = transaction critical error occured
    false = no problems during the transaction
   */
  bool critical_flag;
} GDI_Transaction_desc_t;

typedef GDI_Transaction_desc_t* GDI_Transaction;


typedef struct GDI_VertexHolder_desc {
  /**
    list of all edge objects that are associated with the vertex
   */
  GDA_List* edges;
  /**
    data structure to keep track of all blocks associated with
    the vertex

    the first block serves as the primary block
   */
  GDA_Vector* blocks;
  /**
    pointer to the transaction that the vertex object is part of
   */
  GDI_Transaction transaction;
  /**
    pointer to the labeled lightweight edge buffer
   */
  uint64_t* lightweight_edge_data;
  /**
    pointer to the property/label data
   */
  char* property_data;
  /**
    size of the continuous property data
   */
  uint64_t property_size;
  /**
    size in Bytes of the unused space in the property data
    (empty records, space after the last record)
   */
  uint64_t unused_space;
  /**
    size of the labeled lightweight edge buffer
   */
  size_t lightweight_edge_size;
  /**
    offset to the next free edge space
    only append at the back
   */
  uint32_t lightweight_edge_insert_offset;
  /**
    incarnation field from the lock
   */
  uint32_t incarnation;
  /**
    Value which stores information about the type of the lock
    acquired on the associated vertex.

    Possible values:
    GDA_NO_LOCK
    GDA_READ_LOCK
    GDA_WRITE_LOCK
   */
  uint8_t lock_type;
  /**
    flag to indicate, whether the vertex is marked for deletion

    true  = marked for deletion
    false = vertex will remain in the database
   */
  bool delete_flag;
  /**
    flag to indicate, whether the vertex was changed during the
    transaction

    additionally have to check, whether any of the GDI_EdgeHolder
    objects associated with this vertex have been changed

    true  = vertex was changed
    false = only read access
   */
  bool write_flag;
  /**
    flag to indicate, whether the vertex was created during this
    transaction

    true  = vertex was created during this transaction
    false = vertex is already present in the database
   */
  bool creation_flag;
} GDI_VertexHolder_desc_t;

typedef GDI_VertexHolder_desc_t* GDI_VertexHolder;


typedef struct GDI_EdgeHolder_desc {
  /**
    Vertices keep track of all associated edges with a list. Pointer
    to the element in the list of the origin/target vertex that
    represents the current edge, so we can remove it in O(1).

    TODO: we might not actually need this

    TODO: not sure, that we can actually expect those elements to be
    set (at least both of them) (GDI_AssociateEdge with index lockup)
   */
  GDA_Node* origin_elist_ptr;
  GDA_Node* target_elist_ptr;
  /**
    pointer to the two vertices, to which the edge belongs to

    TODO: not sure, that we can actually expect those pointers
    to be set (at least both of them) (GDI_AssociateEdge with
    index lockup)
   */
  GDI_VertexHolder origin;
  GDI_VertexHolder target;
  /**
    pointer to the transaction that the edge object is part of
   */
  GDI_Transaction transaction;
  /**
    flag to indicate, whether the edge is marked for deletion

    true  = marked for deletion
    false = edge will remain in the database
   */
  bool delete_flag;
  /**
    flag to indicate, whether the edge was changed during the
    transaction

    true  = edge was changed
    false = only read access
   */
  bool write_flag;
  /**
    offset in which the lightweight representation of this edge gets stored
    in the origin respectively in the target vertex
    used for further manipulating the lightweight representation of this edge
   */
  uint32_t origin_lightweight_edge_offset;
  uint32_t target_lightweight_edge_offset;
} GDI_EdgeHolder_desc_t;

typedef GDI_EdgeHolder_desc_t* GDI_EdgeHolder;


typedef GDA_DPointer GDI_Vertex_uid;

typedef GDA_Edge_uid GDI_Edge_uid;


typedef struct GDA_Init_params_struct {
  /**
    MPI communicator that identifies the processes
    that constitute the database backend (MPI handle)
   */
  MPI_Comm comm;
  /**
    size for storage in Bytes (non-negative integer)
   */
  MPI_Aint memory_size;
  /**
    size of the blocks used in the database (in Bytes)
   */
  uint32_t block_size;
} GDA_Init_params;


/**
  GDI function prototypes

  label functions need to be placed here, because otherwise we have
  circular dependencies between the header files
 */

/**
  label function prototypes
 */
int GDI_CreateLabel( const char* name, GDI_Database graph_db, GDI_Label* label );
int GDI_FreeLabel( GDI_Label* label );
int GDI_UpdateLabel( const char* name, GDI_Label label );
int GDI_GetLabelFromName( GDI_Label* label, const char* name, GDI_Database graph_db );
int GDI_GetNameOfLabel( char* name, size_t length, size_t* resultlength, GDI_Label label);
int GDI_GetAllLabelsOfDatabase( GDI_Label array_of_labels[], size_t count, size_t* resultcount, GDI_Database graph_db );

/**
  constraint function prototypes
 */
int GDI_CreateConstraint( GDI_Database graph_db, GDI_Constraint* constraint );
int GDI_FreeConstraint( GDI_Constraint* constraint );
int GDI_GetAllConstraintsOfDatabase( GDI_Constraint array_of_constraints[],
                                      size_t count, size_t* resultcount, GDI_Database graph_db );
int GDI_IsConstraintStale( int* staleness, GDI_Constraint constraint );
int GDI_CreateSubconstraint( GDI_Database graph_db, GDI_Subconstraint* subconstraint );
int GDI_FreeSubconstraint( GDI_Subconstraint* subconstraint );
int GDI_GetAllSubconstraintsOfDatabase( GDI_Subconstraint array_of_subconstraints[], size_t count,
                                       size_t* resultcount, GDI_Database graph_db );
int GDI_IsSubconstraintStale( int* staleness, GDI_Subconstraint subconstraint );
int GDI_AddSubconstraintToConstraint( GDI_Subconstraint subconstraint,
                                      GDI_Constraint constraint );
int GDI_GetAllSubconstraintsOfConstraint( GDI_Subconstraint array_of_subconstraints[],  size_t count,
                                          size_t* resultcount, GDI_Constraint constraint );
int GDI_AddLabelConditionToSubconstraint( GDI_Label label, GDI_Op op, GDI_Subconstraint subconstraint );
int GDI_GetAllLabelConditionsFromSubconstraint( GDI_Label array_of_labels[],
                                                GDI_Op array_of_ops[], size_t count, size_t* resultcount,
                                                GDI_Subconstraint subconstraint );
int GDI_AddPropertyConditionToSubconstraint( GDI_PropertyType ptype, GDI_Op op, void* value,
                                             size_t count, GDI_Subconstraint subconstraint );
int GDI_GetAllPropertyTypesOfSubconstraint( GDI_PropertyType array_of_ptypes[],
                                            size_t count, size_t* resultcount, GDI_Subconstraint subconstraint );
int GDI_GetAllPropertyTypesOfSubconstraint( GDI_PropertyType array_of_ptypes[],
                                            size_t count, size_t* resultcount, GDI_Subconstraint subconstraint );
int GDI_GetPropertyConditionsOfSubconstraint( void* buf, size_t buf_count,
                            size_t* buf_resultcount, size_t array_of_offsets[], GDI_Op array_of_ops[],
                            size_t offset_count, size_t* offset_resultcount, GDI_PropertyType ptype,
                            GDI_Subconstraint subconstraint );

/**
  property type function prototypes
 */
int GDI_CreatePropertyType( const char* name, int etype, GDI_Datatype dtype,
                            int stype, size_t count, GDI_Database graph_db,
                            GDI_PropertyType* ptype );
int GDI_FreePropertyType( GDI_PropertyType* ptype );
int GDI_UpdatePropertyType( const char* name, int etype, GDI_Datatype dtype,
                            int stype, size_t count, const void* default_value,
                            GDI_PropertyType ptype );
int GDI_GetPropertyTypeFromName( GDI_PropertyType* ptype, const char* name,
                                 GDI_Database graph_db );
int GDI_GetAllPropertyTypesOfDatabase( GDI_PropertyType array_of_ptypes[],
                                       size_t count, size_t* resultcount, GDI_Database graph_db );
int GDI_GetNameOfPropertyType( char* name, size_t length, size_t* resultlength,
                               GDI_PropertyType ptype );
int GDI_GetEntityTypeOfPropertyType( int* etype, GDI_PropertyType ptype );
int GDI_GetDatatypeOfPropertyType( GDI_Datatype* dtype, GDI_PropertyType ptype );
int GDI_GetSizeLimitOfPropertyType( int* stype, size_t* count,
                                    GDI_PropertyType ptype );

/**
  transaction function prototypes
 */
int GDI_StartTransaction( GDI_Database graph_db, GDI_Transaction* transaction );
int GDI_CloseTransaction( GDI_Transaction* transaction, int ctype );

int GDI_StartCollectiveTransaction( GDI_Database graph_db, GDI_Transaction* transaction );
int GDI_CloseCollectiveTransaction( GDI_Transaction* transaction, int ctype );

int GDI_GetAllTransactionsOfDatabase( GDI_Transaction array_of_transactions[], size_t count, size_t* resultcount, GDI_Database graph_db );
int GDI_GetTypeOfTransaction( int* ttype, GDI_Transaction transaction );

/**
  vertex function prototypes
 */
int GDI_CreateVertex( const void* external_id, size_t size, GDI_Transaction transaction, GDI_VertexHolder* vertex );
int GDI_AssociateVertex( GDI_Vertex_uid internal_uid, GDI_Transaction transaction, GDI_VertexHolder* vertex );
int GDI_FreeVertex( GDI_VertexHolder* vertex );
int GDI_GetEdgesOfVertex( GDI_Edge_uid array_of_uids[], size_t count, size_t* resultcount, GDI_Constraint constraint, int edge_orientation, GDI_VertexHolder vertex );

int GDI_AddLabelToVertex( GDI_Label label, GDI_VertexHolder vertex );
int GDI_RemoveLabelFromVertex( GDI_Label label, GDI_VertexHolder vertex );
int GDI_GetAllLabelsOfVertex( GDI_Label array_of_labels[], size_t count, size_t* resultcount, GDI_VertexHolder vertex );

int GDI_AddPropertyToVertex( const void* value, size_t count, GDI_PropertyType ptype, GDI_VertexHolder vertex );
int GDI_GetAllPropertyTypesOfVertex( GDI_PropertyType array_of_ptypes[], size_t count, size_t* resultcount, GDI_VertexHolder vertex );
int GDI_GetPropertiesOfVertex( void* buf, size_t buf_count, size_t* buf_resultcount, size_t array_of_offsets[], size_t offset_count,
  size_t* offset_resultcount, GDI_PropertyType ptype, GDI_VertexHolder vertex );
int GDI_RemovePropertiesFromVertex( GDI_PropertyType ptype, GDI_VertexHolder vertex );
int GDI_RemoveSpecificPropertyFromVertex( const void* value, size_t count, GDI_PropertyType ptype, GDI_VertexHolder vertex );
int GDI_UpdatePropertyOfVertex( const void* value, size_t count, GDI_PropertyType ptype, GDI_VertexHolder vertex );
int GDI_UpdateSpecificPropertyOfVertex( const void* old_value, size_t old_count, const void* new_value, size_t new_count, GDI_PropertyType ptype, GDI_VertexHolder vertex );
int GDI_SetPropertyOfVertex( const void* value, size_t count, GDI_PropertyType ptype, GDI_VertexHolder vertex );

/**
  edge function prototypes
 */
int GDI_CreateEdge( int dtype, GDI_VertexHolder origin, GDI_VertexHolder target, GDI_EdgeHolder* edge );
int GDI_FreeEdge( GDI_EdgeHolder* edge );

int GDI_GetVerticesOfEdge( GDI_Vertex_uid* origin_uid, GDI_Vertex_uid* target_uid, GDI_EdgeHolder edge );
int GDI_GetNeighborVerticesOfVertex( GDI_Vertex_uid array_of_uids[], size_t count, size_t* resultcount, GDI_Constraint constraint, int edge_orientation, GDI_VertexHolder vertex );
int GDI_GetDirectionTypeOfEdge( int* dtype, GDI_EdgeHolder edge );
int GDI_SetOriginVertexOfEdge( GDI_VertexHolder origin_vertex, GDI_EdgeHolder edge );
int GDI_SetTargetVertexOfEdge( GDI_VertexHolder target_vertex, GDI_EdgeHolder edge );
int GDI_SetDirectionTypeOfEdge( int dtype, GDI_EdgeHolder edge );

int GDI_AddLabelToEdge( GDI_Label label, GDI_EdgeHolder edge );
int GDI_RemoveLabelFromEdge( GDI_Label label, GDI_EdgeHolder edge );
int GDI_GetAllLabelsOfEdge( GDI_Label array_of_labels[], size_t count, size_t* resultcount, GDI_EdgeHolder edge );

int GDI_AddPropertyToEdge( const void* value, size_t count, GDI_PropertyType ptype, GDI_EdgeHolder edge );
int GDI_RemovePropertiesFromEdge( GDI_PropertyType ptype, GDI_EdgeHolder edge );
int GDI_RemoveSpecificPropertyFromEdge( const void* value, size_t count, GDI_PropertyType ptype, GDI_EdgeHolder edge );
int GDI_UpdatePropertyOfEdge( const void* value, size_t count, GDI_PropertyType ptype, GDI_EdgeHolder edge );
int GDI_UpdateSpecificPropertyOfEdge( const void* old_value, size_t old_count, const void* new_value, size_t new_count, GDI_PropertyType ptype, GDI_EdgeHolder edge );
int GDI_SetPropertyOfEdge( const void* value, size_t count, GDI_PropertyType ptype, GDI_EdgeHolder edge );

/**
  index function prototypes
 */
int GDI_TranslateVertexID( bool* found_flag, GDI_Vertex_uid* internal_uid, GDI_Label label, const void* external_id, size_t size, GDI_Transaction transaction );

/**
  database function prototypes
 */
int GDI_Init( int *argc, char ***argv );
int GDI_Finalize();

int GDI_CreateDatabase( void* params, size_t size, GDI_Database* graph_db );
int GDI_FreeDatabase( GDI_Database* graph_db );

#endif // __GDI_H
