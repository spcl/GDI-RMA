# GDI-RMA (GDA)

<p align="center">
  <img src="paper/pics/gda_2.svg">
</p>

GDI-RMA is a reference implementation of the Graph Database Interface
specification.

## Setup Guide

The library is written in C and uses MPI for its communication needs
(one-sided/collectives). Please update the C compiler and its flags to the needs
of your system at the beginning of the [Makefile](Makefile) (variables `CC` and
`CFLAGS`). Afterwards calling `make` should result in the creation of the
library file `libgdi.a`.

If you intend to use
[foMPI](https://spcl.inf.ethz.ch/Research/Parallel_Programming/foMPI/), please
enable the related parameters in lines 6 to 9 of the Makefile and set the path
to your foMPI installation in line 6.

## Quickstart

The following source code provides a minimal example, that creates a simple
graph database with two vertices that are connected through a directed edge.
Error checking is omitted for clarity.

```C
#include <stdio.h>

#include "gdi.h"

int main( int argc, char* argv[] ) {
  /* initialization */
  MPI_Init( &argc, &argv );
  GDI_Init( &argc, &argv );

  /* graph database creation */
  GDI_Database db;
  GDA_Init_params parameters;
  parameters.block_size = 256;
  parameters.memory_size = 4096;
  parameters.comm = MPI_COMM_WORLD;
  GDI_CreateDatabase( &parameters, sizeof(GDA_Init_params), &db );

  /* start transaction */
  GDI_Transaction transaction;
  GDI_StartTransaction( db, &transaction );

  /* create two vertices */
  GDI_VertexHolder vert1, vert2;
  uint64_t vertex_id = 1;
  GDI_CreateVertex( &vertex_id, 8, transaction, &vert1 );
  vertex_id = 2;
  GDI_CreateVertex( &vertex_id, 8, transaction, &vert2 );

  /* connect vertices with an edge */
  GDI_EdgeHolder edge;
  GDI_CreateEdge( GDI_EDGE_DIRECTED, vert1, vert2, &edge );

  /* commit transaction to the database */
  int status;
  status = GDI_CloseTransaction( &transaction, GDI_TRANSACTION_COMMIT );
  if (status == GDI_SUCCESS ) {
    printf("Transaction successful.\n");
  }

  /* graph database deallocation and clean up */
  GDI_FreeDatabase( &db );
  GDI_Finalize();
  MPI_Finalize();

  return 0;
}
```
Assuming that the source code is stored in this directory in the file
`min_example.c`, it can be compiled for example with the command `mpicc -o
min_example min_example.c -lgdi` and executed with the command `mpirun -n
<#procs> ./min_example`.

## Implementation Status

GDI-RMA currently only supports lightweight edges, which can be directed or
undirected and can have atmost a single label. The following functions provide
an overview of the status of functions from the Graph Database Interface
specification that are only implemented partially or not at all.

### Not Fully Implemented Functions

The following functions only update the graph metadata, but do **not** affect
the graph data stored in the database.

* GDI_FreeLabel
* GDI_FreePropertyType
* GDI_UpdatePropertyType

### Functions Which Implement Only Input Parsing

* GDI_AddPropertyToEdge
* GDI_RemovePropertiesFromEdge
* GDI_RemoveSpecificPropertyFromEdge
* GDI_SetPropertyOfEdge
* GDI_UpdatePropertyOfEdge
* GDI_UpdateSpecificPropertyOfEdge

### Not Implemented Functions

* GDI_AddLabelsAndPropertyTypesToIndex
* GDI_AddLabelToIndex
* GDI_AddPropertyTypeToIndex
* GDI_AssociateEdge
* GDI_CreateIndex
* GDI_FreeIndex
* GDI_GetAllIndexesOfDatabase
* GDI_GetAllLabelsOfIndex
* GDI_GetAllPropertyTypesOfEdge
* GDI_GetAllPropertyTypesOfIndex
* GDI_GetDecimal
* GDI_GetEdgesOfIndex
* GDI_GetErrorClass
* GDI_GetErrorString
* GDI_GetLocalEdgesOfIndex
* GDI_GetLocalVerticesOfIndex
* GDI_GetPropertiesOfEdge
* GDI_GetTypeOfIndex
* GDI_GetVerticesOfIndex
* GDI_LoadEdgeCSVFile
* GDI_LoadVertexCSVFile
* GDI_LoadVertexPropertiesCSVFile
* GDI_RemoveLabelFromIndex
* GDI_RemoveLabelsAndPropertyTypesFromIndex
* GDI_RemovePropertyTypeFromIndex
* GDI_SetDecimal
