# CSV-Generator for Labeled Property Graphs (LPGs)

This directory contains the source files for a graph generator written in C that
creates the same graph used in the GDI-RMA evaluation and writes that graph into
CSV files. These files were used as input for JanusGraph and Neo4j.

The graph generator can be executed in a distributed environment with the help
of MPI. It follows the same logic as the in-memory graph generator and stores
the same graph dataset in a number of CSV files (one for each vertex and edge
label).

The output files for the edges are:
* `edges_canBeUsedWith.csv`
* `edges_canUse.csv`
* `edges_foundAt.csv`
* `edges_hasBranchesAt.csv`
* `edges_impacts.csv`
* `edges_inBusinessWith.csv`
* `edges_influences.csv`
* `edges_inVicinityOf.csv`
* `edges_isPartOf.csv`
* `edges_knows.csv`
* `edges_needs.csv`
* `edges_supports.csv`
* `edges_uses.csv`
* `edges_wasIn.csv`
* `edges_worksAt.csv`

The output files for the vertices are:
* `nodes_Company.csv`
* `nodes_Person.csv`
* `nodes_Place.csv`
* `nodes_Project.csv`
* `nodes_Resource.csv`

The edge files consist of a header in the first line: `:START_ID(VERTEX LABEL
1),:END_ID(VERTEX LABEL 2)`, where `VERTEX LABEL 1` identifies the labels of the
origin vertices and `VERTEX LABEL 2` identifies the labels of the target
vertices. The header is followed by the edges on separate lines. Each line
consists of the application-level IDs of the origin and target vertices of the
edge, which are separated by a comma.

Similarly the vertex files have a header in the first line: `<vertex
label>Id:ID(VERTEX LABEL)`, which identifies the application-level ID property,
followed by the comma-separated names of additional properties these vertices
have. `<vertex label>ID` is the property name, where `<vertex label>` is written
in lowercase letters. `VERTEX LABEL` is the label of the vertices in the
respective files. Each following line represents the properties of a single
vertex. These properties are separated by commas.

The convention for these files follow the requirements for Neo4j.


## Setup Guide

The source code uses the Kronecker module from Graph500 source code located in
`GDI-RMA/benchmarks/graph500-3.0.0`.

Please update the C and C++ compilers (lines 1 and 2) and their respective flags
(lines 4 and 5) to the needs of your system at the beginning of the
[Makefile](Makefile). The files are generated distributed, so your system should
support MPI. Afterwards calling `make` should result in the creation of
`lpg_graph500_csv`.


### Command Line Interface

```
LPG-based Graph500 CSV Generator
 -e <efactor> : edge factor                                                 [16]
 -f <file>    : load graph from file
 -n <verts>   : number of vertices                                           [0]
 -o           : vertex UIDs start at one                                 [false]
 -p <prefix>  : prefix for output file names
 -s <scale>   : log_2(# vertices)                                            [3]
 -h           : print this help message
```

The graph structure for graph generator can either be read from an edgelist file
or be generated as Kronecker graph with the help of a Graph500 module. `-n` and
`-o` are only relevant, when the edge data is loaded from an edgelist file. The
relevant parameters for the Kronecker graph generation are `-s` and `-e`. `-s
<scale>` varies the number of vertices for the Kronecker graph generation:
`2^<scale>`. The edge factor `-e` is the average vertex degree. The resulting
Kronecker graph will have exactly `2^<scale> * edge factor` edges. -p allows to
set a prefix for the CSV files, so that multiple datasets in the same directory
can be identified.
