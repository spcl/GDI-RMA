# Benchmarks

This directory contains the source code files that were used to benchmark
GDI-RMA and to produce the figures presented in the paper The Graph Database
Interface: Scaling Online Transactional and Analytical Graph Workloads to
Hundreds of Thousands of Cores.

## Setup Guide

The following instructions assume that the GDI-RMA library was successfully
compiled.

### LibSciBench Library

The benchmark codes uses the
[LibSciBench](https://spcl.inf.ethz.ch/Research/Performance/LibLSB/) library for
its time measurements. Each rank stores its measurements in a separate file:
`lsb.*.r{rank number}`.

``` bash
% git clone https://github.com/spcl/liblsb.git
% cd liblsb
% git checkout 91d073d7420b4c4d8d30dab202166f9de7d65a10
% mkdir build
% module switch PrgEnv-cray PrgEnv-gnu
  - necessary step on Piz Daint
% MPICC=cc MPICXX=CC ./configure --prefix=$(pwd)/build/ --with-mpi --without-papi
  - set MPICC and MPICXX to the MPI C and C++ compilers of your system
% make
% make install
% module switch PrgEnv-gnu PrgEnv-cray
  - necessary step on Piz Daint
```

### Graph500 Kronecker Module

Please set CC to the C compiler of your system in the first line of the
respective [Makefile](graph500-3.0.0/src/Makefile)

### Compilation

Please update the C and C++ compilers (lines 1 and 2) and their respective flags
(lines 4 and 5) to the needs of your system at the beginning of the
[Makefile](Makefile). Please also ensure that the location of your GDI-RMA and
LibSciBench installations (lines 8 and 9) are correct. If you compiled GDI-RMA
for the use with
[foMPI](https://spcl.inf.ethz.ch/Research/Parallel_Programming/foMPI/), please
enable the respective parameters in lines 12 to 16 and set the path to your
foMPI installation in line 12. Afterwards calling `make` should result in the
creation of a number of executables.

## Execution

### Executables

There is a separate executable for each workload:
* Breadth-first Search (BFS) and k-hop: bench_gdi.bfs
* Community Detection using Label Propagation (CDLP): bench_gdi.cdlp
* Graph Neural Networks (GNN): bench_gdi.gnn
* LDBC Business Intelligence query 2: bench_gdi.bi
* Local Cluster Coefficient (LCC): bench_gdi.lcc
* Online Transactional Processing (OLTP):
  * latency:
    * LinkBench (lb): bench_gdi.oltp.lb.lat
    * read intensive (ri): bench_gdi.oltp.ri.lat
    * read mostly (rm): bench_gdi.oltp.rm.lat
    * write intensive (wi): bench_gdi.oltp.wi.lat
  * throughput:
    * LinkBench (lb): bench_gdi.oltp.lb.tp
    * read intensive (ri): bench_gdi.oltp.ri.tp
    * read mostly (rm): bench_gdi.oltp.rm.tp
    * write intensive (wi): bench_gdi.oltp.wi.tp
* PageRank (PR): bench_gdi.pr
* Weakly Connected Components (WCC): bench_gdi.wcc

Please see the paper for a detailed breakdown of the queries types of each OLTP
workload.

### Command Line Interface

All executables have the same command line interface, however not every
parameter is significant for every executable.

```
GDI Benchmark
 -b <bsize>   : block size                                                 [512]
 -d           : use directed edges                                       [false]
 -e <efactor> : edge factor                                                 [16]
 -f <file>    : load graph from file
 -i <iter>    : iterations for CDLP/PageRank/WCC                             [5]
 -l <layers>  : layers for GNN                                               [5]
 -m <msize>   : memory size per process                                   [4096]
 -n <verts>   : number of vertices                                           [0]
 -o           : vertex UIDs start at one                                 [false]
 -r <rcount>  : number of queries                                          [200]
 -s <scale>   : log_2(# vertices)                                            [3]
 -t <time>    : duration to run Linkbench queries                            [5]
 -v <vector>  : size of feature vector for GNN                             [500]
 -w <damp>    : damping factor for PageRank                               [0.85]
 -h           : print this help message
```

`-b` sets the size of the individual blocks of the Blocked
Graph Data Layout (BGDL) level and `-m` specifies the amount of main memory that each
process makes available for the storage of the graph databases.

It is possible to either use Kronecker graphs or graphs loaded from a file in
the edgelist format as input for the structural graph of the benchmark. `-d`
instructs the use of directed edges. Certain online analytical processing (OLAP)
workloads, namely GNN and PageRank, expect the use of directed edges, whereas
all other workloads use undirected edges.
`-f`, `-n` and `-o` are only significant, when the graph is loaded from a file.
`-f` specifies the path to the edgelist file. `-n` is necessary, when loading
the graph from a file, to inform the benchmark during the graph database
creation on how many vertices should be present. `-o` allows to specify, whether
the vertex UID enumeration should start at one instead of zero.
The relevant parameters for the Kronecker graph generation are `-s` and `-e`.
`-s <scale>` varies the number of vertices for the Kronecker graph generation:
`2^<scale>`. The edge factor `-e` is the average vertex degree. The resulting
Kronecker graph will have exactly `2^<scale> * edge factor` edges.

`-i` specifies the number of iterations for the CDLP, PR and WCC workloads. `-w`
only pertains to the PR workload and is the dampening factor. For the GNN
workload one can set the number of layers (`-l`) and the size of the feature
vectors (`-v`). `-r` adjusts the amount of the OLTP queries. The first 100
queries are used as warm up and are not reported in the resulting output files.
BFS and k-hop expect the root vertices for their queries in the file
`bfs_root.txt` and the number of hops for the k-hop query is hardcoded to 2, 3
and 4.

### Data Scheme

The data scheme for the Labeled Property Graph (LPG) is hardcoded in the files
`data_scheme_1.{c|h}` and the function `createGraphDatabase` in the file
`graph.c`.

There are five different labels for vertices. The percentage indicates the ratio
of the vertices of that type in comparison to the total amount of vertices.
* Company: 5%
* Person: 15%
* Place: 5%
* Project: 50%
* Resource: 25%


### Additional notes

Before executing any benchmark, one should ensure that the LibSciBench shared
library can be found by using the following command:
`export LD_LIBRARY_PATH="{PATH to installation}liblsb/build/lib:$LD_LIBRARY_PATH"`.

Each executable generates a result file for each rank during the benchmark,
which are prefixed with `lsb.gdi_`, followed by the abbreviation of the workload
and the rank number `.r{rank number}`.

## Plotting

We provide Python scripts to visualize the experimental results in the directory
[plots](plots). The execution of `python3 <script name>` will result in a PDF
file, whose name matches the script name: `<script name>.pdf`. The scripts are
intended to produce similar plots as presented in the paper, but should provide
a reference for plotting only a single workload/configuration.
