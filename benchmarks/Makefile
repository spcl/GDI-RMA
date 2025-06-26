CC=cc
CXX=CC

CCFLAGS=-std=c99 -O3 -fcommon -Wall -Wextra #-DNDEBUG
CXXFLAGS=-std=c++11 -O3 -fcommon -Wall -Wextra #-DNDEBUG

GRAPH500=graph500-3.0.0/src
GDI=../src
LSB=liblsb/build

# foMPI related parameters
# FOMPI=<path to foMPI library>
# INC+=-I$(FOMPI)
# CCFLAGS+=-DRMA_USE_FOMPI
# CXXFLAGS+=-DRMA_USE_FOMPI
# LIBS+=-L$(FOMPI) -lfompi -ldmapp -L/opt/cray/xpmem/default/lib64 -lxpmem

LIBS+=-L$(GRAPH500) -lgraph500 -L$(GDI) -lgdi -L$(LSB)/lib -llsb
INC+=-I$(GRAPH500) -I$(GDI) -I$(LSB)/include

OBJS = \
	benchmark.o \
	data_scheme_1.o \
	graph.o \
	queries.o \
	radix7.o \
	radix7_u32.o

EXE = \
	bench_gdi.bfs \
	bench_gdi.bi \
	bench_gdi.cdlp \
	bench_gdi.gnn \
	bench_gdi.lcc \
	bench_gdi.oltp.lb.lat \
	bench_gdi.oltp.lb.tp \
	bench_gdi.oltp.ri.lat \
	bench_gdi.oltp.ri.tp \
	bench_gdi.oltp.rm.lat \
	bench_gdi.oltp.rm.tp \
	bench_gdi.oltp.wi.lat \
	bench_gdi.oltp.wi.tp \
	bench_gdi.pr \
	bench_gdi.wcc

# clear out all suffixes
.SUFFIXES:
# list only those we use
.SUFFIXES: .o .c .cpp

# some suffix rules
.c.o:
	$(CC) $(CCFLAGS) $(INC) -c $<

.cpp.o:
	$(CXX) $(CXXFLAGS) $(INC) -c $<

.PHONY: all
all: $(EXE)

.PHONY: clean
clean:
	rm -rf $(OBJS) $(EXE) main.*.o benchmark.oltp.*.o
	git status

.PHONY: distclean
distclean: clean
	make -C $(GRAPH500) clean
	git status

$(GRAPH500)/libgraph500.a:
	make -C $(GRAPH500) libgraph500.a

bench_gdi.bfs: $(OBJS) $(GRAPH500)/libgraph500.a main.bfs.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) main.bfs.o $(LIBS)

bench_gdi.bi: $(OBJS) $(GRAPH500)/libgraph500.a main.bi.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) main.bi.o $(LIBS)

bench_gdi.cdlp: $(OBJS) $(GRAPH500)/libgraph500.a main.cdlp.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) main.cdlp.o $(LIBS)

bench_gdi.gnn: $(OBJS) $(GRAPH500)/libgraph500.a main.gnn.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) main.gnn.o $(LIBS)

bench_gdi.lcc: $(OBJS) $(GRAPH500)/libgraph500.a main.lcc.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) main.lcc.o $(LIBS)

bench_gdi.oltp.lb.lat: $(OBJS) $(GRAPH500)/libgraph500.a main.oltp.o benchmark.oltp.lb.lat.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) main.oltp.o benchmark.oltp.lb.lat.o $(LIBS)

bench_gdi.oltp.lb.tp: $(OBJS) $(GRAPH500)/libgraph500.a main.oltp.o benchmark.oltp.lb.tp.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) main.oltp.o benchmark.oltp.lb.tp.o $(LIBS)

bench_gdi.oltp.ri.lat: $(OBJS) $(GRAPH500)/libgraph500.a main.oltp.o benchmark.oltp.ri.lat.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) main.oltp.o benchmark.oltp.ri.lat.o $(LIBS)

bench_gdi.oltp.ri.tp: $(OBJS) $(GRAPH500)/libgraph500.a main.oltp.o benchmark.oltp.ri.tp.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) main.oltp.o benchmark.oltp.ri.tp.o $(LIBS)

bench_gdi.oltp.rm.lat: $(OBJS) $(GRAPH500)/libgraph500.a main.oltp.o benchmark.oltp.rm.lat.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) main.oltp.o benchmark.oltp.rm.lat.o $(LIBS)

bench_gdi.oltp.rm.tp: $(OBJS) $(GRAPH500)/libgraph500.a main.oltp.o benchmark.oltp.rm.tp.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) main.oltp.o benchmark.oltp.rm.tp.o $(LIBS)

bench_gdi.oltp.wi.lat: $(OBJS) $(GRAPH500)/libgraph500.a main.oltp.o benchmark.oltp.wi.lat.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) main.oltp.o benchmark.oltp.wi.lat.o $(LIBS)

bench_gdi.oltp.wi.tp: $(OBJS) $(GRAPH500)/libgraph500.a main.oltp.o benchmark.oltp.wi.tp.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) main.oltp.o benchmark.oltp.wi.tp.o $(LIBS)

bench_gdi.pr: $(OBJS) $(GRAPH500)/libgraph500.a main.pr.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) main.pr.o $(LIBS)

bench_gdi.wcc: $(OBJS) $(GRAPH500)/libgraph500.a main.wcc.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) main.wcc.o $(LIBS)

benchmark.o: benchmark.c data_scheme_1.h queries.h

benchmark.oltp.lb.lat.o: benchmark.oltp.lb.c data_scheme_1.h queries.h
	$(CC) $(CCFLAGS) $(INC) -c $< -o benchmark.oltp.lb.lat.o

benchmark.oltp.lb.tp.o: benchmark.oltp.lb.c data_scheme_1.h queries.h
	$(CC) $(CCFLAGS) -DTHROUGHPUT $(INC) -c $< -o benchmark.oltp.lb.tp.o

benchmark.oltp.ri.lat.o: benchmark.oltp.ri.c data_scheme_1.h queries.h
	$(CC) $(CCFLAGS) $(INC) -c $< -o benchmark.oltp.ri.lat.o

benchmark.oltp.ri.tp.o: benchmark.oltp.ri.c data_scheme_1.h queries.h
	$(CC) $(CCFLAGS) -DTHROUGHPUT $(INC) -c $< -o benchmark.oltp.ri.tp.o

benchmark.oltp.rm.lat.o: benchmark.oltp.rm.c data_scheme_1.h queries.h
	$(CC) $(CCFLAGS) $(INC) -c $< -o benchmark.oltp.rm.lat.o

benchmark.oltp.rm.tp.o: benchmark.oltp.rm.c data_scheme_1.h queries.h
	$(CC) $(CCFLAGS) -DTHROUGHPUT $(INC) -c $< -o benchmark.oltp.rm.tp.o

benchmark.oltp.wi.lat.o: benchmark.oltp.wi.c data_scheme_1.h queries.h
	$(CC) $(CCFLAGS) $(INC) -c $< -o benchmark.oltp.wi.lat.o

benchmark.oltp.wi.tp.o: benchmark.oltp.wi.c data_scheme_1.h queries.h
	$(CC) $(CCFLAGS) -DTHROUGHPUT $(INC) -c $< -o benchmark.oltp.wi.tp.o

data_scheme_1.o: data_scheme_1.c

graph.o: graph.c data_scheme_1.h queries.h $(GRAPH500)/make_graph.h $(GRAPH500)/utils.h

main.bfs.o: main.bfs.cpp benchmark.h command_line.h data_scheme_1.h graph.h

main.bi.o: main.bi.cpp benchmark.h command_line.h data_scheme_1.h graph.h

main.cdlp.o: main.cdlp.cpp benchmark.h command_line.h data_scheme_1.h graph.h

main.gnn.o: main.gnn.cpp benchmark.h command_line.h data_scheme_1.h graph.h

main.lcc.o: main.lcc.cpp benchmark.h command_line.h data_scheme_1.h graph.h

main.pr.o: main.pr.cpp benchmark.h command_line.h data_scheme_1.h graph.h

main.wcc.o: main.wcc.cpp benchmark.h command_line.h data_scheme_1.h graph.h

queries.o: data_scheme_1.h queries.h radix7.h
