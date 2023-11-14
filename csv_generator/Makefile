CC=mpicc
CXX=mpic++

CCFLAGS=-std=c99 -O3 -Wall -Wextra -DNDEBUG
CXXFLAGS=-std=c++11 -O3 -Wall -Wextra -DNDEBUG

GRAPH500=../benchmarks/graph500-3.0.0/src

LIBS+=-L$(GRAPH500) -lgraph500
INC+=-I$(GRAPH500)

OBJS = \
	data_scheme_1.o \
	graph.o \
	lpg_graph500_csv.o \
	main.o

EXE = \
	lpg_graph500_csv

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
	rm -rf $(OBJS) $(EXE)

.PHONY: distclean
distclean: clean
	make -C $(GRAPH500) clean

$(GRAPH500)/libgraph500.a:
	make -C $(GRAPH500) libgraph500.a

lpg_graph500_csv: $(OBJS) $(GRAPH500)/libgraph500.a
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) $(LIBS)

data_scheme_1.o: data_scheme_1.c data_scheme_1.h

graph.o: data_scheme_1.h

lpg_graph500_csv.o: lpg_graph500_csv.c data_scheme_1.h $(GRAPH500)/make_graph.h $(GRAPH500)/utils.h

main.o: main.cpp command_line.h graph.h lpg_graph500_csv.h
