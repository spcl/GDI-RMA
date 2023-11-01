include ../../../Makefile.inc

#maybe include Makefile.inc from higher-level directory
CCFLAGS = -fcommon -Drestrict=__restrict__ -O3 #-DDEBUG -g

OBJS = \
	graph_generator.o \
	splittable_mrg.o \
	utils.o

# clear out all suffixes
.SUFFIXES:
# list only those we use
.SUFFIXES: .o .c

# some suffix rules
.c.o:
	$(CC) $(CCFLAGS) -c $<

.PHONY: all
all: libgraph500.a

.PHONY: clean
clean:
	rm -f $(OBJS) libgraph500.a

libgraph500.a: $(OBJS)
	ar -r libgraph500.a $(OBJS)
	ranlib libgraph500.a

graph_generator.c: graph_generator.h splittable_mrg.h user_settings.h

graph_generator.h: user_settings.h

make_graph.h: graph_generator.h

mod_arith.h: mod_arith_64bit.h user_settings.h

mrg_transitions.c: splittable_mrg.h

splittable_mrg.c: mod_arith.h splittable_mrg.h

utils.c: graph_generator.h splittable_mrg.h utils.h

utils.h: splittable_mrg.h
