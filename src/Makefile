CC=cc

CCFLAGS=-O3 -Wall -Wextra -fcommon

# foMPI related parameters
# FOMPI=<path to foMPI library>
# CCFLAGS+=-DRMA_USE_FOMPI
# INC+=-I$(FOMPI)
# LIBS+=-L$(FOMPI) -lfompi -ldmapp -L/opt/cray/xpmem/default/lib64 -lxpmem

CCFLAGS+=$(INC)

OBJS = \
	gdi_constraint.o \
	gdi_database.o \
	gdi_datatype.o \
	gdi_edge.o \
	gdi_index.o \
	gdi_init.o \
	gdi_label.o \
	gdi_property_type.o \
	gdi_transaction.o \
	gdi_vertex.o \
	gda_block.o \
	gda_constraint.o \
	gda_datatype.o \
	gda_distributed_hashtable.o \
	gda_dpointer.o \
	gda_hashmap.o \
	gda_label.o \
	gda_lightweight_edges.o \
	gda_list.o \
	gda_lock.o \
	gda_property.o \
	gda_property_type.o \
	gda_utf8.o \
	gda_vector.o \
	gda_vertex.o

# clear out all suffixes
.SUFFIXES:
# list only those we use
.SUFFIXES: .o .c

# some suffix rules
.c.o:
	$(CC) $(CCFLAGS) -c $<

.PHONY: all
all: $(EXE) libgdi.a

.PHONY: clean
clean:
	rm -rf $(OBJS) $(EXE) libgdi.a

libgdi.a: $(OBJS)
	ar -r libgdi.a $(OBJS)
	ranlib libgdi.a

gdi.h: gdi_constraint.h gdi_datatype.h gdi_label.h gdi_property_type.h gda_distributed_hashtable.h gda_dpointer.h gda_edge_uid.h gda_vector.h

gdi_constraint.h: gdi_label.h gdi_operation.h gdi_property_type.h

gdi_constraint.o: gdi_constraint.c gdi.h gda_constraint.h gda_operation.h

gdi_database.o: gdi_database.c gdi.h gda_block.h gda_constraint.h gda_label.h gda_property_type.h

gdi_datatype.o: gdi_datatype.c gdi.h

gdi_edge.o: gdi_edge.c gdi.h gda_lightweight_edges.h gda_lock.h

gdi_index.o: gdi_index.c gdi.h gda_lock.h gda_vertex.h

gdi_init.o: gdi_init.c gdi.h

gdi_label.h: gda_hashmap.h gda_list.h

gdi_label.o: gdi_label.c gdi.h gda_constraint.h gda_utf8.h

gdi_property_type.h: gdi_datatype.h gda_hashmap.h gda_list.h

gdi_property_type.o: gdi_property_type.c gdi.h gda_constraint.h gda_datatype.h gda_utf8.h

gdi_transaction.o: gdi_transaction.c gdi.h gda_block.h gda_dpointer.h gda_lightweight_edges.h gda_lock.h gda_vertex.h

gdi_vertex.o: gdi_vertex.c gdi.h gda_block.h gda_constraint.h gda_dpointer.h gda_edge_uid.h gda_lightweight_edges.h gda_lock.h gda_property.h gda_vertex.h

gda_block.h: gda_dpointer.h

gda_block.o: gda_block.c gdi.h gda_block.h

gda_constraint.o: gda_constraint.c gda_constraint.h

gda_datatype.o: gda_datatype.c gdi.h

gda_distributed_hashtable.h: gda_dpointer.h

gda_distributed_hashtable.o: gda_distributed_hashtable.c gda_distributed_hashtable.h

gda_dpointer.o: gda_dpointer.c gda_dpointer.h

gda_edge_uid.h: gda_dpointer.h

gda_hashmap.o: gda_hashmap.c gda_hashmap.h

gda_label.o: gda_label.c gdi.h

gda_lightweight_edges.h: gdi.h

gda_lightweight_edges.o: gda_lightweight_edges.c gda_lightweight_edges.h

gda_list.o: gda_list.c gda_list.h

gda_lock.h: gdi.h

gda_lock.o: gda_lock.c gda_lock.h rma.h

gda_property.o: gda_property.c gda_property.h gdi.h

gda_property_type.o: gda_property_type.c

gda_utf8.o: gda_utf8.c gda_utf8.h

gda_vector.o: gda_vector.c gda_vector.h

gda_vertex.o: gda_vertex.c gdi.h gda_block.h gda_lightweight_edges.h gda_vertex.h
