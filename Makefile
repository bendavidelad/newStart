CC=g++
RANLIB=ranlib

LIBSRC=MapReduceClient.h MapReduceFramework.cpp MapReduceFramework.h MapReduceClientUser.cpp MapReduceClientUser.h MapReduceSearch.cpp MapReduceSearch.h

LIBOBJ=MapReduceFramework.o MapReduceClientUser.o MapReduceSearch.o
EXTRADELETE=libMapReduceFramework.a .MapReduceFramework.log Search.o

INCS=-I.
CFLAGS = -Wall -Wextra -g $(INCS)
LOADLIBES = -L./ 
LFLAGS = -o

TAR=tar
TARFLAGS=-cvf
TARNAME=ex3.tar
TARSRCS=Makefile README MapReduceFramework.cpp MapReduceClientUser.cpp MapReduceClientUser.h MapReduceSearch.cpp MapReduceSearch.h Search.cpp

all: Search
 
MapReduceFramework.a: $(LIBOBJ)
	$(AR) $(ARFLAGS) $@ $^
	$(RANLIB) $@

Search: Search.o MapReduceFramework.a
	$(CC) $(CFLAGS) $^ $(LOADLIBES) MapReduceFramework.a -o $@ -lpthread

Search.o: Search.cpp
	$(CC) -c $^ -o $@

clean:
	$(RM) $(TARGETS) $(OBJ) $(LIBOBJ) $(EXTRADELETE) *~ *core

depend:
	makedepend -- $(CFLAGS) -- $(SRC) $(LIBSRC)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)

test: Test
	Test

Test: test.o MapReduceFramework.a
	$(CC) $^ $(LOADLIBES) MapReduceFramework.a -o $@ -lpthread

test.o : test.cpp
	$(CC) -c $^ -o $@

