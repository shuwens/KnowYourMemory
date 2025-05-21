PREFIX = ../..

CC = g++
LDFLAGS =  -libverbs -lpthread -lrdmacm
CFLAGS += -Wall -std=c++11 -I$(PREFIX)/include  -I$(PREFIX)/src -I$(PREFIX)/external
CXXFLAGS = $(CFLAGS)

BINDIR = ./bin
PLOTDIR = ./plot
SRCPATH = $(PREFIX)/src

SRCS =  $(SRCPATH)/endpoint.cpp $(SRCPATH)/ring_buffer/magic_buffer.cpp

OBJS = $(SRCS:.cpp=.o)

PLOT = $(PLOTDIR)/read.png $(PLOTDIR)/write.png $(PLOTDIR)/atomic.png

all:  ./main.o $(OBJS)
	mkdir -pm 755 $(BINDIR)
	$(CC) $(CFLAGS) -o $(BINDIR)/ringbuffer ./main.o $(OBJS) $(LDFLAGS)

debug: CFLAGS += -DDEBUG -g -O0
debug: ./main.o $(OBJS)
	mkdir -pm 755 $(BINDIR)
	$(CC) $(CFLAGS) -o $(BINDIR)/ringbuffer ./main.o $(OBJS) $(LDFLAGS)
plot: $(PLOT)


clean:
	@echo "##### CLEAN-UP #####"
	-rm -f $(OBJS)
	-rm -f $(BINDIR)/*
	-rm -f ./*.o
	@echo "####################"
	@echo

%.png: %.p $(DATADIR)
	gnuplot $<

%.o: %.cpp 
	$(CC) $(FLAGS) $(CFLAGS) -c -o $@ $<

.DELETE_ON_ERROR:
.PHONY: all clean


