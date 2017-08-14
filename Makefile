CXX = mpicxx

OPTFLAGS = -O2
CXXFLAGS = $(OPTFLAGS) -g

DEPS = utils.h
OBJ = mpi_io_mem_usage.o utils.o

all: mpiio

%.o: %.cpp $(DEPS)
	$(CXX) -c -o $@ $< $(CXXFLAGS)

mpiio: $(OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^

.PHONY: clean

clean:
	rm -f mpiio *.o *~
