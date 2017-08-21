CXX = mpicxx

OPTFLAGS = -O0
CXXFLAGS = $(OPTFLAGS) -g -I.

DEPS = utils.h
OBJ = main.o utils.o ReportWriter.o reportinglib/driver.o  main.o

all: mpiio

%.o: %.cpp $(DEPS)
	$(CXX) -c -o $@ $< $(CXXFLAGS)

mpiio: $(OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^

.PHONY: clean

clean:
	rm -rf mpiio *.o *~ *.dSYM *.bbp reportinglib/*.o
