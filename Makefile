CXX      ?= clang++
CXXFLAGS ?= -std=c++17 -O2 -g -Wall -Wextra -pthread -I/opt/homebrew/include
LDFLAGS  ?= -L/opt/homebrew/lib -pthread

BIN := bin/rate-limiter
SRC := $(wildcard src/*.cpp)
OBJ := $(SRC:.cpp=.o)

all: $(BIN)

$(BIN): $(OBJ) | bin
	$(CXX) $(OBJ) $(LDFLAGS) -o $@

src/%.o: src/%.cpp src/*.h | bin
	$(CXX) $(CXXFLAGS) -c $< -o $@

bin:
	mkdir -p bin

clean:
	rm -rf bin src/*.o

.PHONY: all clean
