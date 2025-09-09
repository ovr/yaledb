CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2
ROCKSDB_INCLUDE = /usr/local/include
ROCKSDB_LIB = /usr/local/lib

# Try to detect RocksDB installation
ROCKSDB_INSTALLED := $(shell pkg-config --exists rocksdb && echo "yes" || echo "no")

ifeq ($(ROCKSDB_INSTALLED), yes)
    # Use pkg-config if available
    CXXFLAGS += $(shell pkg-config --cflags rocksdb)
    LDFLAGS = $(shell pkg-config --libs rocksdb)
else
    # Fallback to manual linking
    CXXFLAGS += -I$(ROCKSDB_INCLUDE)
    LDFLAGS = -L$(ROCKSDB_LIB) -lrocksdb -lpthread -lz -lbz2 -llz4 -lzstd -lsnappy
endif

TARGET = generate_fixtures
SOURCE = generate_fixtures.cpp

.PHONY: all run clean install-deps help

all: $(TARGET)

$(TARGET): check-deps $(SOURCE)
	$(CXX) $(CXXFLAGS) -o $@ $(SOURCE) $(LDFLAGS)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(TARGET)
	rm -f sst_files/*.sst

install-deps:
	@echo "Installing RocksDB dependencies..."
	@echo "On macOS: brew install rocksdb"
	@echo "On Ubuntu: sudo apt-get install librocksdb-dev"
	@echo "On CentOS/RHEL: sudo yum install rocksdb-devel"

help:
	@echo "Available targets:"
	@echo "  all        - Build the fixture generator"
	@echo "  run        - Build and run the generator to create SST fixtures"
	@echo "  clean      - Remove generated files"
	@echo "  install-deps - Show instructions for installing RocksDB"
	@echo "  help       - Show this help message"

# Check if RocksDB is available before building
check-deps:
	@command -v pkg-config >/dev/null 2>&1 || { echo >&2 "pkg-config is required but not installed. Aborting."; exit 1; }
ifeq ($(ROCKSDB_INSTALLED), no)
	@echo >&2 "RocksDB development package not found. Please install RocksDB."; exit 1
endif