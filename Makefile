CXX := g++
CC := gcc
CXXFLAGS := -O3 -Wall -Wextra -std=c++17 -MMD -MP
CFLAGS := -O3 -Wall -Wextra -MMD -MP

SRC_DIR := src
TEST_DIR := tests
BENCH_DIR := bench
BUILD_DIR := build

SRCS := $(wildcard $(SRC_DIR)/*.cpp)
OBJS := $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SRCS))

TEST_SRCS := $(wildcard $(TEST_DIR)/*.cpp)
TEST_OBJS := $(patsubst $(TEST_DIR)/%.cpp, $(BUILD_DIR)/tests/%.o, $(TEST_SRCS))
TEST_BIN := $(BUILD_DIR)/test_runner

BENCH_SRCS := $(wildcard $(BENCH_DIR)/*.cpp)
BENCH_OBJS := $(patsubst $(BENCH_DIR)/%.cpp, $(BUILD_DIR)/bench/%.o, $(BENCH_SRCS))
BENCH_BIN := $(BUILD_DIR)/benchmark_runner

LIB_TARGET := $(BUILD_DIR)/libtitan_raft.a

-include $(OBJS:.o=.d) $(TEST_OBJS:.o=.d) $(BENCH_OBJS:.o=.d)

all: build

build: $(LIB_TARGET)

$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)
	@mkdir -p $(BUILD_DIR)/tests
	@mkdir -p $(BUILD_DIR)/bench

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(LIB_TARGET): $(OBJS) | $(BUILD_DIR)
	@if [ -n "$(OBJS)" ]; then \
		ar rcs $@ $(OBJS); \
	else \
		touch $@; \
	fi

$(BUILD_DIR)/tests/%.o: $(TEST_DIR)/%.cpp | $(BUILD_DIR)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -I$(SRC_DIR) -c $< -o $@

$(TEST_BIN): $(LIB_TARGET) $(TEST_OBJS)
	@if [ -n "$(TEST_OBJS)" ]; then \
		$(CXX) $(CXXFLAGS) $(TEST_OBJS) $(LIB_TARGET) -o $@; \
	else \
		touch $@; \
	fi

test: $(TEST_BIN)
	@if [ -x "$(TEST_BIN)" ]; then \
		$(TEST_BIN); \
	elif [ -f "$(TEST_BIN)" ]; then \
		echo "Tests built (empty test suite)."; \
	else \
		echo "No tests found."; \
	fi

$(BUILD_DIR)/bench/%.o: $(BENCH_DIR)/%.cpp | $(BUILD_DIR)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -I$(SRC_DIR) -c $< -o $@

$(BENCH_BIN): $(LIB_TARGET) $(BENCH_OBJS)
	@if [ -n "$(BENCH_OBJS)" ]; then \
		$(CXX) $(CXXFLAGS) $(BENCH_OBJS) $(LIB_TARGET) -o $@; \
	else \
		touch $@; \
	fi

benchmark: $(BENCH_BIN)
	@if [ -x "$(BENCH_BIN)" ]; then \
		$(BENCH_BIN); \
	elif [ -f "$(BENCH_BIN)" ]; then \
		echo "Benchmarks built (empty benchmark suite)."; \
	else \
		echo "No benchmarks found."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)

.PHONY: all build test benchmark clean