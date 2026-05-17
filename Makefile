CXX ?= g++

# ========== Build mode ==========
MODE ?= debug

COMMON_FLAGS := -std=c++11 -Wall -Wextra -Ithird_party -Ithird_party/spdlog/include -Isrc -pthread

ifeq ($(MODE),release)
	CXXFLAGS := $(COMMON_FLAGS) -O3 -DNDEBUG
	BUILD_ROOT := build/release
else
	CXXFLAGS := $(COMMON_FLAGS) -g -O0 -fsanitize=address
	BUILD_ROOT := build/debug
endif

# ========== Directories ==========
SRC_DIR   := src
APPS_DIR  := apps
TEST_DIR  := tests

# ========== Core library (src/) ==========
# 当前 src 基本都是 header-only；这里预留对 .cc/.cpp 的支持，后续把实现拆到 .cc 时不用再改 Makefile
CORE_SRCS := $(shell find $(SRC_DIR) -type f \( -name '*.cc' -o -name '*.cpp' \) 2>/dev/null)
CORE_OBJS := \
  $(patsubst %.cc,$(BUILD_ROOT)/obj/%.o,$(filter %.cc,$(CORE_SRCS))) \
  $(patsubst %.cpp,$(BUILD_ROOT)/obj/%.o,$(filter %.cpp,$(CORE_SRCS)))

# ========== Apps ==========
APP_SRCS := $(sort $(wildcard $(APPS_DIR)/*/*.cc) $(wildcard $(APPS_DIR)/*/*.cpp))
APP_BINS := \
  $(patsubst $(APPS_DIR)/%.cc,$(BUILD_ROOT)/apps/%,$(filter %.cc,$(APP_SRCS))) \
  $(patsubst $(APPS_DIR)/%.cpp,$(BUILD_ROOT)/apps/%,$(filter %.cpp,$(APP_SRCS)))

# ========== Tests ==========
TEST_SRCS_ALL := $(sort $(wildcard $(TEST_DIR)/*.cc) $(wildcard $(TEST_DIR)/*.cpp))

# http_parse_test.cc 依赖 experiments/Http.h 的旧实现；在你把测试迁到新 HTTP 栈前，先默认不编它
TEST_SRCS := $(filter-out $(TEST_DIR)/http_parse_test.cc,$(TEST_SRCS_ALL))

TEST_BINS := \
  $(patsubst $(TEST_DIR)/%.cc,$(BUILD_ROOT)/tests/%,$(filter %.cc,$(TEST_SRCS))) \
  $(patsubst $(TEST_DIR)/%.cpp,$(BUILD_ROOT)/tests/%,$(filter %.cpp,$(TEST_SRCS)))

# ========== Phony targets ==========
.PHONY: all apps tests run-tests run-wrk run-benchmark clean help

all: apps tests

help:
	@echo "Usage:"
	@echo "  make [MODE=debug|release]  # build apps + tests (default: debug)"
	@echo "  make apps [MODE=...]       # build all apps under apps/*/"
	@echo "  make tests [MODE=...]      # build tests under tests/"
	@echo "  make run-tests [MODE=...]  # run scripts/run_tests.sh"
	@echo "  make run-wrk MODE=release  # run scripts/run_wrk.sh"
	@echo "  make run-benchmark MODE=release # run scripts/run_benchmark.sh"
	@echo "  make clean                 # remove build/<mode>"

apps: $(APP_BINS)
tests: $(TEST_BINS)

# ========== Compile core objects ==========
$(BUILD_ROOT)/obj/%.o: %.cc
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -MMD -MP -c -o $@ $<

$(BUILD_ROOT)/obj/%.o: %.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -MMD -MP -c -o $@ $<

# ========== Link apps ==========
$(BUILD_ROOT)/apps/%: $(APPS_DIR)/%.cc $(CORE_OBJS)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -o $@ $< $(CORE_OBJS)

$(BUILD_ROOT)/apps/%: $(APPS_DIR)/%.cpp $(CORE_OBJS)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -o $@ $< $(CORE_OBJS)

# ========== Link tests ==========
$(BUILD_ROOT)/tests/%: $(TEST_DIR)/%.cc $(CORE_OBJS)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -o $@ $< $(CORE_OBJS)

$(BUILD_ROOT)/tests/%: $(TEST_DIR)/%.cpp $(CORE_OBJS)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -o $@ $< $(CORE_OBJS)

# ========== Run scripts ==========
run-tests: tests
	@chmod +x ./scripts/run_tests.sh
	@./scripts/run_tests.sh $(BUILD_ROOT)

run-wrk: apps
	@chmod +x ./scripts/run_wrk.sh
	@./scripts/run_wrk.sh $(BUILD_ROOT)

run-benchmark: tests apps
	@chmod +x ./scripts/run_benchmark.sh
	@./scripts/run_benchmark.sh $(BUILD_ROOT)

clean:
	rm -rf $(BUILD_ROOT)

# deps (only core objs generate .d right now)
-include $(CORE_OBJS:.o=.d)