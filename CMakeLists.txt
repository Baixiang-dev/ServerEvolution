cmake_minimum_required(VERSION 3.16)
project(ServerDemo LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # 生成 compile_commands.json， vscode会用它做智能提示，跳转，静态分析

# ---------- Build type ----------
# 单配置生成器（Unix Makefiles/Ninja）下默认 Debug
if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type (Debug, Release, RelWithDebInfo, MinSizeRel)" FORCE)
endif()

option(ENABLE_ASAN "Enable AddressSanitizer in Debug builds" ON)

# ---------- Common deps ----------
find_package(Threads REQUIRED)

# ---------- spdlog (precompiled) ----------
# 你需要把完整 spdlog repo clone 到 third_party/spdlog，使其包含 CMakeLists.txt
set(SPDLOG_DIR "${PROJECT_SOURCE_DIR}/third_party/spdlog")

# 关掉 spdlog 自己的 tests/examples/install，避免构建拖慢
set(SPDLOG_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_BENCH OFF CACHE BOOL "" FORCE)
set(SPDLOG_INSTALL OFF CACHE BOOL "" FORCE)

# 把 spdlog 作为子项目构建，并指定构建输出目录
add_subdirectory("${SPDLOG_DIR}" "${CMAKE_BINARY_DIR}/_deps/spdlog-build")

# ---------- Project options/includes ----------
add_library(project_options INTERFACE)
target_compile_options(project_options INTERFACE -Wall -Wextra)
target_link_libraries(project_options INTERFACE Threads::Threads)

# 如果当前配置是Debug，添加 ASAN 选项
if(ENABLE_ASAN)
  target_compile_options(project_options INTERFACE
    $<$<CONFIG:Debug>:-fsanitize=address -fno-omit-frame-pointer>
  )
  target_link_options(project_options INTERFACE
    $<$<CONFIG:Debug>:-fsanitize=address>
  )
endif()

add_library(project_includes INTERFACE)

target_include_directories(project_includes INTERFACE
  "${PROJECT_SOURCE_DIR}/src"
  "${PROJECT_SOURCE_DIR}/third_party"
)

# 使用预编译 spdlog 库时，consumer 侧建议定义 SPDLOG_COMPILED_LIB
add_library(project_deps INTERFACE)
target_link_libraries(project_deps INTERFACE spdlog::spdlog)
target_compile_definitions(project_deps INTERFACE SPDLOG_COMPILED_LIB)

# ---------- Helpers ----------
function(add_server_app target source_rel out_subdir)
  add_executable(${target} "${PROJECT_SOURCE_DIR}/${source_rel}") # 创建可执行目标文件
  target_link_libraries(${target} PRIVATE project_options project_includes project_deps) # 继承公共设置

  set_target_properties(${target} PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/apps/${out_subdir}" # 设置可执行目标文件的输出路径
  )
endfunction()

function(add_server_test target source_rel)
  add_executable(${target} "${PROJECT_SOURCE_DIR}/${source_rel}")
  target_link_libraries(${target} PRIVATE project_options project_includes project_deps)

  set_target_properties(${target} PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests"
  )
endfunction()

# ---------- Apps (对齐原 Makefile 的 simple_impl 构建目标) ----------
add_server_app(Iterative_server
  "apps/00_iterative/Iterative_server.cc"
  "00_iterative"
)
add_server_app(multithread_server
  "apps/01_multithread_per_conn/multithread_server.cc"
  "01_multithread_per_conn"
)
add_server_app(threadpool_server
  "apps/02_threadpool_blocking/threadpool_server.cc"
  "02_threadpool_blocking"
)
add_server_app(IO_multiplexing_server
  "apps/03_iomultiplexing/IO_multiplexing_server.cc"
  "03_iomultiplexing"
)

add_custom_target(apps
  DEPENDS Iterative_server multithread_server threadpool_server IO_multiplexing_server
)

# ---------- Tests (对齐原 Makefile 的 tests 目标) ----------
enable_testing()

add_server_test(HttpParser_test "tests/HttpParser_test.cc")
add_server_test(HttpRouter_test "tests/HttpRouter_test.cc")

add_test(NAME HttpParser_test COMMAND "${CMAKE_BINARY_DIR}/tests/HttpParser_test")
add_test(NAME HttpRouter_test COMMAND "${CMAKE_BINARY_DIR}/tests/HttpRouter_test")

add_custom_target(tests
  DEPENDS HttpParser_test HttpRouter_test
)

# ---------- Run targets ----------
# make -C build/debug run-tests
add_custom_target(run-tests
  COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
  DEPENDS tests
  WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
)

# 继续沿用 wrk / benchmark 脚本
add_custom_target(run-wrk
  COMMAND bash "${PROJECT_SOURCE_DIR}/scripts/run_wrk.sh" "${CMAKE_BINARY_DIR}"
  DEPENDS apps
  WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
)