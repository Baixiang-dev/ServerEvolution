cmake_minimum_required(VERSION 3.10)
project(ServerEvolution VERSION 1.0.0 LANGUAGES CXX)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# 查找系统上的 POSIX 线程库(pthread)
find_package(Threads REQUIRED)

# ============================ common dependencies ============================
set(SPDLOG_DIR "${PROJECT_SOURCE_DIR}/third_party/spdlog")

# 关掉 spdlog 自身的 tests/examples/install
set(SPDLOG_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_BENCH OFF CACHE BOOL "" FORCE)
set(SPDLOG_INSTALL OFF CACHE BOOL "" FORCE)

# 把 spdlog 作为子项目添加，并指定构建输出目录
add_subdirectory("${SPDLOG_DIR}" "${CMAKE_BINARY_DIR}/_deps/spdlog-build")

add_library(project_deps INTERFACE)
target_link_libraries(project_deps INTERFACE spdlog::spdlog)
target_compile_definitions(project_deps INTERFACE SPDLOG_COMPILED_LIB) # 定义宏，以预编译模式工作、而非 header-only 模式

# ============================ common options ================================
# 定义默认值为 ON 的开关变量，用于在 Debug 构建中启用 AddressSanitizer
option(ENABLE_ASAN "Enable AddressSanitizer in Debug builds" ON)

# 纯接口库，用于传递编译选项、链接选项、宏定义等内容给依赖它的目标
add_library (project_options INTERFACE)
target_compile_options(project_options INTERFACE -Wall -Wextra)

if(ENABLE_ASAN)
    target_compile_options(project_options INTERFACE 
        $<$<CONFIG:Debug>:-fsanitize=address -fno-omit-frame-pointer> # 对Debug模式，启用 ASAN 并保留栈帧信息
    )
    target_link_options(project_options INTERFACE 
        $<$<CONFIG:Debug>:-fsanitize=address> # 对Debug模式，启用 ASAN 链接选项
    )
endif()

# ============================ common include directories ============================
# 纯接口库，用于传递 include 目录给依赖它的目标
add_library(project_includes INTERFACE)
target_include_directories(project_includes INTERFACE
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/third_party
)

# ============================= helper function ========================================
# 定义一个函数，用于创建可执行目标文件，并设置其输出路径
function(add_server_app target source_rel out_subdir)
  add_executable(${target} "${PROJECT_SOURCE_DIR}/${source_rel}") # 创建可执行目标文件
  target_link_libraries(${target} PRIVATE project_options project_includes project_deps) # 继承公共设置
endfunction()

add_server_app(iterativer_server apps/00_iterative/Iterative_server.cc iterativer_server)
add_server_app(multithread_server apps/01_multithread_per_conn/multithread_server.cc multithread_server)
add_server_app(threadpool_server apps/02_threadpool_blocking/threadpool_server.cc threadpool_server)
add_server_app(IO_multiplexing_server apps/03_iomultiplexing/IO_multiplexing_server.cc io_multiplexing_server)
add_server_app(single_reactor_server apps/04_reactor/single_reactor.cc single_reactor_server)
add_server_app(single_reactor_multithread apps/04_reactor/single_reactor_multithread.cc single_reactor_multithread)
add_server_app(multi_reactor_server apps/04_reactor/multi_reactor.cc multi_reactor_server)