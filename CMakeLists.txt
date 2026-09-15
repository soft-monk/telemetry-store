# telemetry-store · 实时数据留存层（独立构建）
#
# 设计依据：docs/设计/遥测留存概要设计.md §6.2
# 目标 TLM-NFR-07：空环境 `cmake -S . -B build && cmake --build build` 通过，
#                  产出静态库 + 示例 + 工具 + 自测，**不需要任何外部服务**。
#
# 三种接入方式（与 device-ingest 一致）：
#   ① FetchContent add_subdirectory(telemetry-store)
#   ② git submodule  同上
#   ③ 本地 SOURCE_DIR 同上
cmake_minimum_required(VERSION 3.20)
project(telemetry_store
  VERSION 0.1.0
  DESCRIPTION "实时数据留存层：写入 / 查询 / 回放 / 补发 / 保留"
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "构建类型" FORCE)
endif()

# ---------------------------------------------------------------- 选项
option(TELEMETRY_STORE_BUILD_TESTS    "编译 tests/（零依赖自测）" ON)
option(TELEMETRY_STORE_BUILD_EXAMPLES "编译 examples/（最小示例、回放演示）" ON)
option(TELEMETRY_STORE_BUILD_TOOLS    "编译 tools/（造数、写入压测）" ON)
option(TELEMETRY_STORE_WARNINGS_AS_ERRORS "把编译告警当错误" OFF)

# ---------------------------------------------------------------- 依赖
# nlohmann/json：必需。优先用系统/包管理器提供的，否则回落内置单头副本。
# 本模块**永远不需要** Web 框架 / 数据库 / 网络库（TLM-NFR-07）。
find_package(nlohmann_json CONFIG QUIET)
if(nlohmann_json_FOUND)
  set(TELEMETRY_STORE_JSON_TARGET nlohmann_json::nlohmann_json)
  message(STATUS "[telemetry-store] nlohmann/json: 使用已安装的包")
else()
  if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/nlohmann/json.hpp")
    message(FATAL_ERROR
      "[telemetry-store] 未找到 nlohmann/json：既没有安装包，也没有内置副本 "
      "third_party/nlohmann/json.hpp。请安装 nlohmann-json 或放入内置副本。")
  endif()
  add_library(telemetry_store_nlohmann INTERFACE)
  target_include_directories(telemetry_store_nlohmann INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party")
  set(TELEMETRY_STORE_JSON_TARGET telemetry_store_nlohmann)
  message(STATUS "[telemetry-store] nlohmann/json: 使用内置单头副本 third_party/nlohmann/json.hpp")
endif()

find_package(Threads REQUIRED)

# ---------------------------------------------------------------- 库
file(GLOB_RECURSE TELEMETRY_STORE_SOURCES CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")

add_library(telemetry_store STATIC ${TELEMETRY_STORE_SOURCES})
add_library(telemetry_store::telemetry_store ALIAS telemetry_store)

target_include_directories(telemetry_store
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:include>"
  PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/src")

target_link_libraries(telemetry_store
  PUBLIC  ${TELEMETRY_STORE_JSON_TARGET}
  PRIVATE Threads::Threads)

target_compile_definitions(telemetry_store PUBLIC
  TELEMETRY_STORE_VERSION="${PROJECT_VERSION}")

if(MSVC)
  target_compile_options(telemetry_store PRIVATE /utf-8 /W4 /permissive-)
  target_compile_definitions(telemetry_store PRIVATE
    NOMINMAX WIN32_LEAN_AND_MEAN _WIN32_WINNT=0x0A00
    _CRT_SECURE_NO_WARNINGS)
  if(TELEMETRY_STORE_WARNINGS_AS_ERRORS)
    target_compile_options(telemetry_store PRIVATE /WX)
  endif()
else()
  target_compile_options(telemetry_store PRIVATE -Wall -Wextra)
  if(TELEMETRY_STORE_WARNINGS_AS_ERRORS)
    target_compile_options(telemetry_store PRIVATE -Werror)
  endif()
endif()

set_target_properties(telemetry_store PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
  ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

# ---------------------------------------------------------------- 工具 / 示例 / 测试
if(TELEMETRY_STORE_BUILD_TOOLS)
  add_subdirectory(tools)
endif()
if(TELEMETRY_STORE_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
if(TELEMETRY_STORE_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

# ---------------------------------------------------------------- 汇总
message(STATUS "[telemetry-store] ${PROJECT_NAME} ${PROJECT_VERSION} · C++${CMAKE_CXX_STANDARD}")
message(STATUS "[telemetry-store]   构建类型 : ${CMAKE_BUILD_TYPE}")
message(STATUS "[telemetry-store]   tools/examples/tests : "
               "${TELEMETRY_STORE_BUILD_TOOLS}/${TELEMETRY_STORE_BUILD_EXAMPLES}/"
               "${TELEMETRY_STORE_BUILD_TESTS}")
