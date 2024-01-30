
# Extra definitions for standalone compilation
cmake_minimum_required(VERSION 3.5)
project(DuckDB_python)
set(pybind11_DIR //home/cjoy/src/duckdb/.venv/lib/python3.10/site-packages/pybind11/share/cmake/pybind11)
link_directories(../../build/debug/src)

include_directories(src)
include_directories(../../)
include_directories(src/include)
include_directories(${PYTHON_INCLUDE_DIRS})

add_subdirectory(src)

find_package(PythonLibs)
if(NOT PythonLibs_FOUND)
  return()
endif()

find_package(pybind11)
if(NOT pybind11_FOUND)
  return()
endif()
include_directories(${pybind11_INCLUDE_DIR})
# this is used for clang-tidy checks

set(ALL_OBJECT_FILES duckdb_python.cpp ${ALL_OBJECT_FILES})

add_library(python_duckdb ${ALL_OBJECT_FILES})
target_include_directories(python_duckdb PRIVATE ${PYTHON_INCLUDE_DIRS})
target_link_libraries(python_duckdb duckdb_static ${PYTHON_LIBRARIES})

add_executable(cpp_ide_debug EXCLUDE_FROM_ALL cpp_debug/main.cpp cpp_debug/py_import_call_execute.c)
target_include_directories(cpp_ide_debug PRIVATE ${PYTHON_INCLUDE_DIRS})
target_link_libraries(cpp_ide_debug duckdb_static ${PYTHON_LIBRARIES})
