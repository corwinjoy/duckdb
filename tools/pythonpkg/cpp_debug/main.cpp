// Test harness for direct C++ debugging
// Calls python function via C to debug directly in IDE
// Code from https://pythonextensionpatterns.readthedocs.io/en/latest/debugging/debug_in_ide.html

// Call this to invoke a particular python function

#include "py_import_call_execute.h"

// Usage:
// path-to-python-file python-file function-name

/*
* argv - Expected to be 4 strings:
*      - Name of the executable.
*      - Path to the directory that the Python module is in.
*      - Name of the Python module (without the file extension).
*      - Name of the function in the module.
*/

/* example:
 /home/cjoy/src/duckdb/tools/pythonpkg/tests/fast/api test_duckdb_interrupt run_test
*/

int main(int argc, const char *argv[]) {
  return import_call_execute(argc, argv);
}