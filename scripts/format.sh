#!/bin/bash
# hyperQ code formatting script

CLANG_FORMAT="clang-format"
echo "Formatting C++ code..."
find include -name "*.hpp" -exec $CLANG_FORMAT -i {} \;
find src -name "*.cpp" -exec $CLANG_FORMAT -i {} \;
find apps -name "*.cpp" -exec $CLANG_FORMAT -i {} \;
find tests -name "*.cpp" -exec $CLANG_FORMAT -i {} \;
echo "code formatting completed"
