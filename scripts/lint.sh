#!/bin/bash
# Static Analysis Script

CPPCHECK="cppcheck"
echo "Running static anaysis..."
$CPPCHECK --enable=all include/
$CPPCHECK --enable=all src/
$CPPCHECK --enable=all apps/
echo "Analysis complete"
