#!/bin/bash
set -e

# fb
mkdir -p ./python
pip install smartystreets_python_sdk -t ./python
zip -r ./layer.zip ./python
rm -r ./python/