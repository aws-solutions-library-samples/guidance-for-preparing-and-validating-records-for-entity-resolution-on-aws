#!/bin/bash
set -e
dir=../../deployment-packages
path=${dir}/awsloc_lambda_deployment.zip
# make directory if it does not exist
mkdir -p $dir
# install the dependencies
rm -f $path
zip -r $path util
cp awslocation_addr_val.py lambda_function.py
zip $path lambda_function.py
rm lambda_function.py