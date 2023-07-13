#!/bin/bash
set -e
dir=../../deployment-packages
path=${dir}/producer_lambda_deployment.zip
# make directory if it does not exist
mkdir -p $dir
# install the dependencies
rm -f $path
zip -r $path util
cp produce_addr_val_batch_msgs.py lambda_function.py
zip $path lambda_function.py
rm lambda_function.py