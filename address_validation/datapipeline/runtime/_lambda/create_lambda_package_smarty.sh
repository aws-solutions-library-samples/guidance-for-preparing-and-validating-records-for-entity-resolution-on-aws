#!/bin/bash
set -e
path=../../deployment-packages/
mkdir -p ${path}
# install the dependencies
rm -rf ${path}/package ${path}/smarty_lambda_deployment.zip
pip install --target ${path}/package -r ./requirements-smarty.txt
cd ${path}/package
zip -r ${path}/smarty_lambda_deployment.zip .
cd -
zip -r ${path}/smarty_lambda_deployment.zip util
cp smarty_addr_val.py lambda_function.py
zip ${path}/smarty_lambda_deployment.zip lambda_function.py
rm lambda_function.py