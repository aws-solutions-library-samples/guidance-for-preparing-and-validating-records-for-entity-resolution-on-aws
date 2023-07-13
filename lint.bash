#!/bin/bash
set -e

# take profile name as input
if [ -z "$1" ]
then
    echo "No profile name supplied"
    echo "Usage: $0 <profile name>"
    exit 1
fi
# generates cdk nag output of the cdk stack include below line
# in the init method of the class
#        Aspects.of(self).add(cdk_nag.AwsSolutionsChecks())
cdk synth --profile=$1

# runs bandit for python vulnerabilities'
# rm -f bandit*.out
# bandit -r ./ > bandit.out
echo "**********Py files in the main Dir***********" > bandit_apponly.out
bandit ./*.py >> bandit_apponly.out
echo "**********Tests***********" >> bandit_apponly.out
bandit -r ./tests >> bandit_apponly.out
echo "**********Address Validation***********" >> bandit_apponly.out
bandit -r ./address_validation >> bandit_apponly.out
## use below if you want to selectively lint
# declare -a extensions=("*.ipynb" "*.py")
# for j in "${extensions[@]}"
# do
#     echo "${j}"
#     for i in `find . -name "${j}" -type f`
#     do
#         echo "'${i}'"
#         echo "**********'${i}'**********" >> bandit_${j}.out
#         bandit "'${i}'" >> bandit_${j}.out
#     done
# done

# for i in "${extensions[@]}"
# do
#     grep "High" bandit_${j}.out | grep -v ": 0"
#     grep "Medium" bandit_${j}.out | grep -v ": 0"
#     grep "Low" bandit_${j}.out | grep -v ": 0"
#     grep "Files skipped (0)" bandit_${j}.out
# done