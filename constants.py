# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import os

WORKLOAD_NAME = "Entity-Resolution-"
COMPONENT_NAME = "Address-Validation-"
APP_NAME = WORKLOAD_NAME + COMPONENT_NAME

GITHUB_CONNECTION_ARN = "CONNECTION_ARN"
GITHUB_OWNER = "OWNER"
GITHUB_REPO = "REPO"
GITHUB_TRUNK_BRANCH = "TRUNK_BRANCH"
PRODUCTION_ENV_NAME = "Production"
SANDBOX_ENV_NAME = "Sandbox"
# Change this as required to point to the production environment variables
PRODUCTION_ENV_ACCOUNT = os.environ["CDK_DEFAULT_ACCOUNT"]
PRODUCTION_ENV_REGION = os.environ["CDK_DEFAULT_REGION"]

AWS_SOLUTION_ID = "SO9245"
AWS_SOLUTION_GUIDANCE_DESC = f"Guidance for preparing and validating records for Entity Resolution on AWS ({AWS_SOLUTION_ID})"