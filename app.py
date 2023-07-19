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

import aws_cdk as cdk

import constants
from address_validation.component import AddressValidation
# from toolchain import Toolchain

from cdk_nag import AwsSolutionsChecks, NagSuppressions

app = cdk.App()

# Component sandbox stack
av_stack = AddressValidation(
    app,
    f"{constants.APP_NAME}-{constants.SANDBOX_ENV_NAME}",
    env=cdk.Environment(
        account=os.environ["CDK_DEFAULT_ACCOUNT"],
        region=os.environ["CDK_DEFAULT_REGION"],
    ),
    description=constants.AWS_SOLUTION_GUIDANCE_DESC
)

# Toolchain stack (defines the continuous deployment pipeline)
# TODO change the tool chain account through new env variables
# toolchain = Toolchain(
#     app,
#     constants.APP_NAME + "Toolchain"
#     ),
# )

NagSuppressions.add_stack_suppressions(
    av_stack,
    [
        {
            "id": "AwsSolutions-IAM5",
            "reason": "AWS managed policies are allowed which sometimes uses * in the resources like - AWSGlueServiceRole has aws-glue-* . AWS Managed IAM policies have been allowed to maintain secured access with the ease of operational maintenance - however for more granular control the custom IAM policies can be used instead of AWS managed policies",
        },
        {
            "id": "AwsSolutions-IAM4",
            "reason": "AWS Managed IAM policies have been allowed to maintain secured access with the ease of operational maintenance - however for more granular control the custom IAM policies can be used instead of AWS managed policies",
        },
        {
            "id": "AwsSolutions-S1",
            "reason": "S3 Access Logs are enabled for all data buckets. This stack creates a access log bucket which doesnt have its own access log enabled.",
        },
        {
            "id": "AwsSolutions-SQS3",
            "reason": "SQS queue used in the CDC is a DLQ.",
        },
        {
            "id": "AwsSolutions-SMG4",
            "reason": "Rotation is disabled in the sample code. Customers are encouraged to rotate thirdparty api tokens",
        },
        {
            'id': 'AwsSolutions-KMS5',
            'reason': 'SQS KMS key properties are not accessible from cdk',
        },
         {
            'id': 'AwsSolutions-S10',
            'reason': 'Key properties are not accessible from cdk',
        },
         {
            'id': 'AwsSolutions-L1',
            'reason': 'Key properties are not accessible from cdk',
        }
    ],
)

cdk.Aspects.of(app).add(AwsSolutionsChecks())

app.synth()
