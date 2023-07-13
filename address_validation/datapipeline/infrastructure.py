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

# import aws_cdk as cdk
# import aws_cdk.aws_dynamodb as dynamodb
# from constructs import Construct

from constructs import Construct
from aws_cdk import (
    Duration,
    Stack,
    aws_iam as iam,
    aws_s3 as s3,
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_secretsmanager as secretsmanager,
    aws_lambda as _lambda,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_kms as kms,
    aws_dynamodb as dynamodb,
    aws_s3_deployment as s3_deploy,
    aws_logs as logs,
    aws_databrew as databrew,
    RemovalPolicy, CfnOutput,
    aws_lambda_destinations as destinations,
    aws_lambda_event_sources as event_sources,
    aws_ssm as ssm,
    aws_location
)
import json
import aws_cdk.aws_glue_alpha as glue_a
from os import path
from . import databrew_recipe
class DataPipeline(Construct):

    def __init__(
        self, scope: Construct, id_: str,*, account,region,**kwargs
    ):
        super().__init__(scope, id_, **kwargs)
        self.cli_profile = self.node.try_get_context("cli_profile")
        self.data_bucket_prefix = self.node.try_get_context("data_bucket_name")
        self.dataset_file_name = self.node.try_get_context("dataset_file_name")
        self.dataset_path_prefix = self.node.try_get_context("dataset_path_prefix")
        self.databrew_job_name = self.node.try_get_context("databrew_job_name") 
        self.databrew_recipe_file_name = self.node.try_get_context("databrew_recipe_file_name")
        self.glue_job_script = self.node.try_get_context("glue_job_script")
        self.schedule_hour = self.node.try_get_context("schedule_daily_hour")
        self.glue_database_name = self.node.try_get_context("glue_database_name")
        self.glue_job_name = self.node.try_get_context("glue_job_name")
        self.timedelta_days = self.node.try_get_context("timedelta_days")
        self.parent_project = self.node.try_get_context("parent_project")
        self.connection_name = self.node.try_get_context("connection_name")
        self.filter = self.node.try_get_context("filter")
        self.glue_table_name = self.node.try_get_context("glue_table_name")
        self.account = account
        self.region = region
        self.kms_key_alias = self.node.try_get_context("kms_key_alias")
        self.project_prefix = "idResAddrVal"
        self.runtime_asset_path = "address_validation/datapipeline/runtime"
        self.sample_data_path = "address_validation/datapipeline/sample-data"
        self.deployment_packages_path = "address_validation/datapipeline/deployment-packages"
        self.data_bucket_exist_flag = self.node.try_get_context("data_bucket_exist_flag")
        self.data_bucket_name = f"{self.data_bucket_prefix}-{self.account}-{self.region}"
        self.producer_glue_job_name = "produce_addr_val_batch_msgs"
        self.cdk_asset_bucket_name = f"cdk-assets-{self.account}-{self.region}"
        self.producer_glue_job_script = f"s3://{self.cdk_asset_bucket_name}/{self.runtime_asset_path}/_lambda/{self.producer_glue_job_name}.py"
        self.address_val_glue_job_name = "validate-address-smarty"
        self.address_val_glue_job_script = f"s3://{self.cdk_asset_bucket_name}/{self.runtime_asset_path}/_glue/{self.address_val_glue_job_name}.py"
        # update run time as needed
        self.lambda_runtime = _lambda.Runtime.PYTHON_3_9
        print(f"Stack is in account:{self.account} and region:{self.region}")
        print("bucketname is",  f"{self.data_bucket_prefix}-{self.account}-{self.region}" )
        print("glue database is",  f"{self.glue_database_name}")
      
        self.role_name = "AddressValidation_upload_role"
        self.config_prefix = "AddressValidation"
        self.parameter_prefix = "addr-val-"
        self.ssm_smarty_param_name = f"{self.parameter_prefix}smarty"
        self.ssm_awsloc_param_name = f"{self.parameter_prefix}awslocation"
        self.ssm_producer_param_name = f"{self.parameter_prefix}msg-producer"
        self.awsloc_place_index_name = f"{self.project_prefix}-place-index"
        self.glue_crawler_path = f"s3://{self.data_bucket_name}/{self.dataset_path_prefix}/"
        self.databrew_output_path = f"{self.dataset_path_prefix}/{self.dataset_file_name}-normalized"
        self.databrew_recipe_name = "normalize-address-databrew-recipe"
        self.databrew_recipe_path = f"{self.runtime_asset_path}/databrew/{self.databrew_recipe_file_name}"
        # to use in role policies
        self.kms_secret_arn = f"arn:aws:kms:{self.region}:{self.account}:key/{self.parameter_prefix}*"
        
        # gives access to multiple parameters that follow the pattern
        self.ssm_parameter_arn = f"arn:aws:ssm:{self.region}:{self.account}:parameter/{self.parameter_prefix}*"
        
        # for associating resource to policy for kms
        self.glue_log_group_arn = f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws-glue/jobs"
        
        # default KMS key ARN
        self.aws_managed_kms_key_arn = f"arn:aws:kms:{self.region}:{self.account}:alias/aws/s3"
        # Sets a customer managed key as best practise. Customer managed keys comes with higher costs compared to AWS managed.
        self.set_kms_key()

        # creates all s3 buckets

        # for s3 access logs
        self.s3_access_log_bucket_name=f"s3-access-log-{self.account}-{self.region}"
        self.add_s3_access_log_bucket()
        self.add_s3_buckets()
        # deploys all run time assets to S3 bucket
        self.add_scripts()
        # deploys all sample data to S3 bucket
        self.add_sample_data()
        
        # creates an secrets manager skeleton entry
        self.add_secret()

        ## use alternative lambda based architecture instead of glue
        # create sqs queues. Since the queue arn is referred in the role, creating that first
        # self.address_val_sqs_queue = self.add_sqs_queue("address-val-sqs-queue")
        # self.address_val_dlq = self.add_sqs_queue("address-val-dlq")
        # self.producer_dlq = self.add_sqs_queue("producer-dlq")

        # builds a single role for the application
        self.add_role()

        ## starts glue based pipeline build
        self.add_databrew_dataset()
        # self.add_databrew_recipe()
        databrew_recipe.add_update_recipe(self.databrew_recipe_name, self.databrew_recipe_path, self.cli_profile, True)
        self.add_databrew_job()
        self.add_glue_jobs()
        self.add_crawler()

        # ssm parameters used by glue job and lambda functions
        self.add_ssm_parameters()

        # put it all together in a workflow
        self.add_workflow()

        ### Below is needed only for a lambda based execution workflow as a substitute of the glue stack above
        ## Starts lamdba based pipeline build
        # layers needs to be added first before adding function
        # self.add_lambda_layers()
        # self.add_smarty_lambda_function()
        # optionally use producer as a lambda if a event based architecture of validating data on s3 object creation
        # self.add_producer_lambda_function()
        # optionally use aws location service for address validation
        # self.add_awsloc_lambda_function()
        # AWS location place index
        # self.add_awsloc_place_index()


# S3 Buckets #
# Assumption: Input and output of the validation service will be stored in an output folder in the existing S3 bucket.  
    def get_s3_bucket(self, bucket_name:str) -> s3.Bucket:
        """
        Helper method to get the standard s3 bucket construct
        Needs the access log s3 and kms key already defined
        Implements cdk-nag rules
        """
        s3_bucket = s3.Bucket(
            self,
            bucket_name,
            bucket_name=bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            server_access_logs_bucket=self.s3_access_log_bucket,
            server_access_logs_prefix=bucket_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            # FIXED Customer managed key deployment fails, woraround is to use AWS managed Key
            # need to input the iKey construct in iam. iAlias wont work
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.kms_key,
            bucket_key_enabled=True,
            # encryption=s3.BucketEncryption.KMS_MANAGED,
            enforce_ssl=True,
            event_bridge_enabled=True,
        )
        return s3_bucket

    def add_s3_access_log_bucket(self) -> None:
        """
        Creates a bucket to store access logs of other buckets
        This implements cdk-nag rule AwsSolutions-S1
        """
        self.s3_access_log_bucket = s3.Bucket(
            self,
            self.s3_access_log_bucket_name,
            bucket_name=self.s3_access_log_bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.kms_key,
            enforce_ssl=True,
        )

    def add_s3_buckets(self) -> None:
        """
        Creates S3 buckets needed for the pipeline
        """
        # creates cdk asset bucket for copying run time code
        self.cdk_asset_bucket = self.get_s3_bucket(self.cdk_asset_bucket_name)
        # create source bucket only if user denotes that it doesnt exist
        # if it exists lookup the bucket for the cfnoutput
        # Typically source data bucket for validation should exist and that should be the source of glue job
        # create s3 bucket if it doesnt exist. If it does exist, use that bucket for cfnoutput

        if self.data_bucket_exist_flag.lower() == "n":
            self.data_bucket = self.get_s3_bucket(self.data_bucket_name)
        else:
            self.data_bucket = s3.Bucket.from_bucket_name(self, f"{self.data_bucket_prefix}-bucket", self.data_bucket_name)

        # use below if separate target bucket is desired
        # self.glue_target_bucket = self.get_s3_bucket(self.glue_target_bucket_name)

        # if the glue asset bucket exist, refer that instead of creating. 
        # Create will fail if bucket exist
        # self.glue_asset_bucket = self.get_s3_bucket(f"aws-glue-assets-logs-{self.account}-{self.region}")
        self.glue_asset_bucket = s3.Bucket.from_bucket_name(self, f"aws-glue-assets-logs-bucket", f"aws-glue-assets-logs-{self.account}-{self.region}")
        CfnOutput(self, "glue_source_bucket_name", value=self.data_bucket.bucket_name)
        CfnOutput(self, "glue_asset_bucket_name", value=self.glue_asset_bucket.bucket_name)
        CfnOutput(self, "CDK_bucket_name", value=self.cdk_asset_bucket.bucket_name)

# Secret #
    def add_secret(self):
        # Check if the secrets exist and only create if it does not exit - tbd
        # Creates a AWS Secrets manager entry. User needs to update secret manually in console after depolyment
        
        self.AddressValidation_api_secret = secretsmanager.Secret(self, 
            f"{self.config_prefix}-secret",
            secret_name=f"{self.parameter_prefix}smartyapi",
            description="API access token for data validation api",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template=json.dumps({"auth_id": "","auth_token":""}),
                generate_string_key="auth_id,auth_token",
            ),
            encryption_key=self.kms_key
        )
        CfnOutput(self, "secret_manager_entry", value=self.AddressValidation_api_secret.secret_name)

# Application role #
    def add_role(self):
        # Add custom policies
        self.s3_policy_statement = iam.PolicyStatement(
            sid=f"{self.project_prefix}S3Access",
            actions=[
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:DeleteObject",
                "s3:GetBucketLocation",
                "s3:ListMultipartUploadParts",
                "s3:GetBucketAcl",
                ],
            resources=[
                self.cdk_asset_bucket.bucket_arn,
                f"{self.cdk_asset_bucket.bucket_arn}/*",
                self.glue_asset_bucket.bucket_arn,
                f"{self.glue_asset_bucket.bucket_arn}/*",
                self.data_bucket.bucket_arn, 
                # self.glue_target_bucket.bucket_arn,
                f"{self.data_bucket.bucket_arn}/*"
                ],
        )

        self.kms_policy_statement = iam.PolicyStatement(
            sid=f"{self.project_prefix}KMSAccess",
            actions=[
                "kms:Describe*",
                "kms:Get*",
                "kms:List*",
                "kms:Decrypt",
                "kms:Encrypt",
                "kms:GenerateDataKey"
            ],
            resources=[self.kms_key.key_arn]
        )

        self.ssm_policy_statement = iam.PolicyStatement(
            sid=f"{self.project_prefix}SSMAccess",
            actions=[
                "ssm:GetParameter*"
            ],
            resources=[self.ssm_parameter_arn]
        )

        self.loggroup_policy_statement = iam.PolicyStatement(
            sid=f"{self.project_prefix}LogGroupAccess",
            actions=[
                "logs:AssociateKmsKey",
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
            ],
            resources=[
                f"{self.glue_log_group_arn}/error:*",
                f"{self.glue_log_group_arn}/logs-v2:*",
                f"{self.glue_log_group_arn}/output:*"
                ],
        )

        self.secrets_policy_statement = iam.PolicyStatement(
            sid=f"{self.project_prefix}SecretsAccess",
            actions=[
                "secretsmanager:GetResourcePolicy",
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret",
                "secretsmanager:ListSecretVersionIds",
            ],
            resources=[
                self.AddressValidation_api_secret.secret_arn
            ],
        )
        
        # self.sqs_policy_statement = iam.PolicyStatement(
        #     sid=f"{self.project_prefix}SQSAccess",
        #     actions=[
        #         "sqs:SendMessage",
        #         "sqs:ReceiveMessage",
        #         "sqs:DeleteMessage",
        #         "sqs:GetQueueAttributes",
        #         "sqs:GetQueueUrl",
        #         "sqs:ListQueues",
        #         "sqs:PurgeQueue",
        #         "sqs:ChangeMessageVisibility",
        #     ],
        #     resources=[
        #         self.address_val_sqs_queue.queue_arn, 
        #         self.address_val_dlq.queue_arn, 
        #         self.producer_dlq.queue_arn
        #         ],
        # )

        self.awsloc_policy_statement = iam.PolicyStatement(
            sid=f"{self.project_prefix}AWSLocAccess",
            actions=[
                "geo:SearchPlaceIndexForText",
                "geo:SearchPlaceIndexForPosition"
                ],
            resources=[
                f"arn:aws:geo:{self.region}:{self.account}:place-index/{self.awsloc_place_index_name}"
            ]
        )
        
        inline_policy_doc = iam.PolicyDocument(statements=[
            self.s3_policy_statement,
            self.kms_policy_statement,
            self.ssm_policy_statement,
            self.loggroup_policy_statement,
            self.secrets_policy_statement,
            # self.sqs_policy_statement,
            self.awsloc_policy_statement])
    
        self.role = iam.Role(
            self,
            f"AddressValidation-glue-job-role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal(f"logs.{self.region}.amazonaws.com"),
                iam.ServicePrincipal("lambda.amazonaws.com"),
                iam.ServicePrincipal("glue.amazonaws.com"),
                iam.ServicePrincipal("databrew.amazonaws.com"),
                iam.ServicePrincipal(f"states.{self.region}.amazonaws.com")
            ),
            inline_policies={"customPolicies": inline_policy_doc},
        )
        # add managed roles
        self.role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"))
        self.role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonEC2ContainerRegistryReadOnly"))
        self.role.add_managed_policy(iam.ManagedPolicy.from_managed_policy_arn(self,id="databrew_service_role",managed_policy_arn="arn:aws:iam::aws:policy/service-role/AWSGlueDataBrewServiceRole"))
        self.AddressValidation_api_secret.grant_read(self.role)
        CfnOutput(self, "Application Role ARN", value=self.role.role_arn)

# GlueJobs #
    def get_glue_security_config(self, job_name: str) -> glue.CfnSecurityConfiguration:
        """
        Returns the security configuration for glue jobs
        """
        return glue.CfnSecurityConfiguration(
            self,
            "GlueJobSecurityConfiguration",
            encryption_configuration=glue.CfnSecurityConfiguration.EncryptionConfigurationProperty(
                cloud_watch_encryption=glue.CfnSecurityConfiguration.CloudWatchEncryptionProperty(
                    cloud_watch_encryption_mode="SSE-KMS",
                    kms_key_arn=self.kms_key.key_arn
                ),
                job_bookmarks_encryption=glue.CfnSecurityConfiguration.JobBookmarksEncryptionProperty(
                    job_bookmarks_encryption_mode="CSE-KMS",
                    kms_key_arn=self.kms_key.key_arn
                ),
                s3_encryptions=[
                    glue.CfnSecurityConfiguration.S3EncryptionProperty(
                        kms_key_arn=self.kms_key.key_arn,
                        s3_encryption_mode="SSE-KMS"
                    )   
                ]
            ),
            name=f"{job_name}SecConfig",
        )
    
    def add_normalize_glue_job(self) -> None:
        """
        Creates sample glue job with data normalization code
        """
        # https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
        arguments = {
            "--class":	"GlueApp",
            "--job-language":	"python",
            "--job-bookmark-option":	"job-bookmark-enable",
            "--TempDir":	f"s3://{self.glue_asset_bucket.bucket_name}/temporary/",
            "--enable-metrics":	"true",
            "--enable-continuous-cloudwatch-log":	"true",
            "--enable-spark-ui":	"true",
            "--enable-auto-scaling":	"true",
            "--spark-event-logs-path":	f"s3://{self.glue_asset_bucket.bucket_name}/sparkHistoryLogs/",
            "--enable-glue-datacatalog":	"true",
            "--enable-job-insights":	"true",
            "--sourcebucket":	self.data_bucket.bucket_name,
            "--targetbucket": self.data_bucket.bucket_name,
            "--sourcetable": self.glue_table_name,
            "--targettable": f"{self.glue_table_name}_normalized",
            "--targetcatalogdb": self.glue_database_name,
            "--targetcatalogtable": self.glue_database_name
        }

        glue_job = glue.CfnJob(
            self,
            self.glue_job_name,
            name=self.glue_job_name,
            role=self.role.role_arn,
            allocated_capacity=10,
            command=glue.CfnJob.JobCommandProperty(
                name=f"glueetl",
                python_version="3",
                script_location=f"s3://{self.cdk_asset_bucket.bucket_name}/{self.runtime_asset_path}/_glue/{self.glue_job_script}",
            ),
            # security_configuration=get_glue_security_config(self.glue_job_name)",
            glue_version="3.0",
            default_arguments=arguments,
        )

        CfnOutput(self, "Normalize_Glue_Job", value=glue_job.name)
    
    def add_produce_messages_job(self) -> None:
        """
        Creates sample glue job to start address validation by reading files and producing SQS messages
        """
        # https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
        arguments = {
            "--job-language":	"python",
            "--TempDir":	f"s3://{self.glue_asset_bucket.bucket_name}/temporary/",
            "--enable-glue-datacatalog":	"true",
            "--enable-job-insights":	"true",
            "--sqs_queue": self.address_val_sqs_queue.queue_url,
            "--batch_mode": "chunk_size",
            "--delimiter": ",",
            "--chunk_size": "100",
            "--encoding": "utf-8",
            "--bucket": self.data_bucket.bucket_name,
            "--key": self.databrew_output_path,
            "--limit_rows": "100000000",
            "--extra-files": f"s3://{self.cdk_asset_bucket.bucket_name}/{self.runtime_asset_path}/_lambda/util/utils.py"
        }
        
        glue_job = glue.CfnJob(
            self,
            "Producer Glue Job",
            name=self.producer_glue_job_name,
            role=self.role.role_arn,
            # allocated_capacity=10,
            command=glue.CfnJob.JobCommandProperty(
                name=f"pythonshell",
                python_version="3.9",
                script_location=self.producer_glue_job_script,
            ),
            # security_configuration=f"{self.glue_job_name}SecConfig",
            glue_version="3.0",
            default_arguments=arguments,
        )

        CfnOutput(self, "Address_validation_Producer_Glue_Job", value=glue_job.name)

    def add_smarty_validate_job(self) -> None:
        """
        Creates sample glue job to start address validation by reading files and producing SQS messages
        """
        # https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
        arguments = {
            "--job-language":	"python",
            "--TempDir":	f"s3://{self.glue_asset_bucket.bucket_name}/temporary/",
            "--enable-glue-datacatalog":	"true",
            "--enable-job-insights":	"true",
            "--ssm_parameter": self.ssm_smarty_param_name,
            "--delimiter": ",",
            "--chunk_size": "100",
            "--encoding": "utf-8",
            "--bucket": self.data_bucket.bucket_name,
            "--key": self.databrew_output_path,
            "--limit_rows": "100000000",
            "--extra-files": f"s3://{self.cdk_asset_bucket.bucket_name}/{self.runtime_asset_path}/_lambda/util/utils.py",
            "--additional-python-modules":"smartystreets-python-sdk==4.11.16"
        }
        
        glue_job = glue.CfnJob(
            self,
            "Smarty Validation Glue Job",
            name=self.address_val_glue_job_name,
            role=self.role.role_arn,
            # allocated_capacity=10,
            command=glue.CfnJob.JobCommandProperty(
                name=f"pythonshell",
                python_version="3.9",
                script_location=self.address_val_glue_job_script,
            ),
            # security_configuration=f"{self.glue_job_name}SecConfig",
            glue_version="3.0",
            default_arguments=arguments,
        )

        CfnOutput(self, "Smarty Address Validation Glue Job", value=glue_job.name)
    
    def add_glue_jobs(self) -> None:
        """
        Creates sample glue jobss
        """
        self.add_normalize_glue_job()
        # self.add_produce_messages_job()
        self.add_smarty_validate_job()

# deploy all runtime to S3 #
    def add_scripts(self):
        self.script_deployment = s3_deploy.BucketDeployment(
            self,
            "script-deployment",
            sources=[s3_deploy.Source.asset(f"./{self.runtime_asset_path}")],
            destination_bucket=self.cdk_asset_bucket,
            destination_key_prefix=self.runtime_asset_path,
        )

# deploy all sample data to S3 #
    def add_sample_data(self):
        self.sample_data_deployment = s3_deploy.BucketDeployment(
            self,
            "sample-data-deployment",
            sources=[s3_deploy.Source.asset(f"./{self.sample_data_path}")],
            destination_bucket=self.data_bucket,
            destination_key_prefix=self.dataset_path_prefix
        )

# Glue database and Crawler #
    def add_crawler(self):
        # create a glue database
        glue_database= glue_a.Database(
            self,
            id=f"{self.project_prefix}-glue-database",
            database_name=self.glue_database_name,
        )
        # Retain the database when deleting
        glue_database.apply_removal_policy(policy=RemovalPolicy.DESTROY)

        self.audit_policy = glue.CfnCrawler.SchemaChangePolicyProperty(update_behavior='UPDATE_IN_DATABASE', delete_behavior='LOG')
        
        self.glue_crawler = glue.CfnCrawler(self,f"{self.glue_job_name}-crawler",
            name= f"{self.project_prefix}-crawler",
            role=self.role.role_arn,
            database_name=self.glue_database_name,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets= [glue.CfnCrawler.S3TargetProperty(
                    path=self.glue_crawler_path,
                    exclusions= ["glue-scripts/**"],
                    sample_size=100
                )]
            ),
            schema_change_policy=self.audit_policy,
            configuration='{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"}}}',
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior='CRAWL_EVERYTHING'
            )
        )
        CfnOutput(self, "Glue_Crawler_Name", value=self.glue_crawler.name)

# Step Function to managed the job flow #
    def add_workflow(self):

        succeed_nothing_to_job = sfn.Succeed(
            self, "Success",
            comment='Job succeeded'
        )
        # TODO add databrew
        databrew_job_task= sfn_tasks.GlueDataBrewStartJobRun(self, "StartDataBrewNormalizeJob",
            name= self.databrew_job.name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            )
        # Glue task for data normalization
        normalize_glue_job_task= sfn_tasks.GlueStartJobRun(self, "StartDataNormalizationGlueJob",
            glue_job_name= self.glue_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            task_timeout=sfn.Timeout.duration(Duration.minutes(30)),
            notify_delay_after=Duration.minutes(5)
        )

        # Glue task for address validation
        address_val_glue_job_task= sfn_tasks.GlueStartJobRun(self, "StartAddressValidationGlueJob",
            glue_job_name= self.address_val_glue_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            task_timeout=sfn.Timeout.duration(Duration.minutes(30)),
            notify_delay_after=Duration.minutes(5)
        )

        start_crawler_task = sfn_tasks.CallAwsService(self,
            "StartGlueCrawlerTask",
            service="glue",
            action="startCrawler",
            parameters={
                "Name": self.glue_crawler.name
            },
            iam_resources=[
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:*"
            ]
        )
        
        get_crawler_status_task = sfn_tasks.CallAwsService(self,
            "GetCrawlerStatusTask",
            service="glue",
            action="getCrawler",
            parameters={
                "Name": self.glue_crawler.name
            },
            iam_resources=[
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:*"
            ]
        )

        catch_job_error = sfn.Pass(
            self,
            "Catch an Error",
            result_path=sfn.JsonPath.DISCARD
        )

        job_failed = sfn.Fail(self, "Job Failed",
            cause="Job Failed",
            error="JOB FAILED"
        )

        # catch error and retry
        databrew_job_task.add_catch(catch_job_error, errors=['States.ALL'],result_path='$.error')
        databrew_job_task.add_retry(max_attempts=2,backoff_rate=1.05,interval=Duration.seconds(60),errors=["DatabrewJobRetry"])
        # normalize_glue_job_task.add_catch(catch_job_error, errors=['States.ALL'],result_path='$.error')
        # normalize_glue_job_task.add_retry(max_attempts=2,backoff_rate=1.05,interval=Duration.seconds(60),errors=["GlueJobRetry"])
        address_val_glue_job_task.add_catch(catch_job_error, errors=['States.ALL'],result_path='$.error')
        address_val_glue_job_task.add_retry(max_attempts=2,backoff_rate=1.05,interval=Duration.seconds(60),errors=["GlueJobRetry"])
        start_crawler_task.add_catch(catch_job_error, errors=['States.ALL'],result_path='$.error')
        start_crawler_task.add_retry(max_attempts=2,backoff_rate=1.05,interval=Duration.seconds(60),errors=["CrawlerRetry"])
        
        catch_job_error.next(job_failed)

        # check the crawler status
        wait_90m=sfn.Wait(self,"Wait 90 Seconds",
            time=sfn.WaitTime.duration(Duration.seconds(90)))

        wait_90m.next(get_crawler_status_task).next(sfn.Choice(self, 'Crawler Finished?')\
            .when(sfn.Condition.string_equals("$.Crawler.State", "READY"), succeed_nothing_to_job)
            .otherwise(wait_90m))
        
        # TODO update with databrew
        definition = databrew_job_task.next(address_val_glue_job_task).next(start_crawler_task).next(wait_90m)
        # definition = normalize_glue_job_task.next(producer_glue_job_task).next(start_crawler_task).next(wait_90m)\

        # Create state machine
        sm = sfn.StateMachine(
            self, "address-validation-workflow",
            state_machine_name ="address-validation-workflow",
            definition=definition,
            logs=sfn.LogOptions(
                destination=logs.LogGroup(self, "UnfurlStateMachineLogGroup"),
                level=sfn.LogLevel.ALL,
            ),
            timeout=Duration.minutes(1800),
            tracing_enabled=True,
        )


        # EventBridge Trigger Cron
        # below schedules job at specified hour of every day, change this as per requirements
        scheduling_rule = events.Rule(
            self, "AddressValidation-Job-Scheduling-Rule",
            rule_name="AddressValidation-Job-Scheduling-Rule",
            description="Daily triggered event to get analytics data",
            schedule=events.Schedule.cron(
                minute='0',
                hour=self.schedule_hour,
                ),
        )
        
        scheduling_rule.add_target(targets.SfnStateMachine(sm))
        CfnOutput(self, "State Machine Arn", value=sm.state_machine_arn)
        CfnOutput(self, "Event Bridge Rule Arn", value=scheduling_rule.rule_arn)     

# Databrew job #
    def add_databrew_dataset(self):
        
        self.cfn_dataset = databrew.CfnDataset(
            self, 
            f"{self.project_prefix}-databrew-dataset",
            input=databrew.CfnDataset.InputProperty(
                s3_input_definition=databrew.CfnDataset.S3LocationProperty(
                    bucket=self.data_bucket.bucket_name,
                    key=f"{self.dataset_path_prefix}/{self.dataset_file_name}"
                )
            ),
            name=self.glue_table_name,
            format="CSV",
            format_options=databrew.CfnDataset.FormatOptionsProperty(
                csv=databrew.CfnDataset.CsvOptionsProperty(
                    delimiter=",",
                    header_row=True
                )
            )
        )
        self.cfn_dataset.node.add_dependency(self.sample_data_deployment)
        CfnOutput(self, "Databrew dataset name", value=self.cfn_dataset.name)
    # build databrew recipe object[this is not used in this project, boto3 sdk is used instead]
    def get_databrew_recipe_property(self, step_dict: dict) -> tuple:
        action=databrew.CfnRecipe.ActionProperty(
            operation = step_dict["Action"]["Operation"],
            parameters = step_dict["Action"]["Parameters"])
        try:
            condition_expressions=step_dict["ConditionExpressions"]
        except KeyError:
            condition_expressions=None
        else:
            condition_expressions=[databrew.CfnRecipe.ConditionExpressionProperty(
                condition=j["Condition"],
                target_column=j["TargetColumn"],
                value=j["Value"]
            ) for j in step_dict["ConditionExpressions"]]
        # print(action, condition_expressions)
        return action, condition_expressions

    def add_databrew_recipe(self):
        fp = open(f"{self.runtime_asset_path}/databrew/{self.databrew_recipe_file_name}","r")
        self.databrew_recipe = databrew.CfnRecipe(
            self,
            self.databrew_recipe_name,
            name=self.databrew_recipe_name,
            steps=[
                databrew.CfnRecipe.RecipeStepProperty(
                    action= self.get_databrew_recipe_property(i)[0], 
                    condition_expressions=self.get_databrew_recipe_property(i)[1]
                    ) for i in json.load(fp)
                ]
        )
        CfnOutput(self, "Databrew recipe name", value=self.databrew_recipe.name)
        # recipe publish is not in CDK, using boto3 sdk as workaround which needs cli profile as input
        # TODO use the connector solution databrew custom resource https://github.com/aws-solutions/data-connectors-for-aws-clean-rooms/source/infrastructure
        databrew_recipe.publish_recipe(self.databrew_recipe_name, self.cli_profile)
    
    def add_databrew_job(self):
        self.databrew_job = databrew.CfnJob(
            self, 
            self.databrew_job_name,
            name=self.databrew_job_name,
            type="RECIPE",
            role_arn=self.role.role_arn,
            dataset_name = self.cfn_dataset.name,
            #log_subscription='ENABLED',
            recipe=databrew.CfnJob.RecipeProperty(
                 name=self.databrew_recipe_name,
                 version="LATEST_PUBLISHED"
                 # version="LATEST_WORKING"
                 ),
            outputs=[databrew.CfnJob.OutputProperty(
                location=databrew.CfnJob.S3LocationProperty(
                    bucket=self.data_bucket.bucket_name,
                    key=self.databrew_output_path
                    ),
                format="CSV",
                format_options=databrew.CfnJob.OutputFormatOptionsProperty(
                    csv=databrew.CfnJob.CsvOutputOptionsProperty(
                        delimiter=","
                    )
                ),
                overwrite=True,
                compression_format="GZIP",
                max_output_files=1
            )]
        )
        self.databrew_job.add_dependency(self.cfn_dataset)
        CfnOutput(self, "Databrew job name", value=self.databrew_job.name)
# KMS encryption #
    def set_kms_key(self) -> None:
        """
        Retrieves key from the alias
        """
        # self.kms_key = kms.Alias.from_alias_name(self, "myKMSKey", f"alias/{self.kms_key_alias}")
        self.kms_key = kms.Key.from_lookup(self, f"{self.project_prefix}-kms-key",
            alias_name=f"alias/{self.kms_key_alias}"
            )
        CfnOutput(self, "KMS arn used", value=self.kms_key.key_arn)
# SSM parameters #
    def add_ssm_parameters(self) -> None:
        """
        Creates the SSM parameters
        """
        # CHANGE THE LIMIT ROWS AS PER NEED
        producer_param_value = {
            "chunk_size": 100,
            "delimiter": ",",
            "encoding": "utf-8",
            "limit_rows": 1000000,
            "cli_profile": "Default",
            # enable this for lambda based architecture
            # "sqs_queue": self.address_val_sqs_queue.queue_url,
            "batch_mode": "chunk_size"
        }
        self.ssm_producer_param = ssm.StringParameter(
            self,
            "Producer_Parameter",
            parameter_name=self.ssm_producer_param_name,
            string_value=json.dumps(producer_param_value),
        )
        schema_map = {
                        "address_line1": "address1",
                        "address_line2": "address2",
                        "city": "city",
                        "state": "state_code",
                        "zip_code": "zip_code",
                        "country": "country"
                    }
        smarty_param_value = {
                "license_key": "us-core-cloud",
                "url": "https://us-street.api.smartystreets.com/street-address",
                "secret_name":self.AddressValidation_api_secret.secret_name,
                "region_name": self.region,
                "s3_bucket": self.data_bucket_name,
                "s3_key": f"{self.dataset_path_prefix}/{self.dataset_file_name}-validated",
                "schema_map": schema_map
                }
        self.ssm_smarty_param = ssm.StringParameter(
            self,
            "Smarty_Parameter",
            parameter_name=self.ssm_smarty_param_name,
            string_value=json.dumps(smarty_param_value)
        )
        awsloc_param_value = {
            "region_name": self.region,
            "s3_bucket": self.data_bucket_name,
            "s3_key": f"{self.dataset_path_prefix}/{self.dataset_file_name}-validated",
            "place_index": "venice-address-validation",
            "schema_map": schema_map
            }

        self.ssm_awsloc_param = ssm.StringParameter(
            self,
            "AWSLocation_Parameter",
            parameter_name=self.ssm_awsloc_param_name,
            string_value=json.dumps(awsloc_param_value),
        )
        CfnOutput(self, "Producer_SSM_Parameter", value=self.ssm_producer_param.parameter_name)
        CfnOutput(self, "Smarty_SSM_Parameter", value=self.ssm_smarty_param.parameter_name)
        CfnOutput(self, "AWSLocation_SSM_Parameter", value=self.ssm_awsloc_param.parameter_name)
### Below is needed only for a lambda based execution workflow as a substitute of the glue stack above
    def get_deny_non_ssl_policy(self, queue_arn):
        """
        Helper method that returns a IAM policy statement that denies non SSL calls to SQS queue
        To be used in SSL queue creation
        """
        return iam.PolicyStatement(
            sid='Enforce TLS for all principals',
            effect=iam.Effect.DENY,
            principals=[
                iam.AnyPrincipal(),
            ],
            actions=[
                'sqs:*',
            ],
            resources=[queue_arn],
            conditions={
                'Bool': {'aws:SecureTransport': 'false'},
            },
        )
# lamdba functions #
    def add_lambda_layers(self) -> None:
        """
        Creates aws wrangler lambda layer
        All custom lambda should be created in sync with 
        other libraries in the main lambda function. In other words, along with 
        the virtual environment setup, the zip file needs to be re-created
        Update compatible runtimes as needed
        """
        # FIXED layer classes are not getting detected when deployed through CDK, workaround is to manually create layers in console
        # Needed to zip the content under python folder
        # self.smarty_lambda_layer = _lambda.LayerVersion(self, "Smarty4.11.16-Python39",
        #     removal_policy=RemovalPolicy.RETAIN,
        #     code=_lambda.Code.from_asset(path.join(self.runtime_asset_path, "_lambda/layer.zip")),
        #     # As part of s3 deployment, these assets are already uploaded to s3
        #     # TODO referencing s3 instead of local asset
        #     # code=_lambda.Code.from_bucket(self.cdk_asset_bucket, "lambda/layer.zip",),
        #     compatible_architectures=[_lambda.Architecture.X86_64, _lambda.Architecture.ARM_64],
        #     compatible_runtimes=[self.lambda_runtime]
        # )
        # Note : this deployment assumes arm64 architecture. Change the dependencies like layers here if that changes
        # the version of AWS SDK for pandas is hard coded to 8 here. This may get upgraded and the deployment may fail. 
        self.wrangler_layer =_lambda.LayerVersion.from_layer_version_attributes(self, "AWSSDKPandas-Python39-Arm64",
           layer_version_arn=f"arn:aws:lambda:{self.region}:336392948345:layer:AWSSDKPandas-Python39-Arm64:8")
    
    def add_sqs_queue(self, queue_name: str) -> None:
        """
        Creates the sample SQS queue
        """
        sqs_queue = sqs.Queue(
            self,
            queue_name,
            queue_name=queue_name,
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=self.kms_key,
            visibility_timeout=Duration.seconds(900),
        )
        # Deny non SSL traffic
        sqs_queue.add_to_resource_policy(self.get_deny_non_ssl_policy(sqs_queue.queue_arn))

        CfnOutput(self, f"{queue_name}SQS Queue", value=sqs_queue.queue_url)
        return sqs_queue
 
    def add_producer_lambda_function(self) -> None:
        """
        Creates the sample lambda function with smarty sdk and aws wrangler layers
        """
        # create lambda
        self.producer_lambda = _lambda.Function(
            self, 
            "ProduceAddressValidationBatch-lambda",
            function_name="ProduceAddressValidationBatch",
            runtime=self.lambda_runtime,
            handler="produce_addr_val_batch_msgs.lambda_handler",
            # code=_lambda.Code.from_bucket(bucket=self.cdk_asset_bucket, key=f"{self.lambda_script_bucket_key}/{self.lambda_script}"),
            code=_lambda.Code.from_asset(path.join(self.deployment_packages_path, "producer_lambda_deployment.zip")),
            layers=[self.wrangler_layer],
            on_failure=destinations.SqsDestination(self.producer_dlq),
            max_event_age=Duration.hours(2),  # Optional: set the maxEventAge retry policy
            retry_attempts=2,
            timeout=Duration.minutes(15),
            role=self.role,
            environment={"SSM_PARAMETER": self.ssm_producer_param_name},
        )
        CfnOutput(self, "Producer_Lambda_Function", value=self.producer_lambda.function_arn)
    
    def add_smarty_lambda_function(self) -> None:
        """
        Creates the sample lambda function with smarty sdk and aws wrangler layers
        """
        # create lambda
        self.smarty_lambda = _lambda.Function(
            self, 
            "SmartyAddressValidationBatch-lambda",
            function_name="SmartyAddressValidationBatch",
            runtime=self.lambda_runtime,
            handler="smarty_addr_val.lambda_handler",
            # code=_lambda.Code.from_bucket(bucket=self.cdk_asset_bucket, key=f"{self.lambda_script_bucket_key}/{self.lambda_script}"),
            code=_lambda.Code.from_asset(path.join(self.deployment_packages_path, "smarty_lambda_deployment.zip")),
            layers=[self.wrangler_layer],
            on_failure=destinations.SqsDestination(self.address_val_dlq),
            max_event_age=Duration.hours(2),  # Optional: set the maxEventAge retry policy
            retry_attempts=2,
            timeout=Duration.minutes(15),
            role=self.role,
            environment={"SSM_PARAMETER": self.ssm_smarty_param_name},
        )
        self.smarty_lambda.add_event_source(event_sources.SqsEventSource(self.address_val_sqs_queue, batch_size=10))
        CfnOutput(self, "Smarty_Lambda_Function", value=self.smarty_lambda.function_arn)

    def add_awsloc_lambda_function(self) -> None:
        """
        Creates the sample lambda function with aws location sdk and aws wrangler layers
        """
        # create lambda
        self.awsloc_lambda = _lambda.Function(
            self, 
            "AWSLocationAddressValidation-lambda",
            function_name="AWSLocationAddressValidation",
            runtime=self.lambda_runtime,
            handler="awslocation_addr_val.lambda_handler",
            # code=_lambda.Code.from_bucket(bucket=self.cdk_asset_bucket, key=f"{self.lambda_script_bucket_key}/{self.lambda_script}"),
            code=_lambda.Code.from_asset(path.join(self.deployment_packages_path, "awsloc_lambda_deployment.zip")),
            layers=[self.wrangler_layer],
            on_failure=destinations.SqsDestination(self.address_val_dlq),
            max_event_age=Duration.hours(2),  # Optional: set the maxEventAge retry policy
            retry_attempts=2,
            timeout=Duration.minutes(15),
            role=self.role,
            environment={"SSM_PARAMETER": self.ssm_awsloc_param_name},
        )
        self.awsloc_lambda.add_event_source(event_sources.SqsEventSource(self.address_val_sqs_queue, batch_size=10))
        CfnOutput(self, "AWSLocation_Lambda_Function", value=self.awsloc_lambda.function_arn)
# Aws location place index # 
    def add_awsloc_place_index(self) -> None:
        """
        Creates the aws location place index
        """
        self.awsloc_place_index = aws_location.CfnPlaceIndex(
            self,
            "AWSLocationPlaceIndex",
            data_source="Esri",
            index_name=self.awsloc_place_index_name,
            description="Entity resolution Address Validation Place Index",
            data_source_configuration=aws_location.CfnPlaceIndex.DataSourceConfigurationProperty(
                intended_use="Storage"
            ),
        )
        CfnOutput(self, "AddressValidationPlaceIndex", value=self.awsloc_place_index.index_name)
# TODO SNS notification
# TODO full glue spark job