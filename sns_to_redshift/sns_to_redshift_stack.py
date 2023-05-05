from aws_cdk import (
    # BundlingOptions,
    CfnOutput,
    # Duration,
    RemovalPolicy,
    Stack,
    # aws_ec2 as ec2,
    aws_kinesisfirehose as firehose,
    aws_iam as iam,
    # aws_lambda as _lambda,
    # aws_redshift as redshift,
    aws_s3 as s3,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
    # aws_sqs as sqs,
    # triggers,
)
from constructs import Construct


class SnsToRedshiftStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # self.sns_write_to_firehose_role = iam.Role.from_role_name(
        #      self, "SnsWriteToFirehoseRole", role_name="RoleForSnsSubToPutRecordsInKinesis"
        # )
        self.sns_write_to_firehose_role = iam.Role(
            self,
            "SnsWriteToFirehoseRole",
            assumed_by=iam.ServicePrincipal("sns.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonSNSRole"  # write Cloudwatch logs
                ),
            ],
        )
        self.sns_write_to_firehose_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "firehose:DescribeDeliveryStream",
                    "firehose:ListDeliveryStreams",
                    "firehose:ListTagsForDeliveryStream",
                    "firehose:PutRecord",
                    "firehose:PutRecordBatch",
                ],
                resources=["*"],
            ),
        )
        self.firehose_write_to_s3_role = iam.Role(
            self,
            "FirehoseWriteToS3Role",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )
        self.firehose_write_to_s3_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:PutObject",
                ],
                resources=["*"],
            ),
        )
        self.firehose_write_to_s3_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=["logs:PutLogEvents"],
                resources=["*"],
            ),
        )

        self.sns_topic = sns.Topic(
            self, "SnsTopic", topic_name=environment["SNS_TOPIC"]
        )

        self.s3_bucket_for_sns_messages = s3.Bucket(
            self,
            "S3BucketForSnsMessages",
            bucket_name=environment["S3_BUCKET_NAME"],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=False,  # if versioning disabled, then expired files are deleted
            # lifecycle_rules=[
            #     s3.LifecycleRule(
            #         id="expire_files_with_certain_prefix_after_1_day",
            #         expiration=Duration.days(1),
            #         prefix=f"{environment['PROCESSED_DYNAMODB_STREAM_FOLDER']}/",
            #     ),
            # ],
        )

        # self.default_vpc = ec2.Vpc.from_lookup(self, "DefaultVPC", is_default=True)
        # self.default_security_group = ec2.SecurityGroup.from_lookup_by_name(
        #     self,
        #     "DefaultSecurityGroup",
        #     security_group_name="default",
        #     vpc=self.default_vpc,
        # )
        # self.security_group_for_lambda = ec2.SecurityGroup(  ### temporary, should delete later
        #     self,
        #     "SecurityGroupForLambda",
        #     vpc=self.default_vpc,
        #     allow_all_outbound=True,
        # )
        # self.security_group_for_lambda.add_ingress_rule(
        #     peer=ec2.Peer.any_ipv4(),
        #     connection=ec2.Port.tcp(environment["REDSHIFT_PORT"]),
        # )
        # self.redshift_cluster = redshift.CfnCluster(
        #     self,
        #     "RedshiftCluster",
        #     cluster_type="single-node",  # for demo purposes
        #     number_of_nodes=1,  # for demo purposes
        #     node_type="dc2.large",  # for demo purposes
        #     db_name=environment["REDSHIFT_DATABASE_NAME"],
        #     master_username=environment["REDSHIFT_USER"],
        #     master_user_password=environment["REDSHIFT_PASSWORD"],
        #     # iam_roles=[self.redshift_full_commands_full_access_role.role_arn],
        #     # cluster_subnet_group_name=demo_cluster_subnet_group.ref,
        #     vpc_security_group_ids=[
        #         self.default_security_group.security_group_id,
        #         self.security_group_for_lambda.security_group_id,
        #     ],
        #     publicly_accessible=True,  ### change later
        # )

        # self.queue_for_sns_messages = sqs.Queue(  ### need DLQ
        #     self,
        #     "QueueForSnsMessages",
        #     removal_policy=RemovalPolicy.DESTROY,
        #     retention_period=Duration.days(4),
        #     visibility_timeout=Duration.seconds(1),  # retry failed message quickly
        # )

        # self.pull_from_sqs_and_write_to_s3_lambda = _lambda.Function(
        #     self,
        #     "PullFromSqsAndWriteToS3Lambda",
        #     runtime=_lambda.Runtime.PYTHON_3_9,
        #     code=_lambda.Code.from_asset(
        #         "lambda_code/pull_from_sqs_and_write_to_s3_lambda",
        #         exclude=[".venv/*"],
        #     ),
        #     handler="handler.lambda_handler",
        #     timeout=Duration.seconds(60),  ### may take some time, may make this configurable
        #     memory_size=128,  # in MB
        #     # vpc=self.default_vpc,
        #     # allow_public_subnet=True,  ### might not do in real life
        #     # security_groups=[
        #     #     self.default_security_group,
        #     # ],
        # )

        # self.configure_redshift_lambda = _lambda.Function(  # will be used once in Trigger defined below
        #     self,  # create the schema and table in Redshift for DynamoDB CDC
        #     "ConfigureRedshiftLambda",
        #     runtime=_lambda.Runtime.PYTHON_3_9,
        #     code=_lambda.Code.from_asset(
        #         "lambda_code/configure_redshift_lambda",
        #         # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
        #         bundling=BundlingOptions(
        #             image=_lambda.Runtime.PYTHON_3_9.bundling_image,
        #             command=[
        #                 "bash",
        #                 "-c",
        #                 " && ".join(
        #                     [
        #                         "pip install -r requirements.txt -t /asset-output",
        #                         "cp handler.py /asset-output",  # need to cp instead of mv
        #                     ]
        #                 ),
        #             ],
        #         ),
        #     ),
        #     handler="handler.lambda_handler",
        #     timeout=Duration.seconds(10),  # may take some time
        #     memory_size=128,  # in MB
        #     environment={
        #         "REDSHIFT_USER": environment["REDSHIFT_USER"],
        #         "REDSHIFT_PASSWORD": environment["REDSHIFT_PASSWORD"],
        #         "REDSHIFT_DATABASE_NAME": environment["REDSHIFT_DATABASE_NAME"],
        #         "REDSHIFT_SCHEMA_NAME": environment["REDSHIFT_SCHEMA_NAME"],
        #         "REDSHIFT_TABLE_NAME": environment["REDSHIFT_TABLE_NAME"],
        #     },
        #     # vpc=self.default_vpc,
        #     # allow_public_subnet=True,  ### might not do in real life
        #     # security_groups=[
        #     #     self.default_security_group,
        #     # ],
        # )

        # connect the AWS resources
        s3_destination_configuration_property = firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
            bucket_arn=self.s3_bucket_for_sns_messages.bucket_arn,  # need to connect AWS resource
            role_arn=self.firehose_write_to_s3_role.role_arn,  # need to connect AWS resource
            # the properties below are optional
            buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                interval_in_seconds=60,  # size_in_mBs=1
            ),
            cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                enabled=True,
                log_group_name="/aws/kinesisfirehose/firehose-to-s3-cdk",  # hard coded
                log_stream_name="DestinationDelivery",  # hard coded
            ),
            prefix=f"sns_source/unprocessed/{self.sns_topic.topic_name}",  # need to connect AWS resource
            # do we need processor?
            # error_output_prefix="errorOutputPrefix",
            # compression_format="compressionFormat",
        )
        self.firehose_with_s3_target = firehose.CfnDeliveryStream(
            self,
            "FirehoseToS3",
            s3_destination_configuration=s3_destination_configuration_property,
            delivery_stream_name="firehose-to-s3-cdk",
        )
        self.firehose_subscription = sns.Subscription(
            self,
            "FirehoseSubscription",
            topic=self.sns_topic,
            endpoint=self.firehose_with_s3_target.attr_arn,
            protocol=sns.SubscriptionProtocol.FIREHOSE,
            subscription_role_arn=self.sns_write_to_firehose_role.role_arn,
            raw_message_delivery=True,
            # dead_letter_queue=None,
        )
        # self.trigger_configure_redshift_lambda = triggers.Trigger(
        #     self,
        #     "TriggerConfigureRedshiftLambda",
        #     handler=self.configure_redshift_lambda,  # this is underlying Lambda
        #     # runs once after Redshift cluster created
        #     execute_after=[self.redshift_cluster],  ### in reality, Redshift Cluster would have already been created
        #     # invocation_type=triggers.InvocationType.REQUEST_RESPONSE,
        #     # timeout=self.configure_redshift_lambda.timeout,
        # )
        # self.configure_redshift_lambda.add_environment(
        #     key="REDSHIFT_ENDPOINT_ADDRESS",
        #     value=self.redshift_cluster.attr_endpoint_address,
        # )
        # self.sns_topic.add_subscription(
        #     topic_subscription=sns_subs.SqsSubscription(
        #         self.queue_for_sns_messages, raw_message_delivery=True
        #     ),
        # )
        # self.pull_from_sqs_and_write_to_s3_lambda.add_environment(
        #     key="QUEUE_NAME",
        #     value=self.queue_for_sns_messages.queue_name,
        # )
        # self.pull_from_sqs_and_write_to_s3_lambda.add_environment(
        #     key="BUCKET_NAME",
        #     value=self.s3_bucket_for_sqs_to_redshift.bucket_name,
        # )

        ### eventually create an existing Stack and new Stack

        # write Cloudformation Outputs
        self.output_sns_topic_name = CfnOutput(
            self,
            "SnsTopicName",  # Output omits underscores and hyphens
            value=self.sns_topic.topic_name,
        )
        self.output_firehose_name = CfnOutput(
            self,
            "FirehoseName",
            value=self.firehose_with_s3_target.delivery_stream_name,
        )
        self.output_s3_bucket_name = CfnOutput(
            self,
            "S3BucketName",  # Output omits underscores and hyphens
            value=self.s3_bucket_for_sns_messages.bucket_name,
        )
        # self.output_redshift_endpoint_address = CfnOutput(
        #     self,
        #     "RedshiftEndpointAddress",  # Output omits underscores and hyphens
        #     value=self.redshift_cluster.attr_endpoint_address,
        # )
