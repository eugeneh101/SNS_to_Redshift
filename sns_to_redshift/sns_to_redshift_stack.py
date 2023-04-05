from aws_cdk import (
    BundlingOptions,
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_kinesisfirehose as firehose,
    aws_lambda as _lambda,
    aws_redshift as redshift,
    aws_s3 as s3,
    aws_sns as sns,
    triggers,
)
from constructs import Construct


class SnsToRedshiftStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.sns_topic = sns.Topic(
            self, "SnsTopic", topic_name=environment["SNS_TOPIC"]
        )
        self.sns_role_to_put_messages_in_firehose = iam.Role(
            self,
            "SnsRoleToPutMessagesInFirehose",
            assumed_by=iam.ServicePrincipal("sns.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonSNSRole"
                ),
            ],
            inline_policies={
                "WriteToFirehose": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "firehose:DescribeDeliveryStream",
                                "firehose:ListDeliveryStreams",
                                "firehose:ListTagsForDeliveryStream",
                                "firehose:PutRecord",
                                "firehose:PutRecordBatch",
                            ],
                            resources=[
                                "arn:aws:firehose:{}:{}:deliverystream/*".format(
                                    environment["AWS_REGION"],
                                    environment["AWS_ACCOUNT"],
                                ),  ### later principle of least privileges
                            ],
                        ),
                    ],
                ),
            },
        )

        self.s3_bucket_for_firehose_to_redshift = s3.Bucket(
            self,
            "S3BucketForFirehoseToRedshift",
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

        self.default_vpc = ec2.Vpc.from_lookup(self, "DefaultVPC", is_default=True)
        self.default_security_group = ec2.SecurityGroup.from_lookup_by_name(
            self,
            "DefaultSecurityGroup",
            security_group_name="default",
            vpc=self.default_vpc,
        )
        self.security_group_for_firehose_to_redshift = ec2.SecurityGroup(
            self,
            "SecurityGroupForFirehoseToRedshift",
            vpc=self.default_vpc,
            allow_all_outbound=True,
        )
        self.security_group_for_firehose_to_redshift.add_ingress_rule(
            peer=ec2.Peer.ipv4(cidr_ip=environment["FIREHOSE_IP_ADDRESS"]),
            connection=ec2.Port.tcp(5439),
        )
        self.redshift_cluster = redshift.CfnCluster(
            self,
            "RedshiftCluster",
            cluster_type="single-node",  # for demo purposes
            number_of_nodes=1,  # for demo purposes
            node_type="dc2.large",  # for demo purposes
            db_name=environment["REDSHIFT_DATABASE_NAME"],
            master_username=environment["REDSHIFT_USER"],
            master_user_password=environment["REDSHIFT_PASSWORD"],
            # iam_roles=[self.redshift_full_commands_full_access_role.role_arn],
            # cluster_subnet_group_name=demo_cluster_subnet_group.ref,
            vpc_security_group_ids=[
                self.default_security_group.security_group_id,
                self.security_group_for_firehose_to_redshift.security_group_id,
            ],
            publicly_accessible=True,  # needed for Firehose
        )

        self.configure_redshift_for_firehose_lambda = _lambda.Function(  # will be used once in Trigger defined below
            self,  # create the schema and table in Redshift for DynamoDB CDC
            "ConfigureRedshiftForFirehoseLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/configure_redshift_for_firehose_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "pip install -r requirements.txt -t /asset-output",
                                "cp handler.py /asset-output",  # need to cp instead of mv
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(10),  # may take some time
            memory_size=128,  # in MB
            environment={
                "REDSHIFT_USER": environment["REDSHIFT_USER"],
                "REDSHIFT_PASSWORD": environment["REDSHIFT_PASSWORD"],
                "REDSHIFT_DATABASE_NAME": environment["REDSHIFT_DATABASE_NAME"],
                "REDSHIFT_SCHEMA_NAME_FOR_FIREHOSE": environment[
                    "REDSHIFT_SCHEMA_NAME_FOR_FIREHOSE"
                ],
                "REDSHIFT_TABLE_NAME_FOR_FIREHOSE": environment[
                    "REDSHIFT_TABLE_NAME_FOR_FIREHOSE"
                ],
            },
            vpc=self.default_vpc,
            allow_public_subnet=True,  ### might not do in real life
            security_groups=[
                self.default_security_group,
            ],
        )

        # connect the AWS resources
        self.configure_redshift_for_firehose_lambda.add_environment(
            key="REDSHIFT_ENDPOINT_ADDRESS",
            value=self.redshift_cluster.attr_endpoint_address,
        )
        self.trigger_configure_redshift_for_firehose_lambda = triggers.Trigger(
            self,
            "TriggerConfigureRedshiftForFirehoseLambda",
            handler=self.configure_redshift_for_firehose_lambda,  # this is underlying Lambda
            # runs once after Redshift cluster created
            execute_after=[self.redshift_cluster],
            # invocation_type=triggers.InvocationType.REQUEST_RESPONSE,
            # timeout=self.configure_redshift_for_firehose_lambda.timeout,
        )
        self.firehose_role = iam.Role(
            self,
            "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
            inline_policies={
                "WriteToS3": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "s3:AbortMultipartUpload",
                                "s3:GetBucketLocation",
                                "s3:GetObject",
                                "s3:ListBucket",
                                "s3:ListBucketMultipartUploads",
                                "s3:PutObject",
                            ],
                            resources=[
                                f"arn:aws:s3:::{environment['S3_BUCKET_NAME']}",
                                f"arn:aws:s3:::{environment['S3_BUCKET_NAME']}/*",
                            ],
                        ),
                    ],
                ),
                "WriteToCloudwatch": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["logs:PutLogEvents"],
                            resources=[
                                "arn:aws:logs:{}:{}:log-group:{}:log-stream:{}".format(
                                    environment["AWS_REGION"],
                                    environment["AWS_ACCOUNT"],
                                    f"aws/kinesisfirehose/{environment['FIREHOSE_NAME']}",
                                    "*",
                                ),
                            ],
                        ),
                    ],
                ),
                # {
                #    "Effect": "Allow",
                #    "Action": [
                #        "lambda:InvokeFunction",
                #        "lambda:GetFunctionConfiguration"
                #    ],
                #    "Resource": [
                #        "arn:aws:lambda:region:account-id:function:function-name:function-version"
                #    ]
                # }
            },
        )
        redshift_destination_configuration_property = firehose.CfnDeliveryStream.RedshiftDestinationConfigurationProperty(
            cluster_jdbcurl="jdbc:redshift://{}:{}/dev".format(
                self.redshift_cluster.attr_endpoint_address,
                self.redshift_cluster.attr_endpoint_port,
            ),
            copy_command=firehose.CfnDeliveryStream.CopyCommandProperty(
                data_table_name="{}.{}".format(  # needs Redshift info
                    environment["REDSHIFT_SCHEMA_NAME_FOR_FIREHOSE"],
                    environment["REDSHIFT_TABLE_NAME_FOR_FIREHOSE"],
                ),
                copy_options="json 'auto ignorecase'",
            ),
            username=environment["REDSHIFT_USER"],  # needs Redshift info
            password=environment["REDSHIFT_PASSWORD"],  # needs Redshift info
            role_arn=self.firehose_role.role_arn,  # needs role
            s3_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn=self.s3_bucket_for_firehose_to_redshift.bucket_arn,
                role_arn=self.firehose_role.role_arn,
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=60,
                    # size_in_mBs=1,
                ),
                # error_output_prefix="errorOutputPrefix",
                # prefix="prefix"
            ),
            cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                enabled=True,
                log_group_name=f"/aws/kinesisfirehose/{environment['FIREHOSE_NAME']}",
                log_stream_name="DestinationDelivery",  ### check if reasonable
            ),
            # processing_configuration=firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
            #     enabled=True,
            #     processors=[
            #         firehose.CfnDeliveryStream.ProcessorProperty(
            #             type="Lambda",
            #             # the properties below are optional
            #             parameters=[
            #                 firehose.CfnDeliveryStream.ProcessorParameterProperty(
            #                     parameter_name="LambdaArn",
            #                     parameter_value="arn:aws:lambda:us-east-1:638141915324:function:TransformFirehoseRecords:$LATEST",   ### hard coded
            #                 ),
            #             ],
            #         ),
            #     ],
            # ),
        )
        self.kinesis_firehose = firehose.CfnDeliveryStream(
            self,
            "KinesisFirehose",
            # delivery_stream_encryption_configuration_input=None,
            delivery_stream_name=environment["FIREHOSE_NAME"],
            delivery_stream_type="DirectPut",
            redshift_destination_configuration=redshift_destination_configuration_property,
            # the properties below are optional
            # retry_options=kinesisfirehose.CfnDeliveryStream.RedshiftRetryOptionsProperty(
            #     duration_in_seconds=123
            # ),
            ### allow for failed deliveries to save to S3
        )
        self.firehose_subscription = sns.Subscription(
            self,
            "FirehoseSubscription",
            topic=self.sns_topic,  # needs SNS topic
            raw_message_delivery=True,
            endpoint=self.kinesis_firehose.attr_arn,
            protocol=sns.SubscriptionProtocol.FIREHOSE,
            subscription_role_arn=self.sns_role_to_put_messages_in_firehose.role_arn,
        )


        # write Cloudformation Outputs
        self.output_sns_topic_name = CfnOutput(
            self,
            "SnsTopicName",  # Output omits underscores and hyphens
            value=self.sns_topic.topic_name,
        )
        self.output_redshift_endpoint_address = CfnOutput(
            self,
            "RedshiftEndpointAddress",  # Output omits underscores and hyphens
            value=self.redshift_cluster.attr_endpoint_address,
        )
        self.output_firehose_endpoint = CfnOutput(
            self,
            "FirehoseEndpoint",  # Output omits underscores and hyphens
            value=self.kinesis_firehose.attr_arn,
        )
        self.output_s3_bucket_name = CfnOutput(
            self,
            "S3BucketName",  # Output omits underscores and hyphens
            value=self.s3_bucket_for_firehose_to_redshift.bucket_name,
        )

