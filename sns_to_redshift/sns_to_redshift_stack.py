import json

from aws_cdk import (
    CfnOutput,
    Duration,
    NestedStack,
    RemovalPolicy,
    SecretValue,
    Stack,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_kinesisfirehose as firehose,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_redshift as redshift,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_sns as sns,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    triggers,
)
from constructs import Construct


class PreexistingStack(NestedStack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.redshift_role = iam.Role(
            self,
            "RedshiftRole",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            role_name=environment["REDSHIFT_ROLE_NAME"],
        )
        self.redshift_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=["*"],
            ),
        )
        self.publish_to_sns_role = iam.Role(
            self,
            "PublishToSnsRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"  # write Cloudwatch logs
                ),
            ],
        )
        self.publish_to_sns_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=["sns:Publish"],
                resources=["*"],
            ),
        )

        self.redshift_cluster = redshift.CfnCluster(
            self,
            "RedshiftCluster",
            cluster_type="single-node",  # for demo purposes
            number_of_nodes=1,  # for demo purposes
            node_type="dc2.large",  # for demo purposes
            cluster_identifier=environment["REDSHIFT_CLUSTER_NAME"],
            db_name=environment["REDSHIFT_DATABASE_NAME"],
            master_username=environment["REDSHIFT_USER"],
            master_user_password=environment["REDSHIFT_PASSWORD"],
            iam_roles=[self.redshift_role.role_arn],
            publicly_accessible=False,
        )
        self.redshift_secret = secretsmanager.Secret(
            self,
            "RedshiftSecret",
            secret_name=environment["REDSHIFT_SECRET_NAME"],
            secret_object_value={
                "username": SecretValue.unsafe_plain_text(environment["REDSHIFT_USER"]),
                "password": SecretValue.unsafe_plain_text(
                    environment["REDSHIFT_PASSWORD"]
                ),
            },
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.sns_topic = sns.Topic(
            self, "SnsTopic", topic_name=environment["SNS_TOPIC_NAME"]
        )

        self.scheduled_eventbridge_event = events.Rule(
            self,
            "RunPeriodically",
            event_bus=None,  # scheduled events must be on "default" bus
            schedule=events.Schedule.rate(
                Duration.minutes(environment["SNS_GENERATE_MESSAGES_EVERY_X_MINUTES"])
            ),
        )

        self.publish_sns_messages_lambda = _lambda.Function(
            self,
            "PublishSnsMessages",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/publish_sns_messages_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be instantaneous
            memory_size=128,
            environment={
                "SNS_NUM_MESSAGES": json.dumps(environment["SNS_NUM_MESSAGES"]),
                "SNS_TOPIC_ARN": (
                    f"arn:aws:sns:{environment['AWS_REGION']}:"
                    f"{environment['AWS_ACCOUNT']}:{environment['SNS_TOPIC_NAME']}"
                ),
            },
            role=self.publish_to_sns_role,
        )

        # connect the AWS resources
        self.scheduled_eventbridge_event.add_target(
            events_targets.LambdaFunction(
                handler=self.publish_sns_messages_lambda,
            )
        )


class UpgradeStack(NestedStack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

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
                    # "s3:AbortMultipartUpload",
                    # "s3:GetBucketLocation",
                    # "s3:GetObject",
                    # "s3:ListBucket",
                    # "s3:ListBucketMultipartUploads",
                    "s3:PutObject",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            ),
        )
        self.lambda_redshift_access_role = iam.Role(
            self,
            "LambdaRedshiftAccessRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"  # write Cloudwatch logs
                ),
            ],
        )
        # for `configure_redshift_table_lambda` and `truncate_and_load_redshift_table_lambda`
        self.lambda_redshift_access_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    # for `configure_redshift_table_lambda`
                    "redshift-data:DescribeStatement",
                    # for `configure_redshift_table_lambda` and `truncate_and_load_redshift_table_lambda`
                    "redshift:GetClusterCredentials",
                    "redshift-data:ExecuteStatement",
                    "redshift-data:BatchExecuteStatement",
                    "secretsmanager:DescribeSecret",  # needed to get secret ARN
                    "secretsmanager:GetSecretValue",  # needed for authenticating BatchExecuteStatement
                    # for `truncate_and_load_redshift_table_lambda`
                    "iam:GetRole",  # needed to get Redshift role ARN
                ],
                resources=["*"],
            ),
        )
        # for `move_s3_files_to_processing_folder_lambda` and `redshift_queries_finished_lambda`
        self.lambda_redshift_access_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "s3:GetObject*",
                    # "s3:GetBucket*",
                    "s3:List*",
                    "s3:DeleteObject*",
                    "s3:PutObject",
                    # "s3:PutObjectLegalHold",
                    # "s3:PutObjectRetention",
                    # "s3:PutObjectTagging",
                    # "s3:PutObjectVersionTagging",
                    # "s3:Abort*",
                ],
                resources=["*"],
            ),
        )
        # for `truncate_and_load_redshift_table_lambda`
        self.lambda_redshift_access_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=["dynamodb:PutItem"],
                resources=["*"],
            ),
        )
        # for `redshift_queries_finished_lambda`
        self.lambda_redshift_access_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "redshift-data:GetStatementResult",
                    "states:SendTaskSuccess",
                    "states:SendTaskFailure",
                    "dynamodb:UpdateItem",
                    "dynamodb:Query",
                ],
                resources=["*"],
            ),
        )

        self.scheduled_eventbridge_event = events.Rule(
            self,
            "RunPeriodically",
            event_bus=None,  # scheduled events must be on "default" bus
            schedule=events.Schedule.rate(
                Duration.minutes(environment["REDSHIFT_LOAD_EVERY_X_MINUTES"])
            ),
        )
        self.event_rule_to_trigger_redshift_queries_finished_lambda = events.Rule(
            self,
            "EventRuleToTriggerRedshiftQueriesFinishedLambda",
        )

        self.sns_topic = sns.Topic.from_topic_arn(
            self,
            "SnsTopic",
            topic_arn=(
                f"arn:aws:sns:{environment['AWS_REGION']}:"
                f"{environment['AWS_ACCOUNT']}:{environment['SNS_TOPIC_NAME']}"
            ),
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

        self.dynamodb_table = dynamodb.Table(
            self,
            "DynamoDBTableForRedshiftQueries",
            table_name=environment["DYNAMODB_TABLE_NAME"],
            partition_key=dynamodb.Attribute(
                name="full_table_name", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="utc_now_human_readable", type=dynamodb.AttributeType.STRING
            ),
            time_to_live_attribute="delete_record_on",
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.dynamodb_table.add_global_secondary_index(
            index_name="is_still_processing_sql",  # hard coded
            partition_key=dynamodb.Attribute(
                name="redshift_queries_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="is_still_processing_sql?", type=dynamodb.AttributeType.STRING
            ),
        )

        # will be used once in Trigger defined below
        self.configure_redshift_table_lambda = _lambda.Function(
            self,  # create the schema and table in Redshift
            "ConfigureRedshiftTable",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/configure_redshift_table_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(10),  # may take some time
            memory_size=128,  # in MB
            environment={
                "REDSHIFT_CLUSTER_NAME": environment["REDSHIFT_CLUSTER_NAME"],
                "REDSHIFT_DATABASE_NAME": environment["REDSHIFT_DATABASE_NAME"],
                "REDSHIFT_SCHEMA_NAME": environment["REDSHIFT_SCHEMA_NAME"],
                "REDSHIFT_SECRET_NAME": environment["REDSHIFT_SECRET_NAME"],
                "REDSHIFT_TABLE_NAME": environment["REDSHIFT_TABLE_NAME"],
            },
            role=self.lambda_redshift_access_role,
            retry_attempts=0,
        )
        self.move_s3_files_to_processing_folder_lambda = _lambda.Function(
            self,
            "MoveS3FilesToProcessingFolder",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/move_s3_files_to_processing_folder_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(30),  # depends on number of files to move
            memory_size=128,  # in MB
            environment={
                "S3_BUCKET_NAME": environment["S3_BUCKET_NAME"],
                "S3_BUCKET_PREFIX": f"sns_source/topic={environment['SNS_TOPIC_NAME']}/unprocessed/",
            },
            role=self.lambda_redshift_access_role,
            retry_attempts=0,
        )
        self.truncate_and_load_redshift_table_lambda = _lambda.Function(
            self,
            "TruncateAndLoadRedshiftTable",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/truncate_and_load_redshift_table_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be instantaneous
            memory_size=128,  # in MB
            environment={
                "DYNAMODB_TABLE_NAME": environment["DYNAMODB_TABLE_NAME"],
                "DYNAMODB_TTL_IN_DAYS": json.dumps(environment["DYNAMODB_TTL_IN_DAYS"]),
                "FILE_TYPE": environment["FILE_TYPE"],
                "REDSHIFT_CLUSTER_NAME": environment["REDSHIFT_CLUSTER_NAME"],
                "REDSHIFT_COPY_ADDITIONAL_ARGUMENTS": environment[
                    "REDSHIFT_COPY_ADDITIONAL_ARGUMENTS"
                ],
                "REDSHIFT_DATABASE_NAME": environment["REDSHIFT_DATABASE_NAME"],
                "REDSHIFT_ROLE_NAME": environment["REDSHIFT_ROLE_NAME"],
                "REDSHIFT_SCHEMA_NAME": environment["REDSHIFT_SCHEMA_NAME"],
                "REDSHIFT_SECRET_NAME": environment["REDSHIFT_SECRET_NAME"],
                "REDSHIFT_TABLE_NAME": environment["REDSHIFT_TABLE_NAME"],
                "S3_BUCKET_NAME": environment["S3_BUCKET_NAME"],
                "TRUNCATE_TABLE": json.dumps(environment["TRUNCATE_TABLE"]),
            },
            role=self.lambda_redshift_access_role,
            retry_attempts=0,
        )
        self.redshift_queries_finished_lambda = _lambda.Function(
            self,
            "RedshiftQueriesFinished",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/redshift_queries_finished_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be instantaneous
            memory_size=128,  # in MB
            environment={
                "DYNAMODB_TABLE_NAME": environment["DYNAMODB_TABLE_NAME"],
                "S3_BUCKET_NAME": environment["S3_BUCKET_NAME"],
            },
            role=self.lambda_redshift_access_role,
            retry_attempts=0,
        )

        # Step Function definition
        move_s3_files_to_processing_folder = sfn_tasks.LambdaInvoke(
            self,
            "move_s3_files_to_processing_folder",
            lambda_function=self.move_s3_files_to_processing_folder_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "Execution.$": "$$.Execution.Name"
                }  # maybe figure out what other input payload would be
            ),
            payload_response_only=True,
            timeout=self.move_s3_files_to_processing_folder_lambda.timeout,
            retry_on_service_exceptions=False,
        )
        empty_manifest_file = sfn.Succeed(self, "empty_manifest_file")
        truncate_and_load_redshift_table = sfn_tasks.LambdaInvoke(
            self,
            "truncate_and_load_redshift_table",
            lambda_function=self.truncate_and_load_redshift_table_lambda,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            payload=sfn.TaskInput.from_object(
                {
                    "redshift_manifest_file_name.$": "$.redshift_manifest_file_name",
                    "s3_prefix_processing.$": "$.s3_prefix_processing",
                    "task_token": sfn.JsonPath.task_token,
                }
            ),
            timeout=Duration.minutes(environment["REDSHIFT_LOAD_EVERY_X_MINUTES"]),
            retry_on_service_exceptions=False,
        )
        self.state_machine = sfn.StateMachine(
            self,
            "truncate_and_load_redshift_table_with_task_token",
            definition=move_s3_files_to_processing_folder.next(
                sfn.Choice(self, "non-empty_manifest_file?")
                .when(
                    sfn.Condition.is_present(variable="$.redshift_manifest_file_name"),
                    truncate_and_load_redshift_table,
                )
                .otherwise(empty_manifest_file)
            ),
            # role=self.lambda_redshift_access_role,  # somehow creates circular dependency
        )

        # connect the AWS resources
        self.trigger_configure_redshift_table_lambda = triggers.Trigger(
            self,
            "TriggerConfigureRedshiftTableLambda",
            handler=self.configure_redshift_table_lambda,  # this is underlying Lambda
            # runs once after Redshift cluster created
            execute_before=[self.scheduled_eventbridge_event],
            # invocation_type=triggers.InvocationType.REQUEST_RESPONSE,
            # timeout=self.configure_redshift_table_lambda.timeout,
        )
        s3_destination_configuration_property = firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
            bucket_arn=self.s3_bucket_for_sns_messages.bucket_arn,  # connect AWS resource
            role_arn=self.firehose_write_to_s3_role.role_arn,  # connect AWS resource
            # the properties below are optional
            buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                interval_in_seconds=60, size_in_m_bs=1  ### parametrize
            ),
            cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                enabled=True,
                log_group_name="/aws/kinesisfirehose/firehose-to-s3-cdk",  # hard coded
                log_stream_name="DestinationDelivery",  # hard coded
            ),
            prefix=f"sns_source/topic={environment['SNS_TOPIC_NAME']}/unprocessed/",  # hard coded
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
        self.scheduled_eventbridge_event.add_target(
            target=events_targets.SfnStateMachine(
                self.state_machine,
                # input=events.RuleTargetInput.from_object({"SomeParam": "SomeValue"}),
                # dead_letter_queue=dlq,
                # role=role
            )
        )
        self.event_rule_to_trigger_redshift_queries_finished_lambda.add_event_pattern(
            source=["aws.redshift-data"],
            resources=events.Match.suffix(environment["REDSHIFT_CLUSTER_NAME"]),
            detail={
                "principal": [
                    {
                        "suffix": self.truncate_and_load_redshift_table_lambda.function_name
                    }
                ],
                "statementId": [{"exists": True}],
                "state": [{"exists": True}],
            },
            # detail=["arn:aws:sts::...:assumed-role/LoadRedshiftWithSfnStack-LambdaRedshiftFullAccessR-1BCCVKE7JB2LH/LoadRedshiftWithSfnStack-TruncateAndLoadRedshiftTa-NhdJFrmC13Nl"],
            detail_type=["Redshift Data Statement Status Change"],
        )
        self.event_rule_to_trigger_redshift_queries_finished_lambda.add_target(
            events_targets.LambdaFunction(
                handler=self.redshift_queries_finished_lambda,
                # retry_attempts=0,  ### doesn't seem to do anything
                ### then put in DLQ
            )
        )


class SnsToRedshiftStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        kwargs.pop("env")  # NestedStack does not have `env` argument
        self.preexisting_stack = PreexistingStack(
            self, "PreexistingStack", environment=environment, **kwargs
        )
        self.upgrade_stack = UpgradeStack(
            self,
            "UpgradeStack",
            environment=environment,
            **kwargs,
        )
        self.upgrade_stack.node.add_dependency(
            self.preexisting_stack
        )  # preexisting stack is deployed first


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
