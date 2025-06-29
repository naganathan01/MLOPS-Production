# infrastructure/cdk/mlops_stack.py
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_kinesis as kinesis,
    aws_kinesisanalytics_v2 as kinesisanalytics,
    aws_kinesisfirehose as firehose,
    aws_s3 as s3,
    aws_iam as iam,
    aws_sagemaker as sagemaker,
    aws_cloudwatch as cloudwatch,
    aws_sns as sns,
    aws_events as events,
    aws_events_targets as targets,
    aws_apigateway as apigateway,
    aws_dynamodb as dynamodb,
    aws_secretsmanager as secrets,
    Duration,
    RemovalPolicy
)
from aws_cdk.aws_lambda_event_sources import KinesisEventSource
from constructs import Construct
import json

class MLOpsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, environment: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.environment = environment
        
        # Core infrastructure
        self.create_storage_resources()
        self.create_streaming_resources()
        self.create_compute_resources()
        self.create_ml_resources()
        self.create_monitoring_resources()
        self.create_api_resources()
        
    def create_storage_resources(self):
        """Create S3 buckets and DynamoDB tables"""
        
        # S3 bucket for data lake
        self.data_bucket = s3.Bucket(
            self, f"DataBucket{self.environment.title()}",
            bucket_name=f"mlops-failure-prediction-data-{self.environment}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ArchiveOldData",
                    enabled=True,
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30)
                        ),
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90)
                        )
                    ]
                )
            ],
            removal_policy=RemovalPolicy.RETAIN if self.environment == "prod" else RemovalPolicy.DESTROY
        )
        
        # S3 bucket for model artifacts
        self.model_bucket = s3.Bucket(
            self, f"ModelBucket{self.environment.title()}",
            bucket_name=f"mlops-model-artifacts-{self.environment}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.RETAIN if self.environment == "prod" else RemovalPolicy.DESTROY
        )
        
        # DynamoDB table for metadata
        self.metadata_table = dynamodb.Table(
            self, f"MetadataTable{self.environment.title()}",
            table_name=f"mlops-metadata-{self.environment}",
            partition_key=dynamodb.Attribute(
                name="id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True if self.environment == "prod" else False,
            removal_policy=RemovalPolicy.RETAIN if self.environment == "prod" else RemovalPolicy.DESTROY
        )
        
        # DynamoDB table for predictions cache
        self.predictions_table = dynamodb.Table(
            self, f"PredictionsTable{self.environment.title()}",
            table_name=f"mlops-predictions-{self.environment}",
            partition_key=dynamodb.Attribute(
                name="server_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="prediction_timestamp",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
            removal_policy=RemovalPolicy.DESTROY
        )
    
    def create_streaming_resources(self):
        """Create Kinesis streams and analytics"""
        
        # Kinesis Data Stream for real-time metrics
        self.metrics_stream = kinesis.Stream(
            self, f"MetricsStream{self.environment.title()}",
            stream_name=f"mlops-metrics-stream-{self.environment}",
            shard_count=2 if self.environment == "dev" else 5,
            retention_period=Duration.days(1)
        )
        
        # Kinesis Data Stream for predictions
        self.predictions_stream = kinesis.Stream(
            self, f"PredictionsStream{self.environment.title()}",
            stream_name=f"mlops-predictions-stream-{self.environment}",
            shard_count=1 if self.environment == "dev" else 3,
            retention_period=Duration.days(1)
        )
        
        # Kinesis Firehose for data archival
        self.data_firehose = firehose.DeliveryStream(
            self, f"DataFirehose{self.environment.title()}",
            delivery_stream_name=f"mlops-data-firehose-{self.environment}",
            destinations=[
                firehose.Destination.s3(
                    bucket=self.data_bucket,
                    prefix="raw-data/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/",
                    error_output_prefix="errors/",
                    buffering_interval=Duration.minutes(5),
                    buffering_size=cdk.Size.mebibytes(5)
                )
            ]
        )
    
    def create_compute_resources(self):
        """Create Lambda functions and execution roles"""
        
        # Shared IAM role for Lambda functions
        self.lambda_execution_role = iam.Role(
            self, f"LambdaExecutionRole{self.environment.title()}",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole")
            ]
        )
        
        # Grant permissions to Lambda role
        self.data_bucket.grant_read_write(self.lambda_execution_role)
        self.model_bucket.grant_read(self.lambda_execution_role)
        self.metrics_stream.grant_read_write(self.lambda_execution_role)
        self.predictions_stream.grant_read_write(self.lambda_execution_role)
        self.metadata_table.grant_read_write_data(self.lambda_execution_role)
        self.predictions_table.grant_read_write_data(self.lambda_execution_role)
        
        # Data Collector Lambda
        self.data_collector_function = _lambda.Function(
            self, f"DataCollectorFunction{self.environment.title()}",
            function_name=f"mlops-data-collector-{self.environment}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions/data_collector"),
            timeout=Duration.minutes(5),
            memory_size=512,
            role=self.lambda_execution_role,
            environment={
                "ENVIRONMENT": self.environment,
                "KINESIS_STREAM_NAME": self.metrics_stream.stream_name,
                "FIREHOSE_STREAM_NAME": self.data_firehose.delivery_stream_name,
                "METADATA_TABLE": self.metadata_table.table_name
            }
        )
        
        # Prediction Processor Lambda
        self.prediction_processor_function = _lambda.Function(
            self, f"PredictionProcessorFunction{self.environment.title()}",
            function_name=f"mlops-prediction-processor-{self.environment}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions/prediction_processor"),
            timeout=Duration.minutes(5),
            memory_size=1024,
            role=self.lambda_execution_role,
            environment={
                "ENVIRONMENT": self.environment,
                "SAGEMAKER_ENDPOINT_NAME": f"mlops-failure-prediction-{self.environment}",
                "PREDICTIONS_STREAM": self.predictions_stream.stream_name,
                "PREDICTIONS_TABLE": self.predictions_table.table_name
            }
        )
        
        # Model Monitor Lambda
        self.model_monitor_function = _lambda.Function(
            self, f"ModelMonitorFunction{self.environment.title()}",
            function_name=f"mlops-model-monitor-{self.environment}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions/model_monitor"),
            timeout=Duration.minutes(10),
            memory_size=1024,
            role=self.lambda_execution_role,
            environment={
                "ENVIRONMENT": self.environment,
                "DATA_BUCKET": self.data_bucket.bucket_name,
                "METADATA_TABLE": self.metadata_table.table_name
            }
        )
        
        # Add Kinesis event source to prediction processor
        self.prediction_processor_function.add_event_source(
            KinesisEventSource(
                stream=self.metrics_stream,
                starting_position=_lambda.StartingPosition.LATEST,
                batch_size=10,
                parallelization_factor=2
            )
        )
    
    def create_ml_resources(self):
        """Create SageMaker resources"""
        
        # SageMaker execution role
        self.sagemaker_execution_role = iam.Role(
            self, f"SageMakerExecutionRole{self.environment.title()}",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess")
            ]
        )
        
        # Grant S3 access to SageMaker
        self.data_bucket.grant_read_write(self.sagemaker_execution_role)
        self.model_bucket.grant_read_write(self.sagemaker_execution_role)
        
        # SageMaker Model Package Group
        self.model_package_group = sagemaker.CfnModelPackageGroup(
            self, f"ModelPackageGroup{self.environment.title()}",
            model_package_group_name=f"mlops-failure-prediction-{self.environment}",
            model_package_group_description="Model package group for failure prediction models"
        )
    
    def create_monitoring_resources(self):
        """Create CloudWatch and SNS resources"""
        
        # SNS topic for critical alerts
        self.critical_alerts_topic = sns.Topic(
            self, f"CriticalAlertsTopic{self.environment.title()}",
            topic_name=f"mlops-critical-alerts-{self.environment}",
            display_name="MLOps Critical Alerts"
        )
        
        # SNS topic for warnings
        self.warning_alerts_topic = sns.Topic(
            self, f"WarningAlertsTopic{self.environment.title()}",
            topic_name=f"mlops-warning-alerts-{self.environment}",
            display_name="MLOps Warning Alerts"
        )
        
        # Grant SNS publish permissions
        self.critical_alerts_topic.grant_publish(self.prediction_processor_function)
        self.warning_alerts_topic.grant_publish(self.prediction_processor_function)
        
        # CloudWatch Dashboard
        self.dashboard = cloudwatch.Dashboard(
            self, f"MLOpsDashboard{self.environment.title()}",
            dashboard_name=f"MLOps-FailurePrediction-{self.environment}"
        )
        
        # Add dashboard widgets
        self.create_dashboard_widgets()
        
        # CloudWatch Alarms
        self.create_cloudwatch_alarms()
    
    def create_dashboard_widgets(self):
        """Create CloudWatch dashboard widgets"""
        
        # Lambda metrics widget
        lambda_metrics_widget = cloudwatch.GraphWidget(
            title="Lambda Function Metrics",
            left=[
                self.data_collector_function.metric_invocations(),
                self.prediction_processor_function.metric_invocations(),
                self.model_monitor_function.metric_invocations()
            ],
            right=[
                self.data_collector_function.metric_errors(),
                self.prediction_processor_function.metric_errors(),
                self.model_monitor_function.metric_errors()
            ]
        )
        
        # Kinesis metrics widget
        kinesis_metrics_widget = cloudwatch.GraphWidget(
            title="Kinesis Stream Metrics",
            left=[
                self.metrics_stream.metric_incoming_records(),
                self.predictions_stream.metric_incoming_records()
            ],
            right=[
                self.metrics_stream.metric_incoming_bytes(),
                self.predictions_stream.metric_incoming_bytes()
            ]
        )
        
        # Custom metrics widget
        custom_metrics_widget = cloudwatch.GraphWidget(
            title="ML Prediction Metrics",
            left=[
                cloudwatch.Metric(
                    namespace="MLOps/FailurePrediction",
                    metric_name="HighRiskPredictions",
                    statistic="Sum"
                ),
                cloudwatch.Metric(
                    namespace="MLOps/FailurePrediction",
                    metric_name="CriticalAlerts",
                    statistic="Sum"
                )
            ]
        )
        
        # Add widgets to dashboard
        self.dashboard.add_widgets(
            lambda_metrics_widget,
            kinesis_metrics_widget,
            custom_metrics_widget
        )
    
    def create_cloudwatch_alarms(self):
        """Create CloudWatch alarms"""
        
        # High error rate alarm for prediction processor
        prediction_error_alarm = cloudwatch.Alarm(
            self, f"PredictionErrorAlarm{self.environment.title()}",
            alarm_name=f"MLOps-PredictionProcessor-HighErrors-{self.environment}",
            metric=self.prediction_processor_function.metric_errors(),
            threshold=5,
            evaluation_periods=2,
            datapoints_to_alarm=2
        )
        
        prediction_error_alarm.add_alarm_action(
            cloudwatch.SnsAction(self.critical_alerts_topic)
        )
        
        # High latency alarm for SageMaker endpoint
        endpoint_latency_alarm = cloudwatch.Alarm(
            self, f"EndpointLatencyAlarm{self.environment.title()}",
            alarm_name=f"MLOps-Endpoint-HighLatency-{self.environment}",
            metric=cloudwatch.Metric(
                namespace="AWS/SageMaker/Endpoints",
                metric_name="ModelLatency",
                dimensions={
                    "EndpointName": f"mlops-failure-prediction-{self.environment}"
                }
            ),
            threshold=5000,  # 5 seconds
            evaluation_periods=3
        )
        
        endpoint_latency_alarm.add_alarm_action(
            cloudwatch.SnsAction(self.warning_alerts_topic)
        )
    
    def create_api_resources(self):
        """Create API Gateway and related resources"""
        
        # API Gateway REST API
        self.api = apigateway.RestApi(
            self, f"MLOpsApi{self.environment.title()}",
            rest_api_name=f"mlops-failure-prediction-api-{self.environment}",
            description="MLOps Failure Prediction API",
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS,
                allow_headers=["Content-Type", "Authorization"]
            )
        )
        
        # API Lambda function
        self.api_function = _lambda.Function(
            self, f"ApiFunction{self.environment.title()}",
            function_name=f"mlops-api-{self.environment}",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="main.handler",
            code=_lambda.Code.from_asset("api/fastapi_app"),
            timeout=Duration.seconds(30),
            memory_size=512,
            role=self.lambda_execution_role,
            environment={
                "ENVIRONMENT": self.environment,
                "SAGEMAKER_ENDPOINT_NAME": f"mlops-failure-prediction-{self.environment}",
                "PREDICTIONS_TABLE": self.predictions_table.table_name
            }
        )
        
        # API Gateway integration
        api_integration = apigateway.LambdaIntegration(
            self.api_function,
            proxy=True
        )
        
        # API routes
        self.api.root.add_method("ANY", api_integration)
        proxy_resource = self.api.root.add_resource("{proxy+}")
        proxy_resource.add_method("ANY", api_integration)
        
        # EventBridge rules for scheduled tasks
        self.create_scheduled_tasks()
    
    def create_scheduled_tasks(self):
        """Create EventBridge rules for scheduled tasks"""
        
        # Data collection rule (every 5 minutes)
        data_collection_rule = events.Rule(
            self, f"DataCollectionRule{self.environment.title()}",
            rule_name=f"mlops-data-collection-{self.environment}",
            schedule=events.Schedule.rate(Duration.minutes(5))
        )
        data_collection_rule.add_target(
            targets.LambdaFunction(self.data_collector_function)
        )
        
        # Model monitoring rule (daily)
        model_monitoring_rule = events.Rule(
            self, f"ModelMonitoringRule{self.environment.title()}",
            rule_name=f"mlops-model-monitoring-{self.environment}",
            schedule=events.Schedule.rate(Duration.days(1))
        )
        model_monitoring_rule.add_target(
            targets.LambdaFunction(self.model_monitor_function)
        )