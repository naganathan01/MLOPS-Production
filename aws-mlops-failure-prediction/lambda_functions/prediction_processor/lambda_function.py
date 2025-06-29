# lambda_functions/data_collector/lambda_function.py
import json
import boto3
import pandas as pd
from datetime import datetime, timedelta
import os
import asyncio
import aiohttp
from utils import CloudWatchMetricsCollector, RDSMetricsCollector, ALBMetricsCollector

# Initialize AWS clients
kinesis = boto3.client('kinesis')
firehose = boto3.client('firehose')
dynamodb = boto3.resource('dynamodb')

# Environment variables
ENVIRONMENT = os.environ['ENVIRONMENT']
KINESIS_STREAM_NAME = os.environ['KINESIS_STREAM_NAME']
FIREHOSE_STREAM_NAME = os.environ['FIREHOSE_STREAM_NAME']
METADATA_TABLE_NAME = os.environ['METADATA_TABLE']

# Initialize DynamoDB table
metadata_table = dynamodb.Table(METADATA_TABLE_NAME)

def lambda_handler(event, context):
    """
    Main Lambda handler for collecting system metrics
    """
    
    try:
        print(f"Starting data collection for environment: {ENVIRONMENT}")
        
        # Initialize collectors
        cloudwatch_collector = CloudWatchMetricsCollector()
        rds_collector = RDSMetricsCollector()
        alb_collector = ALBMetricsCollector()
        
        # Collect metrics from different sources
        metrics = []
        
        # Collect EC2 metrics
        ec2_metrics = cloudwatch_collector.collect_ec2_metrics()
        metrics.extend(ec2_metrics)
        
        # Collect RDS metrics
        rds_metrics = rds_collector.collect_rds_metrics()
        metrics.extend(rds_metrics)
        
        # Collect ALB metrics
        alb_metrics = alb_collector.collect_alb_metrics()
        metrics.extend(alb_metrics)
        
        # Collect custom application metrics
        app_metrics = collect_custom_application_metrics()
        metrics.extend(app_metrics)
        
        print(f"Collected {len(metrics)} total metrics")
        
        # Send metrics to Kinesis Data Stream
        kinesis_records = []
        firehose_records = []
        
        for metric in metrics:
            # Add metadata
            metric['environment'] = ENVIRONMENT
            metric['collection_timestamp'] = datetime.now().isoformat()
            
            # Prepare for Kinesis
            kinesis_record = {
                'Data': json.dumps(metric),
                'PartitionKey': metric.get('server_id', 'unknown')
            }
            kinesis_records.append(kinesis_record)
            
            # Prepare for Firehose (archival)
            firehose_record = {
                'Data': json.dumps(metric) + '\n'
            }
            firehose_records.append(firehose_record)
        
        # Send to Kinesis Data Stream in batches
        batch_size = 500
        for i in range(0, len(kinesis_records), batch_size):
            batch = kinesis_records[i:i + batch_size]
            
            response = kinesis.put_records(
                Records=batch,
                StreamName=KINESIS_STREAM_NAME
            )
            
            print(f"Sent batch {i//batch_size + 1} to Kinesis: {response['FailedRecordCount']} failed")
        
        # Send to Firehose for archival
        if firehose_records:
            firehose.put_record_batch(
                DeliveryStreamName=FIREHOSE_STREAM_NAME,
                Records=firehose_records[:500]  # Firehose limit
            )
        
        # Update metadata table
        metadata_table.put_item(
            Item={
                'id': f"data_collection_{ENVIRONMENT}",
                'timestamp': datetime.now().isoformat(),
                'metrics_collected': len(metrics),
                'sources': ['ec2', 'rds', 'alb', 'application'],
                'status': 'success'
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully collected and sent {len(metrics)} metrics',
                'environment': ENVIRONMENT,
                'timestamp': datetime.now().isoformat()
            })
        }
        
    except Exception as e:
        print(f"Error in data collection: {str(e)}")
        
        # Log error to metadata table
        metadata_table.put_item(
            Item={
                'id': f"data_collection_{ENVIRONMENT}",
                'timestamp': datetime.now().isoformat(),
                'status': 'error',
                'error_message': str(e)
            }
        )
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def collect_custom_application_metrics():
    """Collect custom application metrics"""
    
    try:
        # Define application endpoints based on environment
        if ENVIRONMENT == 'prod':
            endpoints = [
                'http://web-app-1.internal:8080/metrics',
                'http://web-app-2.internal:8080/metrics',
                'http://api-service.internal:8000/metrics'
            ]
        else:
            endpoints = [
                'http://dev-web-app.internal:8080/metrics',
                'http://dev-api-service.internal:8000/metrics'
            ]
        
        metrics = []
        
        for endpoint in endpoints:
            try:
                import requests
                response = requests.get(endpoint, timeout=5)
                
                if response.status_code == 200:
                    app_metrics = response.json()
                    
                    metric_data = {
                        'timestamp': datetime.now().isoformat(),
                        'source': 'application',
                        'server_id': endpoint.split('//')[1].split('.')[0],
                        'endpoint': endpoint,
                        'cpu_usage': app_metrics.get('system_cpu_percent', 0),
                        'memory_usage': app_metrics.get('system_memory_percent', 0),
                        'active_connections': app_metrics.get('active_connections', 0),
                        'response_time_ms': app_metrics.get('avg_response_time_ms', 0),
                        'error_count': app_metrics.get('error_count_5min', 0),
                        'request_count': app_metrics.get('request_count_5min', 0),
                        'disk_usage': app_metrics.get('disk_usage_percent', 0),
                        'network_latency_ms': app_metrics.get('network_latency_ms', 0)
                    }
                    
                    metrics.append(metric_data)
                    
            except Exception as e:
                print(f"Error collecting from {endpoint}: {str(e)}")
        
        return metrics
        
    except Exception as e:
        print(f"Error in custom metrics collection: {str(e)}")
        return []