# lambda_functions/data_collector/utils.py
import boto3
import pandas as pd
from datetime import datetime, timedelta
import json

class CloudWatchMetricsCollector:
    def __init__(self):
        self.cloudwatch = boto3.client('cloudwatch')
        self.ec2 = boto3.client('ec2')
    
    def collect_ec2_metrics(self):
        """Collect EC2 instance metrics from CloudWatch"""
        metrics = []
        
        try:
            # Get all running EC2 instances
            instances = self.ec2.describe_instances(
                Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]
            )
            
            for reservation in instances['Reservations']:
                for instance in reservation['Instances']:
                    instance_id = instance['InstanceId']
                    
                    # Collect multiple metrics for each instance
                    instance_metrics = self._collect_instance_metrics(instance_id, instance)
                    if instance_metrics:
                        metrics.append(instance_metrics)
            
            return metrics
            
        except Exception as e:
            print(f"Error collecting EC2 metrics: {str(e)}")
            return []
    
    def _collect_instance_metrics(self, instance_id, instance_info):
        """Collect metrics for a single EC2 instance"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=10)
            
            # CPU Utilization
            cpu_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName='CPUUtilization',
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Average']
            )
            
            # Memory utilization (requires CloudWatch agent)
            memory_response = self.cloudwatch.get_metric_statistics(
                Namespace='CWAgent',
                MetricName='mem_used_percent',
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Average']
            )
            
            # Disk utilization
            disk_response = self.cloudwatch.get_metric_statistics(
                Namespace='CWAgent',
                MetricName='disk_used_percent',
                Dimensions=[
                    {'Name': 'InstanceId', 'Value': instance_id},
                    {'Name': 'device', 'Value': '/dev/xvda1'},
                    {'Name': 'fstype', 'Value': 'ext4'},
                    {'Name': 'path', 'Value': '/'}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Average']
            )
            
            # Network metrics
            network_in_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName='NetworkIn',
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Sum']
            )
            
            network_out_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName='NetworkOut',
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Sum']
            )
            
            # Extract latest values
            cpu_usage = cpu_response['Datapoints'][-1]['Average'] if cpu_response['Datapoints'] else 0
            memory_usage = memory_response['Datapoints'][-1]['Average'] if memory_response['Datapoints'] else 0
            disk_usage = disk_response['Datapoints'][-1]['Average'] if disk_response['Datapoints'] else 0
            network_in = network_in_response['Datapoints'][-1]['Sum'] if network_in_response['Datapoints'] else 0
            network_out = network_out_response['Datapoints'][-1]['Sum'] if network_out_response['Datapoints'] else 0
            
            # Get instance tags
            tags = {tag['Key']: tag['Value'] for tag in instance_info.get('Tags', [])}
            
            return {
                'timestamp': datetime.now().isoformat(),
                'source': 'ec2',
                'server_id': instance_id,
                'instance_type': instance_info.get('InstanceType', 'unknown'),
                'availability_zone': instance_info.get('Placement', {}).get('AvailabilityZone', 'unknown'),
                'cpu_usage': round(cpu_usage, 2),
                'memory_usage': round(memory_usage, 2),
                'disk_usage': round(disk_usage, 2),
                'network_in_bytes': network_in,
                'network_out_bytes': network_out,
                'network_latency_ms': self._calculate_network_latency(network_in, network_out),
                'tags': tags,
                'server_importance': self._calculate_server_importance(tags)
            }
            
        except Exception as e:
            print(f"Error collecting metrics for instance {instance_id}: {str(e)}")
            return None
    
    def _calculate_network_latency(self, network_in, network_out):
        """Estimate network latency based on traffic (simplified)"""
        # This is a simplified calculation - in production you'd use actual latency metrics
        total_traffic = network_in + network_out
        if total_traffic > 1000000000:  # 1GB
            return 150  # High traffic = higher latency
        elif total_traffic > 100000000:  # 100MB
            return 100
        else:
            return 50
    
    def _calculate_server_importance(self, tags):
        """Calculate server importance based on tags"""
        # Priority servers get higher importance
        if tags.get('Environment') == 'production':
            if tags.get('Role') in ['database', 'payment', 'auth']:
                return 2.0  # Critical servers
            elif tags.get('Role') in ['web', 'api']:
                return 1.5  # Important servers
        return 1.0  # Normal servers

class RDSMetricsCollector:
    def __init__(self):
        self.cloudwatch = boto3.client('cloudwatch')
        self.rds = boto3.client('rds')
    
    def collect_rds_metrics(self):
        """Collect RDS database metrics"""
        metrics = []
        
        try:
            # Get all RDS instances
            rds_instances = self.rds.describe_db_instances()
            
            for db_instance in rds_instances['DBInstances']:
                db_metrics = self._collect_db_instance_metrics(db_instance)
                if db_metrics:
                    metrics.append(db_metrics)
            
            return metrics
            
        except Exception as e:
            print(f"Error collecting RDS metrics: {str(e)}")
            return []
    
    def _collect_db_instance_metrics(self, db_instance):
        """Collect metrics for a single RDS instance"""
        try:
            db_instance_id = db_instance['DBInstanceIdentifier']
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=10)
            
            # CPU Utilization
            cpu_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='CPUUtilization',
                Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Average']
            )
            
            # Database connections
            connections_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='DatabaseConnections',
                Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Average']
            )
            
            # Free storage space
            free_storage_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='FreeStorageSpace',
                Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Average']
            )
            
            # Read/Write IOPS
            read_iops_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='ReadIOPS',
                Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Average']
            )
            
            write_iops_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='WriteIOPS',
                Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Average']
            )
            
            # Read/Write Latency
            read_latency_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='ReadLatency',
                Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Average']
            )
            
            # Extract values
            cpu_usage = cpu_response['Datapoints'][-1]['Average'] if cpu_response['Datapoints'] else 0
            connections = connections_response['Datapoints'][-1]['Average'] if connections_response['Datapoints'] else 0
            free_storage = free_storage_response['Datapoints'][-1]['Average'] if free_storage_response['Datapoints'] else 0
            read_iops = read_iops_response['Datapoints'][-1]['Average'] if read_iops_response['Datapoints'] else 0
            write_iops = write_iops_response['Datapoints'][-1]['Average'] if write_iops_response['Datapoints'] else 0
            read_latency = read_latency_response['Datapoints'][-1]['Average'] if read_latency_response['Datapoints'] else 0
            
            # Calculate storage usage percentage
            allocated_storage = db_instance.get('AllocatedStorage', 100) * 1024 * 1024 * 1024  # GB to bytes
            storage_used_percent = ((allocated_storage - free_storage) / allocated_storage) * 100 if allocated_storage > 0 else 0
            
            return {
                'timestamp': datetime.now().isoformat(),
                'source': 'rds',
                'server_id': db_instance_id,
                'engine': db_instance.get('Engine', 'unknown'),
                'instance_class': db_instance.get('DBInstanceClass', 'unknown'),
                'cpu_usage': round(cpu_usage, 2),
                'storage_usage': round(storage_used_percent, 2),
                'active_connections': round(connections, 0),
                'read_iops': round(read_iops, 2),
                'write_iops': round(write_iops, 2),
                'network_latency_ms': round(read_latency * 1000, 2),  # Convert to ms
                'total_iops': round(read_iops + write_iops, 2),
                'server_importance': 2.0 if 'prod' in db_instance_id.lower() else 1.0
            }
            
        except Exception as e:
            print(f"Error collecting metrics for RDS instance {db_instance_id}: {str(e)}")
            return None

class ALBMetricsCollector:
    def __init__(self):
        self.cloudwatch = boto3.client('cloudwatch')
        self.elbv2 = boto3.client('elbv2')
    
    def collect_alb_metrics(self):
        """Collect Application Load Balancer metrics"""
        metrics = []
        
        try:
            # Get all load balancers
            load_balancers = self.elbv2.describe_load_balancers()
            
            for lb in load_balancers['LoadBalancers']:
                if lb['Type'] == 'application':  # Only ALBs
                    alb_metrics = self._collect_alb_instance_metrics(lb)
                    if alb_metrics:
                        metrics.append(alb_metrics)
            
            return metrics
            
        except Exception as e:
            print(f"Error collecting ALB metrics: {str(e)}")
            return []
    
    def _collect_alb_instance_metrics(self, lb):
        """Collect metrics for a single ALB"""
        try:
            lb_name = lb['LoadBalancerName']
            lb_arn = lb['LoadBalancerArn']
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=10)
            
            # Request count
            request_count_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/ApplicationELB',
                MetricName='RequestCount',
                Dimensions=[{'Name': 'LoadBalancer', 'Value': lb_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Sum']
            )
            
            # Target response time
            response_time_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/ApplicationELB',
                MetricName='TargetResponseTime',
                Dimensions=[{'Name': 'LoadBalancer', 'Value': lb_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Average']
            )
            
            # HTTP 4XX errors
            http_4xx_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/ApplicationELB',
                MetricName='HTTPCode_Target_4XX_Count',
                Dimensions=[{'Name': 'LoadBalancer', 'Value': lb_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Sum']
            )
            
            # HTTP 5XX errors
            http_5xx_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/ApplicationELB',
                MetricName='HTTPCode_Target_5XX_Count',
                Dimensions=[{'Name': 'LoadBalancer', 'Value': lb_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Sum']
            )
            
            # Active connection count
            active_connections_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/ApplicationELB',
                MetricName='ActiveConnectionCount',
                Dimensions=[{'Name': 'LoadBalancer', 'Value': lb_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Average']
            )
            
            # Extract values
            request_count = request_count_response['Datapoints'][-1]['Sum'] if request_count_response['Datapoints'] else 0
            response_time = response_time_response['Datapoints'][-1]['Average'] if response_time_response['Datapoints'] else 0
            http_4xx = http_4xx_response['Datapoints'][-1]['Sum'] if http_4xx_response['Datapoints'] else 0
            http_5xx = http_5xx_response['Datapoints'][-1]['Sum'] if http_5xx_response['Datapoints'] else 0
            active_connections = active_connections_response['Datapoints'][-1]['Average'] if active_connections_response['Datapoints'] else 0
            
            total_errors = http_4xx + http_5xx
            error_rate = (total_errors / max(request_count, 1)) * 100
            
            return {
                'timestamp': datetime.now().isoformat(),
                'source': 'alb',
                'server_id': lb_name,
                'load_balancer_arn': lb_arn,
                'request_count': round(request_count, 0),
                'response_time_ms': round(response_time * 1000, 2),  # Convert to ms
                'error_count': round(total_errors, 0),
                'error_rate': round(error_rate, 2),
                'active_connections': round(active_connections, 0),
                'http_4xx_count': round(http_4xx, 0),
                'http_5xx_count': round(http_5xx, 0),
                'cpu_usage': min(75, (request_count / 1000) * 10),  # Estimated CPU based on load
                'memory_usage': min(80, (active_connections / 100) * 15),  # Estimated memory
                'disk_usage': 30,  # ALBs don't have disk usage, set default
                'network_latency_ms': response_time * 1000 * 0.1,  # Estimated network component
                'server_importance': 1.8  # ALBs are important for traffic routing
            }
            
        except Exception as e:
            print(f"Error collecting metrics for ALB {lb_name}: {str(e)}")
            return None