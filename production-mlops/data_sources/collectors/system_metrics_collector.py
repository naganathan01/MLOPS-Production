# data_sources/collectors/system_metrics_collector.py
import psutil
import docker
import kubernetes
import pandas as pd
from datetime import datetime
import asyncio
import aiohttp

class SystemMetricsCollector:
    def __init__(self, config):
        self.config = config
        self.docker_client = docker.from_env()
        
    async def collect_server_metrics(self, server_list):
        """Collect metrics from multiple servers"""
        tasks = []
        for server in server_list:
            tasks.append(self.collect_single_server(server))
        
        results = await asyncio.gather(*tasks)
        return pd.concat(results, ignore_index=True)
    
    async def collect_single_server(self, server):
        """Collect metrics from a single server"""
        async with aiohttp.ClientSession() as session:
            # Collect system metrics
            cpu_usage = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            network = psutil.net_io_counters()
            
            # Collect application metrics via API
            app_metrics = await self.collect_app_metrics(session, server)
            
            return pd.DataFrame([{
                'timestamp': datetime.now(),
                'server_id': server['id'],
                'cpu_usage': cpu_usage,
                'memory_usage': memory.percent,
                'disk_usage': (disk.used / disk.total) * 100,
                'network_latency_ms': await self.ping_server(session, server['host']),
                'error_count': app_metrics.get('error_count', 0),
                'response_time_ms': app_metrics.get('response_time', 0),
                'active_connections': app_metrics.get('connections', 0)
            }])
    
    async def collect_app_metrics(self, session, server):
        """Collect application-specific metrics"""
        try:
            async with session.get(f"http://{server['host']}/metrics") as response:
                if response.status == 200:
                    data = await response.json()
                    return data
        except:
            return {}
        
    async def ping_server(self, session, host):
        """Measure network latency"""
        start_time = datetime.now()
        try:
            async with session.get(f"http://{host}/health", timeout=5) as response:
                end_time = datetime.now()
                return (end_time - start_time).total_seconds() * 1000
        except:
            return 9999  # Timeout indicator

# Docker container metrics
class DockerMetricsCollector:
    def collect_container_metrics(self):
        """Collect Docker container metrics"""
        client = docker.from_env()
        containers_data = []
        
        for container in client.containers.list():
            stats = container.stats(stream=False)
            
            # Calculate CPU percentage
            cpu_delta = stats['cpu_stats']['cpu_usage']['total_usage'] - \
                       stats['precpu_stats']['cpu_usage']['total_usage']
            system_delta = stats['cpu_stats']['system_cpu_usage'] - \
                          stats['precpu_stats']['system_cpu_usage']
            cpu_percent = (cpu_delta / system_delta) * 100.0
            
            # Calculate memory usage
            memory_usage = stats['memory_stats']['usage']
            memory_limit = stats['memory_stats']['limit']
            memory_percent = (memory_usage / memory_limit) * 100
            
            containers_data.append({
                'timestamp': datetime.now(),
                'container_id': container.id[:12],
                'container_name': container.name,
                'cpu_usage': cpu_percent,
                'memory_usage': memory_percent,
                'network_rx': stats['networks']['eth0']['rx_bytes'],
                'network_tx': stats['networks']['eth0']['tx_bytes']
            })
            
        return pd.DataFrame(containers_data)

# Kubernetes metrics collector
class K8sMetricsCollector:
    def __init__(self):
        kubernetes.config.load_incluster_config()  # For in-cluster
        # kubernetes.config.load_kube_config()  # For local development
        self.v1 = kubernetes.client.CoreV1Api()
        self.metrics_v1beta1 = kubernetes.client.CustomObjectsApi()
    
    def collect_pod_metrics(self):
        """Collect Kubernetes pod metrics"""
        # Get metrics from metrics-server
        pod_metrics = self.metrics_v1beta1.list_cluster_custom_object(
            group="metrics.k8s.io",
            version="v1beta1",
            plural="pods"
        )
        
        pods_data = []
        for pod_metric in pod_metrics['items']:
            for container in pod_metric['containers']:
                pods_data.append({
                    'timestamp': datetime.now(),
                    'namespace': pod_metric['metadata']['namespace'],
                    'pod_name': pod_metric['metadata']['name'],
                    'container_name': container['name'],
                    'cpu_usage': self.parse_cpu(container['usage']['cpu']),
                    'memory_usage': self.parse_memory(container['usage']['memory'])
                })
        
        return pd.DataFrame(pods_data)
    
    def parse_cpu(self, cpu_str):
        """Parse CPU string to numeric value"""
        if cpu_str.endswith('n'):
            return float(cpu_str[:-1]) / 1000000000  # nanocores to cores
        elif cpu_str.endswith('m'):
            return float(cpu_str[:-1]) / 1000  # millicores to cores
        return float(cpu_str)
    
    def parse_memory(self, memory_str):
        """Parse memory string to MB"""
        if memory_str.endswith('Ki'):
            return float(memory_str[:-2]) / 1024  # KiB to MB
        elif memory_str.endswith('Mi'):
            return float(memory_str[:-2])  # MiB
        elif memory_str.endswith('Gi'):
            return float(memory_str[:-2]) * 1024  # GiB to MB
        return float(memory_str) / (1024 * 1024)  # bytes to MB