# data_sources/collectors/log_collector.py
import pandas as pd
import re
from datetime import datetime
import asyncio
import aiofiles
from elasticsearch import AsyncElasticsearch

class LogCollector:
    def __init__(self, config):
        self.config = config
        self.es_client = AsyncElasticsearch([config['elasticsearch_url']])
    
    async def collect_application_logs(self, time_range='5m'):
        """Collect application logs from Elasticsearch"""
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"@timestamp": {"gte": f"now-{time_range}"}}},
                        {"term": {"level": "ERROR"}}
                    ]
                }
            },
            "size": 1000
        }
        
        response = await self.es_client.search(
            index="application-logs-*",
            body=query
        )
        
        logs_data = []
        for hit in response['hits']['hits']:
            log_entry = hit['_source']
            logs_data.append({
                'timestamp': log_entry['@timestamp'],
                'level': log_entry['level'],
                'message': log_entry['message'],
                'service': log_entry.get('service', 'unknown'),
                'error_type': self.extract_error_type(log_entry['message'])
            })
        
        return pd.DataFrame(logs_data)
    
    def extract_error_type(self, message):
        """Extract error type from log message"""
        error_patterns = {
            'database': r'(database|sql|connection)',
            'network': r'(timeout|connection refused|network)',
            'memory': r'(out of memory|memory)',
            'authentication': r'(auth|unauthorized|forbidden)'
        }
        
        for error_type, pattern in error_patterns.items():
            if re.search(pattern, message.lower()):
                return error_type
        return 'unknown'

# data_sources/connectors/postgres_connector.py
import asyncpg
import pandas as pd

class PostgreSQLConnector:
    def __init__(self, config):
        self.config = config
        
    async def collect_db_metrics(self):
        """Collect database performance metrics"""
        conn = await asyncpg.connect(**self.config['postgres'])
        
        # Query for database metrics
        query = """
        SELECT 
            current_timestamp as timestamp,
            datname as database_name,
            numbackends as active_connections,
            xact_commit as transactions_committed,
            xact_rollback as transactions_rolled_back,
            blks_read as blocks_read,
            blks_hit as blocks_hit,
            tup_returned as tuples_returned,
            tup_fetched as tuples_fetched,
            tup_inserted as tuples_inserted,
            tup_updated as tuples_updated,
            tup_deleted as tuples_deleted
        FROM pg_stat_database 
        WHERE datname NOT IN ('template0', 'template1', 'postgres');
        """
        
        rows = await conn.fetch(query)
        await conn.close()
        
        return pd.DataFrame([dict(row) for row in rows])