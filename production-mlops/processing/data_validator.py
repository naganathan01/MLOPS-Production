# processing/data_validator.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import great_expectations as ge

class DataValidator:
    def __init__(self, config):
        self.config = config
        self.quality_thresholds = config['data_quality']
    
    def validate_metrics_data(self, df):
        """Validate incoming metrics data"""
        validation_results = {}
        
        # Check for required columns
        required_columns = ['timestamp', 'cpu_usage', 'memory_usage', 'disk_usage']
        missing_columns = [col for col in required_columns if col not in df.columns]
        validation_results['missing_columns'] = missing_columns
        
        # Check data types
        numeric_columns = ['cpu_usage', 'memory_usage', 'disk_usage', 'response_time_ms']
        for col in numeric_columns:
            if col in df.columns:
                validation_results[f'{col}_valid_numeric'] = pd.api.types.is_numeric_dtype(df[col])
        
        # Check value ranges
        validation_results['cpu_usage_valid_range'] = ((df['cpu_usage'] >= 0) & (df['cpu_usage'] <= 100)).all()
        validation_results['memory_usage_valid_range'] = ((df['memory_usage'] >= 0) & (df['memory_usage'] <= 100)).all()
        
        # Check for null values
        validation_results['null_percentage'] = (df.isnull().sum() / len(df) * 100).to_dict()
        
        # Check data freshness
        latest_timestamp = pd.to_datetime(df['timestamp']).max()
        validation_results['data_freshness_minutes'] = (datetime.now() - latest_timestamp).total_seconds() / 60
        
        # Overall data quality score
        quality_score = self.calculate_quality_score(validation_results)
        validation_results['overall_quality_score'] = quality_score
        
        return validation_results
    
    def calculate_quality_score(self, validation_results):
        """Calculate overall data quality score"""
        score = 100
        
        # Deduct for missing columns
        if validation_results['missing_columns']:
            score -= 20
        
        # Deduct for invalid ranges
        if not validation_results.get('cpu_usage_valid_range', True):
            score -= 15
        if not validation_results.get('memory_usage_valid_range', True):
            score -= 15
        
        # Deduct for high null percentage
        avg_null_percentage = np.mean(list(validation_results['null_percentage'].values()))
        if avg_null_percentage > 10:
            score -= 20
        
        # Deduct for stale data
        if validation_results['data_freshness_minutes'] > 10:
            score -= 30
        
        return max(0, score)

# processing/feature_processor.py
class AdvancedFeatureProcessor:
    def __init__(self, config):
        self.config = config
        self.feature_history = {}
    
    def process_real_time_features(self, current_data, historical_data=None):
        """Process features in real-time"""
        df = current_data.copy()
        
        # Time-based features
        df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
        df['day_of_week'] = pd.to_datetime(df['timestamp']).dt.dayofweek
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        df['is_business_hours'] = ((df['hour'] >= 9) & (df['hour'] <= 17) & (df['day_of_week'] < 5)).astype(int)
        
        # Interaction features
        df['cpu_memory_product'] = df['cpu_usage'] * df['memory_usage']
        df['resource_pressure'] = (df['cpu_usage'] + df['memory_usage'] + df['disk_usage']) / 3
        
        if 'response_time_ms' in df.columns and 'active_connections' in df.columns:
            df['performance_ratio'] = df['response_time_ms'] / (df['active_connections'] + 1)
        
        if 'error_count' in df.columns and 'active_connections' in df.columns:
            df['error_rate'] = df['error_count'] / (df['active_connections'] + 1)
        
        # Stress indicators
        df['cpu_high'] = (df['cpu_usage'] > 80).astype(int)
        df['memory_high'] = (df['memory_usage'] > 85).astype(int)
        
        if 'response_time_ms' in df.columns:
            df['response_slow'] = (df['response_time_ms'] > 1000).astype(int)
        
        if 'error_count' in df.columns:
            df['errors_high'] = (df['error_count'] > 5).astype(int)
        
        # Calculate total stress
        stress_columns = ['cpu_high', 'memory_high']
        if 'response_slow' in df.columns:
            stress_columns.append('response_slow')
        if 'errors_high' in df.columns:
            stress_columns.append('errors_high')
        
        df['total_stress'] = df[stress_columns].sum(axis=1)
        
        # Rolling features (if historical data available)
        if historical_data is not None:
            combined_data = pd.concat([historical_data, df]).sort_values('timestamp')
            for window in [3, 5, 10]:
                for col in ['cpu_usage', 'memory_usage']:
                    if col in combined_data.columns:
                        combined_data[f'{col}_rolling_mean_{window}'] = combined_data[col].rolling(
                            window=window, min_periods=1
                        ).mean()
                        combined_data[f'{col}_rolling_std_{window}'] = combined_data[col].rolling(
                            window=window, min_periods=1
                        ).std()
            
            # Return only the new data with rolling features
            df = combined_data.tail(len(df)).copy()
        
        # Cyclical encoding for time features
        df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
        df['day_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
        df['day_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
        
        return df