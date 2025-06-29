# models/prediction_service.py
import joblib
import pandas as pd
import numpy as np
from datetime import datetime
import asyncio
import redis
import json
from typing import Dict, List

class RealTimePredictionService:
    def __init__(self, config):
        self.config = config
        self.models = {}
        self.scaler = None
        self.feature_columns = []
        self.redis_client = redis.Redis(**config['redis'])
        self.load_models()
    
    def load_models(self):
        """Load trained models"""
        model_path = self.config['model_path']
        
        # Load models
        self.models['random_forest'] = joblib.load(f"{model_path}/random_forest_model.joblib")
        self.models['xgboost'] = joblib.load(f"{model_path}/xgboost_model.joblib")
        self.scaler = joblib.load(f"{model_path}/scaler.joblib")
        
        # Load feature columns
        with open(f"{model_path}/feature_columns.json", 'r') as f:
            self.feature_columns = json.load(f)
    
    async def predict_batch(self, processed_data: pd.DataFrame) -> List[Dict]:
        """Make predictions for a batch of data"""
        predictions = []
        
        for _, row in processed_data.iterrows():
            prediction = await self.predict_single(row.to_dict())
            predictions.append(prediction)
        
        return predictions
    
    async def predict_single(self, data: Dict) -> Dict:
        """Make prediction for single data point"""
        # Prepare feature vector
        feature_vector = self.prepare_features(data)
        
        # Get ensemble prediction
        rf_prob = self.models['random_forest'].predict_proba([feature_vector])[0][1]
        xgb_prob = self.models['xgboost'].predict_proba([feature_vector])[0][1]
        
        # Ensemble average
        ensemble_prob = (rf_prob + xgb_prob) / 2
        
        # Calculate confidence
        confidence = 1 - abs(rf_prob - xgb_prob)  # Higher confidence when models agree
        
        # Determine risk level and impact
        risk_level, impact_score = self.calculate_risk_impact(ensemble_prob, data)
        
        # Generate recommendations
        recommendations = self.generate_recommendations(ensemble_prob, data)
        
        # Store prediction in cache
        prediction_result = {
            'timestamp': datetime.now().isoformat(),
            'server_id': data.get('server_id', 'unknown'),
            'failure_probability': float(ensemble_prob),
            'confidence': float(confidence),
            'risk_level': risk_level,
            'impact_score': impact_score,
            'recommendations': recommendations,
            'model_versions': {
                'random_forest': 'v1.0',
                'xgboost': 'v1.0'
            },
            'input_data': data
        }
        
        # Cache result
        await self.cache_prediction(prediction_result)
        
        return prediction_result
    
    def prepare_features(self, data: Dict) -> List:
        """Prepare feature vector from input data"""
        # Create feature dictionary with all required features
        features = {}
        
        # Copy basic features
        basic_features = ['cpu_usage', 'memory_usage', 'disk_usage', 'network_latency_ms', 
                         'error_count', 'response_time_ms', 'active_connections']
        
        for feature in basic_features:
            features[feature] = data.get(feature, 0)
        
        # Add derived features (these should be calculated in feature processing)
        features.update({
            'cpu_memory_product': data.get('cpu_memory_product', 0),
            'resource_pressure': data.get('resource_pressure', 0),
            'performance_ratio': data.get('performance_ratio', 0),
            'error_rate': data.get('error_rate', 0),
            'total_stress': data.get('total_stress', 0),
            'hour_sin': data.get('hour_sin', 0),
            'hour_cos': data.get('hour_cos', 0),
            'day_sin': data.get('day_sin', 0),
            'day_cos': data.get('day_cos', 0)
        })
        
        # Add missing features with default values
        for col in self.feature_columns:
            if col not in features:
                features[col] = 0
        
        # Create ordered feature vector
        feature_vector = [features.get(col, 0) for col in self.feature_columns]
        
        return feature_vector
    
    def calculate_risk_impact(self, probability: float, data: Dict) -> tuple:
        """Calculate risk level and business impact score"""
        # Risk levels based on probability
        if probability >= 0.8:
            risk_level = "CRITICAL"
            base_impact = 95
        elif probability >= 0.6:
            risk_level = "HIGH"
            base_impact = 75
        elif probability >= 0.4:
            risk_level = "MEDIUM"
            base_impact = 50
        elif probability >= 0.2:
            risk_level = "LOW"
            base_impact = 25
        else:
            risk_level = "MINIMAL"
            base_impact = 5
        
        # Adjust impact based on server importance and current load
        server_importance = data.get('server_importance', 1.0)  # 1.0 = normal, 2.0 = critical
        current_load = (data.get('cpu_usage', 0) + data.get('memory_usage', 0)) / 2
        
        # Higher impact for critical servers and high current load
        impact_multiplier = server_importance * (1 + current_load / 200)
        impact_score = min(100, base_impact * impact_multiplier)
        
        return risk_level, round(impact_score, 2)
    
    def generate_recommendations(self, probability: float, data: Dict) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []
        
        if probability > 0.7:
            recommendations.append("🚨 URGENT: High failure risk detected - take immediate action")
        
        # Resource-specific recommendations
        cpu_usage = data.get('cpu_usage', 0)
        memory_usage = data.get('memory_usage', 0)
        response_time = data.get('response_time_ms', 0)
        error_count = data.get('error_count', 0)
        
        if cpu_usage > 85:
            recommendations.append("⚡ CPU Critical: Scale out instances or optimize CPU-intensive processes")
        elif cpu_usage > 75:
            recommendations.append("⚠️ CPU High: Monitor closely and prepare for scaling")
        
        if memory_usage > 90:
            recommendations.append("💾 Memory Critical: Check for memory leaks, restart services if needed")
        elif memory_usage > 80:
            recommendations.append("💾 Memory High: Investigate memory usage patterns")
        
        if response_time > 2000:
            recommendations.append("🚀 Performance Critical: Investigate slow queries and optimize bottlenecks")
        elif response_time > 1000:
            recommendations.append("🚀 Performance Degraded: Review application performance metrics")
        
        if error_count > 10:
            recommendations.append("🐛 Error Spike: Check application logs and fix critical issues")
        elif error_count > 5:
            recommendations.append("🐛 Errors Elevated: Monitor error patterns and investigate root causes")
        
        if not recommendations:
            recommendations.append("✅ System appears healthy - continue regular monitoring")
        
        return recommendations
    
    async def cache_prediction(self, prediction: Dict):
        """Cache prediction result in Redis"""
        cache_key = f"prediction:{prediction['server_id']}:{prediction['timestamp']}"
        await asyncio.get_event_loop().run_in_executor(
            None, 
            self.redis_client.setex, 
            cache_key, 
            3600,  # 1 hour TTL
            json.dumps(prediction)
        )