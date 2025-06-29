# dashboard/streamlit_app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import redis
import json
from datetime import datetime, timedelta
import asyncio
import time

st.set_page_config(
    page_title="MLOps System Monitor",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded"
)

class SystemDashboard:
    def __init__(self):
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        
    def load_recent_predictions(self, hours=24):
        """Load recent predictions from Redis"""
        keys = self.redis_client.keys(f"prediction:*")
        predictions = []
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        for key in keys:
            data = json.loads(self.redis_client.get(key))
            pred_time = datetime.fromisoformat(data['timestamp'])
            
            if pred_time > cutoff_time:
                predictions.append(data)
        
        return pd.DataFrame(predictions)
    
    def create_risk_gauge(self, current_risk):
        """Create risk level gauge"""
        fig = go.Figure(go.Indicator(
            mode = "gauge+number+delta",
            value = current_risk,
            domain = {'x': [0, 1], 'y': [0, 1]},
            title = {'text': "Current Risk Level"},
            delta = {'reference': 50},
            gauge = {
                'axis': {'range': [None, 100]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, 25], 'color': "lightgreen"},
                    {'range': [25, 50], 'color': "yellow"},
                    {'range': [50, 75], 'color': "orange"},
                    {'range': [75, 100], 'color': "red"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 90
                }
            }
        ))
        fig.update_layout(height=300)
        return fig
    
    def create_time_series_plot(self, df):
        """Create time series plot of predictions"""
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('Failure Probability Over Time', 'System Metrics'),
            vertical_spacing=0.1
        )
        
        # Failure probability
        fig.add_trace(
            go.Scatter(
                x=pd.to_datetime(df['timestamp']),
                y=df['failure_probability'],
                mode='lines+markers',
                name='Failure Probability',
                line=dict(color='red', width=2)
            ),
            row=1, col=1
        )
        
        # Add threshold line
        fig.add_hline(y=0.8, line_dash="dash", line_color="orange", 
                     annotation_text="Critical Threshold", row=1, col=1)
        
        # System metrics
        if 'input_data' in df.columns:
            cpu_data = [json.loads(data).get('cpu_usage', 0) for data in df['input_data']]
            memory_data = [json.loads(data).get('memory_usage', 0) for data in df['input_data']]
            
            fig.add_trace(
                go.Scatter(
                    x=pd.to_datetime(df['timestamp']),
                    y=cpu_data,
                    mode='lines',
                    name='CPU Usage (%)',
                    line=dict(color='blue')
                ),
                row=2, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=pd.to_datetime(df['timestamp']),
                    y=memory_data,
                    mode='lines',
                    name='Memory Usage (%)',
                    line=dict(color='green')
                ),
                row=2, col=1
            )
        
        fig.update_layout(height=600, showlegend=True)
        return fig
    
    def create_server_status_table(self, df):
        """Create server status table"""
        if df.empty:
            return pd.DataFrame()
        
        # Get latest status for each server
        latest_predictions = df.sort_values('timestamp').groupby('server_id').tail(1)
        
        status_data = []
        for _, row in latest_predictions.iterrows():
            input_data = json.loads(row['input_data']) if isinstance(row['input_data'], str) else row['input_data']
            
            status_data.append({
                'Server ID': row['server_id'],
                'Risk Level': row['risk_level'],
                'Failure Probability': f"{row['failure_probability']:.3f}",
                'Impact Score': row['impact_score'],
                'CPU Usage': f"{input_data.get('cpu_usage', 0):.1f}%",
                'Memory Usage': f"{input_data.get('memory_usage', 0):.1f}%",
                'Response Time': f"{input_data.get('response_time_ms', 0):.0f}ms",
                'Last Updated': pd.to_datetime(row['timestamp']).strftime('%H:%M:%S')
            })
        
        return pd.DataFrame(status_data)

# Streamlit Dashboard Layout
def main():
    dashboard = SystemDashboard()
    
    st.title("🚨 MLOps System Failure Prediction Dashboard")
    st.markdown("---")
    
    # Sidebar controls
    st.sidebar.header("⚙️ Controls")
    auto_refresh = st.sidebar.checkbox("Auto Refresh", value=True)
    refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 10)
    hours_to_show = st.sidebar.slider("Hours to Display", 1, 24, 6)
    
    # Create placeholder for dynamic content
    main_container = st.container()
    
    # Auto-refresh logic
    if auto_refresh:
        placeholder = st.empty()
        
        while True:
            with placeholder.container():
                render_dashboard(dashboard, hours_to_show)
            time.sleep(refresh_interval)
    else:
        render_dashboard(dashboard, hours_to_show)

def render_dashboard(dashboard, hours_to_show):
    """Render the main dashboard content"""
    # Load data
    df = dashboard.load_recent_predictions(hours=hours_to_show)
    
    if df.empty:
        st.warning("No prediction data available. Make sure the prediction service is running.")
        return
    
    # Top metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_servers = df['server_id'].nunique()
        st.metric("Active Servers", total_servers)
    
    with col2:
        critical_servers = (df['risk_level'] == 'CRITICAL').sum()
        st.metric("Critical Alerts", critical_servers, delta=f"+{critical_servers}" if critical_servers > 0 else None)
    
    with col3:
        avg_risk = df['failure_probability'].mean()
        st.metric("Average Risk", f"{avg_risk:.3f}", delta=f"{avg_risk-0.5:.3f}")
    
    with col4:
        high_impact = (df['impact_score'] > 75).sum()
        st.metric("High Impact Events", high_impact)
    
    # Risk gauge and recent alerts
    col1, col2 = st.columns([1, 2])
    
    with col1:
        current_max_risk = df['failure_probability'].max() * 100
        risk_gauge = dashboard.create_risk_gauge(current_max_risk)
        st.plotly_chart(risk_gauge, use_container_width=True)
    
    with col2:
        st.subheader("🚨 Recent Critical Alerts")
        critical_alerts = df[df['risk_level'].isin(['CRITICAL', 'HIGH'])].sort_values('timestamp', ascending=False).head(5)
        
        for _, alert in critical_alerts.iterrows():
            risk_color = "🔴" if alert['risk_level'] == 'CRITICAL' else "🟠"
            st.warning(f"{risk_color} **{alert['server_id']}** - {alert['risk_level']} "
                      f"(Risk: {alert['failure_probability']:.3f}, Impact: {alert['impact_score']:.0f})")
    
    # Time series visualization
    st.subheader("📈 System Health Trends")
    time_series_plot = dashboard.create_time_series_plot(df)
    st.plotly_chart(time_series_plot, use_container_width=True)
    
    # Server status table
    st.subheader("🖥️ Server Status Overview")
    status_table = dashboard.create_server_status_table(df)
    if not status_table.empty:
        # Color code the risk levels
        def color_risk_level(val):
            if val == 'CRITICAL':
                return 'background-color: #ffcccc'
            elif val == 'HIGH':
                return 'background-color: #ffe6cc'
            elif val == 'MEDIUM':
                return 'background-color: #fff2cc'
            else:
                return 'background-color: #e6ffe6'
        
        styled_table = status_table.style.applymap(color_risk_level, subset=['Risk Level'])
        st.dataframe(styled_table, use_container_width=True)
    
    # Recommendations section
    st.subheader("💡 Recommendations")
    latest_recommendations = df.sort_values('timestamp').tail(1)['recommendations'].iloc[0]
    
    if isinstance(latest_recommendations, str):
        recommendations = json.loads(latest_recommendations)
    else:
        recommendations = latest_recommendations
    
    for i, rec in enumerate(recommendations, 1):
        st.info(f"{i}. {rec}")

if __name__ == "__main__":
    main()