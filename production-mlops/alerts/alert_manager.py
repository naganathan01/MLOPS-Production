# alerts/alert_manager.py
import asyncio
import json
import smtplib
import requests
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import yaml

class AlertManager:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.alert_rules = self.config['alert_rules']
        self.notification_channels = self.config['notification_channels']
        self.alert_history = {}
    
    async def process_prediction(self, prediction):
        """Process prediction and trigger alerts if needed"""
        alerts_triggered = []
        
        for rule_name, rule in self.alert_rules.items():
            if self.evaluate_rule(prediction, rule):
                # Check if we should suppress duplicate alerts
                if not self.should_suppress_alert(rule_name, prediction):
                    alert = await self.create_alert(rule_name, rule, prediction)
                    alerts_triggered.append(alert)
                    await self.send_notifications(alert)
                    self.record_alert(rule_name, prediction)
        
        return alerts_triggered
    
    def evaluate_rule(self, prediction, rule):
        """Evaluate if alert rule conditions are met"""
        conditions = rule['conditions']
        
        # Check failure probability threshold
        if 'failure_probability' in conditions:
            if prediction['failure_probability'] < conditions['failure_probability']['min']:
                return False
        
        # Check impact score threshold
        if 'impact_score' in conditions:
            if prediction['impact_score'] < conditions['impact_score']['min']:
                return False
        
        # Check risk level
        if 'risk_levels' in conditions:
            if prediction['risk_level'] not in conditions['risk_levels']:
                return False
        
        # Check server importance (if specified in input data)
        if 'server_importance' in conditions:
            server_importance = prediction['input_data'].get('server_importance', 1.0)
            if server_importance < conditions['server_importance']['min']:
                return False
        
        return True
    
    def should_suppress_alert(self, rule_name, prediction):
        """Check if alert should be suppressed due to recent similar alerts"""
        alert_key = f"{rule_name}:{prediction['server_id']}"
        
        if alert_key in self.alert_history:
            last_alert_time = self.alert_history[alert_key]['last_sent']
            suppression_period = self.alert_rules[rule_name].get('suppression_minutes', 15)
            
            if (datetime.now() - last_alert_time).total_seconds() < suppression_period * 60:
                return True
        
        return False
    
    async def create_alert(self, rule_name, rule, prediction):
        """Create alert object"""
        alert = {
            'id': f"alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{prediction['server_id']}",
            'rule_name': rule_name,
            'severity': rule['severity'],
            'title': rule['title'].format(**prediction),
            'description': rule['description'].format(**prediction),
            'server_id': prediction['server_id'],
            'failure_probability': prediction['failure_probability'],
            'impact_score': prediction['impact_score'],
            'risk_level': prediction['risk_level'],
            'recommendations': prediction['recommendations'],
            'timestamp': datetime.now().isoformat(),
            'prediction_data': prediction
        }
        
        return alert
    
    async def send_notifications(self, alert):
        """Send notifications through configured channels"""
        tasks = []
        
        for channel_name, channel_config in self.notification_channels.items():
            if channel_config.get('enabled', False):
                if channel_config['type'] == 'email':
                    tasks.append(self.send_email_notification(alert, channel_config))
                elif channel_config['type'] == 'slack':
                    tasks.append(self.send_slack_notification(alert, channel_config))
                elif channel_config['type'] == 'webhook':
                    tasks.append(self.send_webhook_notification(alert, channel_config))
                elif channel_config['type'] == 'pagerduty':
                    tasks.append(self.send_pagerduty_notification(alert, channel_config))
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def send_email_notification(self, alert, config):
        """Send email notification"""
        try:
            msg = MIMEMultipart()
            msg['From'] = config['from_email']
            msg['To'] = ', '.join(config['to_emails'])
            msg['Subject'] = f"[{alert['severity'].upper()}] {alert['title']}"
            
            # Create HTML email body
            html_body = f"""
            <html>
            <body>
                <h2 style="color: {'red' if alert['severity'] == 'critical' else 'orange'};">
                    {alert['title']}
                </h2>
                <p><strong>Server:</strong> {alert['server_id']}</p>
                <p><strong>Risk Level:</strong> {alert['risk_level']}</p>
                <p><strong>Failure Probability:</strong> {alert['failure_probability']:.3f}</p>
                <p><strong>Impact Score:</strong> {alert['impact_score']:.1f}</p>
                <p><strong>Time:</strong> {alert['timestamp']}</p>
                
                <h3>Description:</h3>
                <p>{alert['description']}</p>
                
                <h3>Recommendations:</h3>
                <ul>
                    {''.join([f"<li>{rec}</li>" for rec in alert['recommendations']])}
                </ul>
                
                <p><em>This alert was generated by the MLOps Failure Prediction System</em></p>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(html_body, 'html'))
            
            # Send email
            server = smtplib.SMTP(config['smtp_server'], config['smtp_port'])
            if config.get('use_tls', True):
                server.starttls()
            if config.get('username'):
                server.login(config['username'], config['password'])
            
            text = msg.as_string()
            server.sendmail(config['from_email'], config['to_emails'], text)
            server.quit()
            
        except Exception as e:
            print(f"Failed to send email notification: {str(e)}")
    
    async def send_slack_notification(self, alert, config):
        """Send Slack notification"""
        try:
            webhook_url = config['webhook_url']
            
            # Create Slack message
            color = "#ff0000" if alert['severity'] == 'critical' else "#ff9900"
            
            slack_message = {
                "attachments": [
                    {
                        "color": color,
                        "title": alert['title'],
                        "text": alert['description'],
                        "fields": [
                            {"title": "Server", "value": alert['server_id'], "short": True},
                            {"title": "Risk Level", "value": alert['risk_level'], "short": True},
                            {"title": "Failure Probability", "value": f"{alert['failure_probability']:.3f}", "short": True},
                            {"title": "Impact Score", "value": f"{alert['impact_score']:.1f}", "short": True}
                        ],
                        "footer": "MLOps Failure Prediction System",
                        "ts": int(datetime.now().timestamp())
                    }
                ]
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=slack_message) as response:
                    if response.status != 200:
                        print(f"Failed to send Slack notification: {response.status}")
                        
        except Exception as e:
            print(f"Failed to send Slack notification: {str(e)}")
    
    async def send_pagerduty_notification(self, alert, config):
        """Send PagerDuty notification"""
        try:
            pagerduty_payload = {
                "routing_key": config['integration_key'],
                "event_action": "trigger",
                "dedup_key": f"{alert['server_id']}_{alert['rule_name']}",
                "payload": {
                    "summary": alert['title'],
                    "source": alert['server_id'],
                    "severity": alert['severity'],
                    "component": "MLOps Prediction System",
                    "group": "Infrastructure",
                    "class": "System Failure Prediction",
                    "custom_details": {
                        "failure_probability": alert['failure_probability'],
                        "impact_score": alert['impact_score'],
                        "risk_level": alert['risk_level'],
                        "recommendations": alert['recommendations']
                    }
                }
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://events.pagerduty.com/v2/enqueue",
                    json=pagerduty_payload
                ) as response:
                    if response.status != 202:
                        print(f"Failed to send PagerDuty notification: {response.status}")
                        
        except Exception as e:
            print(f"Failed to send PagerDuty notification: {str(e)}")
    
    def record_alert(self, rule_name, prediction):
        """Record alert in history for suppression logic"""
        alert_key = f"{rule_name}:{prediction['server_id']}"
        self.alert_history[alert_key] = {
            'last_sent': datetime.now(),
            'count': self.alert_history.get(alert_key, {}).get('count', 0) + 1
        }

# config/alert_rules.yaml
alert_rules:
  critical_failure_risk:
    title: "Critical Failure Risk Detected on {server_id}"
    description: "Server {server_id} has a {failure_probability:.1%} probability of failure with impact score {impact_score:.1f}"
    severity: "critical"
    conditions:
      failure_probability:
        min: 0.8
      impact_score:
        min: 70
    suppression_minutes: 15
  
  high_impact_warning:
    title: "High Impact System Warning on {server_id}"
    description: "Server {server_id} showing elevated risk ({failure_probability:.1%}) with high business impact"
    severity: "warning"
    conditions:
      failure_probability:
        min: 0.6
      impact_score:
        min: 80
    suppression_minutes: 30
  
  critical_server_alert:
    title: "Critical Server at Risk: {server_id}"
    description: "Critical infrastructure server {server_id} showing failure signs"
    severity: "critical"
    conditions:
      failure_probability:
        min: 0.5
      server_importance:
        min: 2.0
    suppression_minutes: 10

notification_channels:
  email:
    type: "email"
    enabled: true
    smtp_server: "smtp.gmail.com"
    smtp_port: 587
    use_tls: true
    username: "alerts@company.com"
    password: "app_password"
    from_email: "alerts@company.com"
    to_emails:
      - "devops-team@company.com"
      - "oncall@company.com"
  
  slack:
    type: "slack"
    enabled: true
    webhook_url: "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
  
  pagerduty:
    type: "pagerduty"
    enabled: true
    integration_key: "YOUR_PAGERDUTY_INTEGRATION_KEY"