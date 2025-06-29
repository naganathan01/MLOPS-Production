import os

structure = {
    "production-mlops": {
        "data_sources": {
            "collectors": [
                "system_metrics_collector.py",
                "log_collector.py",
                "api_collector.py"
            ],
            "connectors": [
                "postgres_connector.py",
                "redis_connector.py"
            ]
        },
        "processing": [
            "data_validator.py",
            "feature_processor.py",
            "quality_checker.py"
        ],
        "models": [
            "prediction_service.py",
            "model_manager.py"
        ],
        "dashboard": {
            "": ["streamlit_app.py"],
            "grafana_config": []
        },
        "alerts": [
            "alert_manager.py",
            "notification_handlers.py"
        ],
        "infrastructure": {
            "": ["docker-compose.yml"],
            "airflow_dags": [],
            "prometheus_config": []
        },
        "config": [
            "settings.py",
            "alert_rules.yaml"
        ]
    }
}

def create_structure(base_path, tree):
    for key, value in tree.items():
        dir_path = os.path.join(base_path, key)
        os.makedirs(dir_path, exist_ok=True)

        if isinstance(value, dict):
            create_structure(dir_path, value)
        elif isinstance(value, list):
            for item in value:
                if item.endswith(".py") or item.endswith(".yml") or item.endswith(".yaml"):
                    open(os.path.join(dir_path, item), 'w').close()

if __name__ == "__main__":
    create_structure(".", structure)
    print("✅ Folder and file structure created successfully.")
