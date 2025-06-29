import os

project_structure = {
    "aws-mlops-failure-prediction": {
        "infrastructure": {
            "cdk": ["app.py", "mlops_stack.py", "requirements.txt", "cdk.json"],
            "cloudformation": ["mlops-template.yaml"],
            "terraform": ["main.tf", "variables.tf", "outputs.tf"]
        },
        "lambda_functions": {
            "data_collector": ["lambda_function.py", "requirements.txt", "utils.py"],
            "prediction_processor": ["lambda_function.py", "requirements.txt", "alert_manager.py"],
            "model_monitor": ["lambda_function.py", "drift_detector.py"],
            "shared": ["aws_utils.py", "common.py"]
        },
        "sagemaker": {
            "training": {
                "source": ["train.py", "evaluate.py", "requirements.txt"],
                "processing": ["feature_engineering.py", "data_validation.py", "preprocessing.py"],
                "": ["pipeline_definition.py"]
            },
            "inference": ["inference.py", "requirements.txt", "model_handler.py"],
            "monitoring": ["model_quality_monitor.py", "data_quality_monitor.py", "bias_monitor.py"],
            "pipelines": ["training_pipeline.py", "inference_pipeline.py", "monitoring_pipeline.py"]
        },
        "kinesis": {
            "analytics": ["feature_engineering.sql", "anomaly_detection.sql", "aggregations.sql"],
            "firehose": ["data_transformation.py"]
        },
        "api": {
            "fastapi_app": ["main.py", "models.py", "routes.py", "requirements.txt"],
            "api_gateway": ["swagger.yaml", "lambda_authorizer.py"]
        },
        "monitoring": {
            "cloudwatch": ["dashboard_config.json", "custom_metrics.py", "alarm_definitions.py"],
            "grafana": ["dashboard.json", "datasource_config.yaml"],
            "quicksight": ["dashboard_template.json"]
        },
        "data": {
            "schemas": ["input_schema.json", "feature_schema.json", "output_schema.json"],
            "sample": ["sample_metrics.json", "test_data.csv"]
        },
        "config": ["development.yaml", "production.yaml", "alert_rules.yaml"],
        "scripts": ["deploy.sh", "setup_environment.py", "data_migration.py", "model_deployment.py"],
        "tests": {
            "unit": ["test_lambda_functions.py", "test_sagemaker_inference.py", "test_data_validation.py"],
            "integration": ["test_end_to_end_pipeline.py", "test_api_endpoints.py"],
            "load": ["test_performance.py"]
        },
        "docs": ["architecture.md", "deployment_guide.md", "api_documentation.md"],
        "": ["requirements.txt", "README.md", "docker-compose.yml", ".env.example"]
    }
}

def create_structure(base_path, structure):
    for name, contents in structure.items():
        path = os.path.join(base_path, name)
        if isinstance(contents, dict):
            os.makedirs(path, exist_ok=True)
            create_structure(path, contents)
        elif isinstance(contents, list):
            os.makedirs(path, exist_ok=True)
            for file in contents:
                open(os.path.join(path, file), 'w').close()

if __name__ == "__main__":
    create_structure(".", project_structure)
    print("✅ aws-mlops-failure-prediction folder structure created successfully.")
