# config/spark_config.py
import os
from dotenv import load_dotenv

load_dotenv()

SPARK_CONFIG = {
    'app_name': os.getenv('SPARK_APP_NAME', 'RSSAnalytics'),
    'master': os.getenv('SPARK_MASTER', 'local[*]'),
    'kafka_bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'kafka_topic': os.getenv('KAFKA_TOPIC', 'rss-articles'),
    'checkpoint_location': os.getenv('CHECKPOINT_PATH', './data/checkpoints'),
    'output_path': os.getenv('DATA_PROCESSED_PATH', './data/processed'),
}

# Spark packages needed
SPARK_PACKAGES = [
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
]