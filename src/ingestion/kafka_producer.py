import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from typing import List, Dict
from config.kafka_config import KAFKA_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ArticleProducer:
    """Kafka Producer for RSS articles"""
    
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=KAFKA_CONFIG['api_version'],
            acks='all',  # Wait for all replicas
            retries=3
        )
        self.topic = KAFKA_CONFIG['topic']
        logger.info(f"Kafka Producer connected to {KAFKA_CONFIG['bootstrap_servers']}")
    
    def send_article(self, article: Dict):
        """Send single article to Kafka"""
        try:
            future = self.producer.send(self.topic, article)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Sent to Kafka - Topic: {record_metadata.topic}, "
                       f"Partition: {record_metadata.partition}, "
                       f"Offset: {record_metadata.offset}")
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send article: {e}")
            return False
    
    def send_batch(self, articles: List[Dict]):
        """Send batch of articles to Kafka"""
        success_count = 0
        
        for article in articles:
            if self.send_article(article):
                success_count += 1
        
        self.producer.flush()  # Ensure all messages are sent
        logger.info(f"Batch sent: {success_count}/{len(articles)} articles")
        
        return success_count
    
    def close(self):
        """Close Kafka producer"""
        self.producer.close()
        logger.info("Kafka Producer closed")


# Test producer
if __name__ == "__main__":
    producer = ArticleProducer()
    
    # Test message
    test_article = {
        'id': 'test123',
        'title': 'Test Article',
        'summary': 'This is a test',
        'link': 'http://example.com',
        'source': 'Test Source',
        'category': 'technology'
    }
    
    producer.send_article(test_article)
    producer.close()