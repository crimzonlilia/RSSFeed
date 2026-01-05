# src/ingestion/pipeline.py
import logging
from rss_crawler import RSSCrawler
from kafka_producer import ArticleProducer
from config.rss_sources import ALL_FEEDS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_pipeline(interval: int = 300):
    """Run RSS crawling + Kafka streaming pipeline"""
    
    crawler = RSSCrawler(ALL_FEEDS)
    producer = ArticleProducer()
    
    logger.info("🚀 Starting RSS → Kafka pipeline...")
    
    try:
        for articles in crawler.crawl_continuously(interval):
            if articles:
                producer.send_batch(articles)
            else:
                logger.warning("⚠️ No new articles found")
    
    except KeyboardInterrupt:
        logger.info("⛔ Pipeline stopped by user")
    
    finally:
        producer.close()
        logger.info("✅ Pipeline shutdown complete")


if __name__ == "__main__":
    # Run pipeline - crawl every 5 minutes (300 seconds)
    run_pipeline(interval=300)