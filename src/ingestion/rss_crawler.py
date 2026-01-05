# src/ingestion/rss_crawler.py
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import feedparser
import time
import hashlib
from datetime import datetime
from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RSSCrawler:
    """Crawler for RSS feeds"""
    
    def __init__(self, feeds: List[Dict]):
        self.feeds = feeds
        self.seen_articles = set()  # Track seen articles by hash
    
    def _generate_article_hash(self, url: str) -> str:
        """Generate unique hash for article"""
        return hashlib.md5(url.encode()).hexdigest()
    
    def _parse_feed(self, feed_url: str, category: str) -> List[Dict]:
        """Parse single RSS feed"""
        articles = []
        
        try:
            feed = feedparser.parse(feed_url)
            
            if feed.bozo:  # Feed parsing error
                logger.warning(f"Error parsing feed: {feed_url}")
                return articles
            
            for entry in feed.entries:
                # Generate unique ID
                article_url = entry.get('link', '')
                article_hash = self._generate_article_hash(article_url)
                
                # Skip if already seen
                if article_hash in self.seen_articles:
                    continue
                
                self.seen_articles.add(article_hash)
                
                # Extract article data
                article = {
                    'id': article_hash,
                    'title': entry.get('title', ''),
                    'summary': entry.get('summary', ''),
                    'content': entry.get('content', [{}])[0].get('value', '') if 'content' in entry else '',
                    'link': article_url,
                    'author': entry.get('author', 'Unknown'),
                    'published': entry.get('published', datetime.now().isoformat()),
                    'source': feed.feed.get('title', 'Unknown'),
                    'category': category,
                    'crawled_at': datetime.now().isoformat(),
                }
                
                articles.append(article)
                logger.info(f"Crawled: {article['title'][:50]}...")
            
        except Exception as e:
            logger.error(f"Error crawling {feed_url}: {e}")
        
        return articles
    
    def crawl_all_feeds(self) -> List[Dict]:
        """Crawl all configured feeds"""
        all_articles = []
        
        for feed_info in self.feeds:
            feed_url = feed_info['url']
            category = feed_info['category']
            
            logger.info(f"Crawling: {feed_url}")
            articles = self._parse_feed(feed_url, category)
            all_articles.extend(articles)
            
            time.sleep(1)  # Be nice to servers
        
        logger.info(f"Total articles crawled: {len(all_articles)}")
        return all_articles
    
    def crawl_continuously(self, interval: int = 300):
        """Crawl feeds continuously every {interval} seconds"""
        logger.info(f"Starting continuous crawling (interval: {interval}s)")
        
        while True:
            try:
                articles = self.crawl_all_feeds()
                yield articles
                
                logger.info(f"Sleeping for {interval} seconds...")
                time.sleep(interval)
                
            except KeyboardInterrupt:
                logger.info("Stopping crawler...")
                break
            except Exception as e:
                logger.error(f"Error in continuous crawl: {e}")
                time.sleep(60)  # Wait 1 min on error


# Test crawler
if __name__ == "__main__":
    from config.rss_sources import ALL_FEEDS
    
    crawler = RSSCrawler(ALL_FEEDS)
    articles = crawler.crawl_all_feeds()
    
    print(f"\nSample article:")
    if articles:
        print(articles[0])