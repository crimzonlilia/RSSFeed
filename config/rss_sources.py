import os
from dotenv import load_dotenv

load_dotenv()

# Parse RSS feeds from environment variables
RSS_FEEDS = {}
categories = ['technology', 'business', 'science', 'world']

for category in categories:
    env_key = f'RSS_FEEDS_{category.upper()}'
    feeds_str = os.getenv(env_key, '')
    if feeds_str:
        RSS_FEEDS[category] = [feed.strip() for feed in feeds_str.split(',')]

# Flatten all feeds into single list
ALL_FEEDS = []
for category, feeds in RSS_FEEDS.items():
    for feed in feeds:
        ALL_FEEDS.append({'url': feed, 'category': category})