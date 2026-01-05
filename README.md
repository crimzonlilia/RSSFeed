# RSS Feed Real-time Analytics Pipeline

## Overview
This project implements a real-time data analytics pipeline for collecting, processing, and analyzing news articles from multiple RSS feeds. The system ingests streaming data, performs text preprocessing and sentiment analysis, and provides analytical insights through an interactive dashboard.

The pipeline is designed to demonstrate concepts in **stream processing, big data systems, and real-time analytics**.


## System Architecture
RSS Feeds → RSS Crawler → Kafka → Spark Streaming → Parquet Storage → Analytics & Dashboard

## Technologies Used
- **Language:** Python  
- **Message Queue:** Apache Kafka  
- **Stream Processing:** Apache Spark (PySpark)  
- **Sentiment Analysis:** VADER  
- **Text Processing:** NLTK, BeautifulSoup  
- **Storage:** Parquet  
- **Visualization:** Streamlit  


## How to Run (Local)

1. Start Kafka and Zookeeper.
2. Run the RSS crawler to publish articles to Kafka.
3. Start Spark Streaming job to process incoming data.
4. Launch the Streamlit dashboard to visualize analytics results.


