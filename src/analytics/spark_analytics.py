import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, hour, to_date
from config.spark_config import SPARK_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkAnalytics:
    """Analytics on processed RSS data"""
    
    def __init__(self, data_path=None):
        self.data_path = data_path or SPARK_CONFIG['output_path']
        
        # Initialize Spark
        self.spark = SparkSession.builder \
            .appName("RSS Analytics") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        logger.info(" Spark Analytics initialized")
    
    def load_data(self):
        """Load processed data from Parquet"""
        try:
            df = self.spark.read.parquet(self.data_path)
            logger.info(f" Loaded {df.count()} articles")
            return df
        except Exception as e:
            logger.error(f" Error loading data: {e}")
            return None
    
    def sentiment_distribution(self, df):
        """Analyze sentiment distribution"""
        result = df.groupBy("sentiment") \
            .agg(count("*").alias("count")) \
            .orderBy(desc("count"))
        
        logger.info("\n Sentiment Distribution:")
        result.show()
        return result
    
    def category_sentiment(self, df):
        """Sentiment analysis by category"""
        result = df.groupBy("category", "sentiment") \
            .agg(
                count("*").alias("count"),
                avg("sentiment_compound").alias("avg_score")
            ) \
            .orderBy("category", desc("count"))
    
        logger.info("\n Category Sentiment Analysis:")
        result.show()
        return result
    
    def top_keywords(self, df, limit=20):
        """Extract top keywords across all articles"""
        from pyspark.sql.functions import explode, split, trim, lower
        
        keywords_df = df.select(explode(split(col("keywords"), ",")).alias("keyword"))
        
        result = keywords_df \
            .withColumn("keyword", trim(lower(col("keyword")))) \
            .filter(col("keyword") != "") \
            .groupBy("keyword") \
            .agg(count("*").alias("count")) \
            .orderBy(desc("count")) \
            .limit(limit)
        
        logger.info(f"\n Top {limit} Keywords:")
        result.show(limit)
        return result
    
    def source_analysis(self, df):
        """Analyze articles by source"""
        result = df.groupBy("source") \
            .agg(
                count("*").alias("total_articles"),
                avg("sentiment_compound").alias("avg_sentiment"),
                avg("text_length").alias("avg_length")
            ) \
            .orderBy(desc("total_articles"))
        
        logger.info("\n Source Analysis:")
        result.show()
        return result
    
    def hourly_activity(self, df):
        """Analyze article distribution by hour"""
        result = df.withColumn("hour", hour(col("processed_at"))) \
            .groupBy("hour") \
            .agg(count("*").alias("count")) \
            .orderBy("hour")
        
        logger.info("\n Hourly Activity:")
        result.show(24)
        return result
    
    def positive_negative_ratio(self, df):
        """Calculate positive/negative ratio by category"""
        from pyspark.sql.functions import when, sum as spark_sum
        
        result = df.groupBy("category") \
            .agg(
                spark_sum(when(col("sentiment") == "positive", 1).otherwise(0)).alias("positive"),
                spark_sum(when(col("sentiment") == "negative", 1).otherwise(0)).alias("negative"),
                spark_sum(when(col("sentiment") == "neutral", 1).otherwise(0)).alias("neutral")
            ) \
            .orderBy("category")
        
        logger.info("\n Sentiment Ratio by Category:")
        result.show()
        return result
    
    def run_all_analytics(self):
        """Run all analytics queries"""
        df = self.load_data()
        
        if df is None or df.count() == 0:
            logger.warning(" No data found!")
            return
        
        # Run all analytics
        self.sentiment_distribution(df)
        self.category_sentiment(df)
        self.top_keywords(df)
        self.source_analysis(df)
        self.hourly_activity(df)
        self.positive_negative_ratio(df)
    
    def get_latest_articles(self, df, limit=10):
        """Get latest processed articles"""
        result = df.select(
            "title_clean", "source", "category", "sentiment", 
            "sentiment_compound", "processed_at"
        ).orderBy(desc("processed_at")).limit(limit)
        
        logger.info(f"\n Latest {limit} Articles:")
        result.show(limit, truncate=50)
        return result
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
        logger.info(" Spark stopped")


# Test analytics
if __name__ == "__main__":
    analytics = SparkAnalytics()
    analytics.run_all_analytics()
    analytics.stop()