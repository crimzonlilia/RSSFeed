# src/processing/spark_streaming.py
import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, udf, struct, to_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType
)
from config.spark_config import SPARK_CONFIG, SPARK_PACKAGES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkStreamProcessor:
    """Spark Streaming processor for RSS articles"""
    
    def __init__(self):
        # Force Spark to use current Python interpreter
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName(SPARK_CONFIG['app_name']) \
            .master(SPARK_CONFIG['master']) \
            .config("spark.jars.packages", ','.join(SPARK_PACKAGES)) \
            .config("spark.sql.streaming.checkpointLocation", SPARK_CONFIG['checkpoint_location']) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .config("spark.network.timeout", "600s") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.default.parallelism", "2") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        
        logger.info("Spark Session created")
        logger.info(f"Using Python: {sys.executable}")
        
        self._define_schema()
        self._register_udfs()
    
    def _define_schema(self):
        """Define schema for incoming Kafka messages"""
        return StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("summary", StringType(), True),
            StructField("content", StringType(), True),
            StructField("link", StringType(), True),
            StructField("author", StringType(), True),
            StructField("published", StringType(), True),
            StructField("source", StringType(), True),
            StructField("category", StringType(), True),
            StructField("crawled_at", StringType(), True),
        ])
    
    def _register_udfs(self):
        """Register User Defined Functions"""
        
        # Clean text UDF - Khởi tạo TextCleaner bên trong
        def clean_text_udf(title, summary, content):
            from src.processing.data_cleaner import TextCleaner
            cleaner = TextCleaner()
            result = cleaner.process_article(title or "", summary or "", content or "")
            return (
                result['title_clean'],
                result['full_text'],
                result['keywords'],
                result['text_length']
            )
        
        # Sentiment analysis UDF - Khởi tạo SentimentAnalyzer bên trong
        def sentiment_udf(text):
            from src.analytics.sentiment_analyzer import SentimentAnalyzer
            analyzer = SentimentAnalyzer()
            result = analyzer.analyze(text or "")
            return (
                result['sentiment'],
                float(result['compound']),
                float(result['pos']),
                float(result['neg']),
                float(result['neu'])
            )
        
        # Register UDFs
        self.clean_udf = udf(clean_text_udf, 
                            StructType([
                                StructField("title_clean", StringType()),
                                StructField("full_text", StringType()),
                                StructField("keywords", StringType()),
                                StructField("text_length", IntegerType())
                            ]))
        
        self.sentiment_udf = udf(sentiment_udf,
                                StructType([
                                    StructField("sentiment", StringType()),
                                    StructField("compound", FloatType()),
                                    StructField("pos", FloatType()),
                                    StructField("neg", FloatType()),
                                    StructField("neu", FloatType())
                                ]))
    
    def read_from_kafka(self):
        """Read streaming data from Kafka"""
        logger.info(f"Reading from Kafka topic: {SPARK_CONFIG['kafka_topic']}")
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", SPARK_CONFIG['kafka_bootstrap_servers']) \
            .option("subscribe", SPARK_CONFIG['kafka_topic']) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON from Kafka value
        schema = self._define_schema()
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        return parsed_df
    
    def process_stream(self, df):
        """Process streaming dataframe"""
        logger.info("️ Processing stream...")
        
        # Register UDFs
        self._register_udfs()
        
        # Apply text cleaning
        df_cleaned = df.withColumn(
            "cleaned_data",
            self.clean_udf(col("title"), col("summary"), col("content"))
        )
        
        # Extract cleaned fields
        df_cleaned = df_cleaned \
            .withColumn("title_clean", col("cleaned_data.title_clean")) \
            .withColumn("full_text", col("cleaned_data.full_text")) \
            .withColumn("keywords", col("cleaned_data.keywords")) \
            .withColumn("text_length", col("cleaned_data.text_length")) \
            .drop("cleaned_data")
        
        # Apply sentiment analysis
        df_sentiment = df_cleaned.withColumn(
            "sentiment_data",
            self.sentiment_udf(col("full_text"))
        )
        
        # Extract sentiment fields
        df_final = df_sentiment \
            .withColumn("sentiment", col("sentiment_data.sentiment")) \
            .withColumn("sentiment_compound", col("sentiment_data.compound")) \
            .withColumn("sentiment_pos", col("sentiment_data.pos")) \
            .withColumn("sentiment_neg", col("sentiment_data.neg")) \
            .withColumn("sentiment_neu", col("sentiment_data.neu")) \
            .withColumn("processed_at", current_timestamp()) \
            .drop("sentiment_data", "title", "summary", "content")
        
        return df_final
    
    def write_to_parquet(self, df, output_path=None):
        """Write streaming data to Parquet files"""
        if output_path is None:
            output_path = SPARK_CONFIG['output_path']
        
        logger.info(f"Writing to Parquet: {output_path}")
        
        query = df.writeStream \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", f"{SPARK_CONFIG['checkpoint_location']}/parquet") \
            .outputMode("append") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        return query
    
    def write_to_console(self, df):
        """Write streaming data to console (for debugging)"""
        logger.info("️ Writing to console...")
        
        query = df.writeStream \
            .format("console") \
            .outputMode("append") \
            .trigger(processingTime='10 seconds') \
            .option("truncate", False) \
            .start()
        
        return query
    
    def run_pipeline(self, output_mode='parquet'):
        """Run complete streaming pipeline"""
        logger.info("Starting Spark Streaming pipeline...")
        
        # Read from Kafka
        raw_df = self.read_from_kafka()
        
        # Process stream
        processed_df = self.process_stream(raw_df)
        
        # Write output
        if output_mode == 'parquet':
            query = self.write_to_parquet(processed_df)
        elif output_mode == 'console':
            query = self.write_to_console(processed_df)
        elif output_mode == 'both':
            query1 = self.write_to_parquet(processed_df)
            query2 = self.write_to_console(processed_df)
            return [query1, query2]
        else:
            raise ValueError(f"Unknown output mode: {output_mode}")
        
        logger.info("Pipeline started successfully!")
        logger.info("Press Ctrl+C to stop...")
        
        return query
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
        logger.info("Spark Session stopped")


# Run streaming job
if __name__ == "__main__":
    processor = SparkStreamProcessor()
    
    try:
        # Run with both parquet output and console debug
        queries = processor.run_pipeline(output_mode='both')
        
        # Wait for termination
        if isinstance(queries, list):
            for query in queries:
                query.awaitTermination()
        else:
            queries.awaitTermination()
            
    except KeyboardInterrupt:
        logger.info("Stopping pipeline...")
    finally:
        processor.stop()