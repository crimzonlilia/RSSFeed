import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from config.spark_config import SPARK_CONFIG

# Page config
st.set_page_config(
    page_title="RSS Analytics Dashboard",
    page_icon="",
    layout="wide"
)

# Initialize Spark (cached)
@st.cache_resource
def init_spark():
    spark = SparkSession.builder \
        .appName("RSS Dashboard") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# Load data (cached for 1 minute)
@st.cache_data(ttl=60)
def load_data():
    spark = init_spark()
    try:
        df = spark.read.parquet(SPARK_CONFIG['output_path'])
        return df.toPandas()
    except:
        return pd.DataFrame()

# Title
st.title("RSS News Analytics Dashboard")
st.markdown("Real-time analytics from RSS feeds")

# Sidebar filters
st.sidebar.header("Filters")

# Load data
df = load_data()

if df.empty:
    st.warning("No data available. Run the streaming pipeline first!")
    st.stop()

# Convert timestamp
df['processed_at'] = pd.to_datetime(df['processed_at'])
df['date'] = df['processed_at'].dt.date
df['hour'] = df['processed_at'].dt.hour

# Filters
categories = ['All'] + sorted(df['category'].unique().tolist())
selected_category = st.sidebar.selectbox("Category", categories)

sources = ['All'] + sorted(df['source'].unique().tolist())
selected_source = st.sidebar.selectbox("Source", sources)

sentiments = ['All'] + sorted(df['sentiment'].unique().tolist())
selected_sentiment = st.sidebar.selectbox("Sentiment", sentiments)

# Apply filters
filtered_df = df.copy()
if selected_category != 'All':
    filtered_df = filtered_df[filtered_df['category'] == selected_category]
if selected_source != 'All':
    filtered_df = filtered_df[filtered_df['source'] == selected_source]
if selected_sentiment != 'All':
    filtered_df = filtered_df[filtered_df['sentiment'] == selected_sentiment]

# Metrics
st.header("Key Metrics")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Articles", len(filtered_df))

with col2:
    avg_sentiment = filtered_df['sentiment_compound'].mean()
    st.metric("Avg Sentiment Score", f"{avg_sentiment:.3f}")

with col3:
    positive_pct = (filtered_df['sentiment'] == 'positive').sum() / len(filtered_df) * 100
    st.metric("Positive %", f"{positive_pct:.1f}%")

with col4:
    avg_length = filtered_df['text_length'].mean()
    st.metric("Avg Text Length", f"{avg_length:.0f}")

# Refresh button
if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Visualizations
st.header("Analytics")

# Row 1: Sentiment Distribution
col1, col2 = st.columns(2)

with col1:
    st.subheader("Sentiment Distribution")
    sentiment_counts = filtered_df['sentiment'].value_counts()
    fig = px.pie(
        values=sentiment_counts.values,
        names=sentiment_counts.index,
        color=sentiment_counts.index,
        color_discrete_map={
            'positive': '#00CC96',
            'negative': '#EF553B',
            'neutral': '#636EFA'
        }
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Category Distribution")
    category_counts = filtered_df['category'].value_counts()
    fig = px.bar(
        x=category_counts.index,
        y=category_counts.values,
        labels={'x': 'Category', 'y': 'Count'},
        color=category_counts.values,
        color_continuous_scale='Blues'
    )
    st.plotly_chart(fig, use_container_width=True)

# Row 2: Sentiment by Category
st.subheader("Sentiment by Category")
sentiment_category = filtered_df.groupby(['category', 'sentiment']).size().reset_index(name='count')
fig = px.bar(
    sentiment_category,
    x='category',
    y='count',
    color='sentiment',
    barmode='group',
    color_discrete_map={
        'positive': '#00CC96',
        'negative': '#EF553B',
        'neutral': '#636EFA'
    }
)
st.plotly_chart(fig, use_container_width=True)

# Row 3: Time Series
st.subheader("Articles Over Time")
time_series = filtered_df.groupby(filtered_df['processed_at'].dt.floor('H')).size().reset_index(name='count')
fig = px.line(
    time_series,
    x='processed_at',
    y='count',
    labels={'processed_at': 'Time', 'count': 'Number of Articles'}
)
st.plotly_chart(fig, use_container_width=True)

# Row 4: Top Keywords
st.subheader("Top 20 Keywords")
all_keywords = []
for keywords in filtered_df['keywords'].dropna():
    all_keywords.extend([k.strip() for k in str(keywords).split(',')])

keyword_counts = pd.Series(all_keywords).value_counts().head(20)
fig = px.bar(
    x=keyword_counts.values,
    y=keyword_counts.index,
    orientation='h',
    labels={'x': 'Count', 'y': 'Keyword'},
    color=keyword_counts.values,
    color_continuous_scale='Viridis'
)
fig.update_layout(height=500)
st.plotly_chart(fig, use_container_width=True)

# Row 5: Source Analysis
st.subheader("Source Analysis")
source_stats = filtered_df.groupby('source').agg({
    'sentiment_compound': 'mean',
    'text_length': 'mean',
    'source': 'count'
}).rename(columns={'source': 'count'}).reset_index()

fig = go.Figure()
fig.add_trace(go.Bar(
    name='Article Count',
    x=source_stats['source'],
    y=source_stats['count'],
    yaxis='y',
    offsetgroup=1
))
fig.add_trace(go.Scatter(
    name='Avg Sentiment',
    x=source_stats['source'],
    y=source_stats['sentiment_compound'],
    yaxis='y2',
    mode='lines+markers',
    marker=dict(size=10)
))

fig.update_layout(
    xaxis=dict(title='Source'),
    yaxis=dict(title='Article Count'),
    yaxis2=dict(title='Avg Sentiment Score', overlaying='y', side='right'),
    hovermode='x'
)
st.plotly_chart(fig, use_container_width=True)

# Latest Articles Table
st.header("Latest Articles")
latest = filtered_df.nlargest(10, 'processed_at')[
    ['title_clean', 'source', 'category', 'sentiment', 'sentiment_compound', 'processed_at']
]
st.dataframe(latest, use_container_width=True)

# Footer
st.sidebar.markdown("---")
st.sidebar.info(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
st.sidebar.markdown("Built using Streamlit")