# YouTube Trending Videos Analysis

An interactive data pipeline and visualization tool for analyzing global YouTube trending patterns using Apache Spark, MySQL, and Flask.

## Overview

This project analyzes YouTube trending video data across 10 countries (CA, DE, FR, GB, IN, JP, KR, MX, RU, US) to identify engagement patterns, popular categories, and trending hashtags. The system processes 539.22 MB of data using distributed computing with Spark and presents results through an interactive map-based web interface.

## Features

- **Interactive World Map**: Click on any country to view engagement metrics, top categories, and trending hashtags
- **Search Functionality**: Filter videos by title, category, or tags across different countries
- **Engagement Analysis**: Calculated engagement rates based on likes, comments, and views
- **Top-N Rankings**: Dynamically view top 5, 10, or 15 results for each metric
- **Multi-Country Comparison**: Compare trending patterns across 10 different regions

## Architecture

```
Raw Data (CSV/JSON)
    ↓
Apache Spark Processing (Distributed Computing)
    ↓
MySQL Database (3 Tables)
    ↓
Flask Backend (Python)
    ↓
Interactive Web Interface (Folium Maps)
```

## Tech Stack

- **Data Processing**: Apache Spark 3.5.0, PySpark
- **Database**: MySQL 8.0.43
- **Backend**: Flask, Python 3.10.19
- **Frontend**: Folium (Leaflet.js), HTML/CSS
- **Data Analysis**: Pandas, NumPy

## Dataset

Source: [YouTube Trending Videos Dataset (Kaggle)](https://www.kaggle.com/datasets/datasnaek/youtube-new)

**Size**: 539.22 MB  
**Countries**: CA, DE, FR, GB, IN, JP, KR, MX, RU, US  
**Records**: 48,137+ trending videos

### Key Attributes
- Video metadata (title, channel, category, tags)
- Engagement metrics (views, likes, dislikes, comments)
- Temporal data (trending date, publish time)
- Country-specific data

## Installation

### Prerequisites
```bash
Python 3.10.19
Apache Spark 3.5.0
MySQL 8.0.43
MySQL Connector/J 8.3.0
```

### Setup

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/youtube-trends-analysis.git
cd youtube-trends-analysis
```

2. **Create and activate Spark environment**
```bash
conda create -n spark_env python=3.10.19
conda activate spark_env
```

3. **Install Python dependencies**
```bash
pip install pyspark pandas mysql-connector-python flask folium
```

4. **Download MySQL Connector**
```bash
# Download MySQL Connector/J 8.3.0
# Place in ~/mysql-connector/mysql-connector-j-8.3.0/
```

5. **Set up MySQL database**
```bash
cs179g_db_start
mysql -h 127.0.0.1 -u root

# Create database
CREATE DATABASE yt169;
```

6. **Download dataset**
- Download from [Kaggle](https://www.kaggle.com/datasets/datasnaek/youtube-new)
- Extract to `dataset/` directory

## Usage

### 1. Process Data with Spark

```bash
conda activate spark_env
cd part_2
python part_2.py
```

This script will:
- Read CSV and JSON files for all countries
- Clean and transform data (dates, tags, categories)
- Calculate engagement metrics
- Generate category and tag frequency tables
- Write results to MySQL database

**Expected tables in MySQL:**
- `engagement_metrics` - Video engagement rates
- `category_count` - Category frequencies by country
- `tag_count` - Tag frequencies by country

### 2. Run the Web Application

```bash
cd part_3
python app.py
```

Access the application at: `http://127.0.0.1:5000`

### 3. Query Database Directly (Optional)

```bash
mysql -h 127.0.0.1 -u root -D yt169

# Example queries
SELECT * FROM engagement_metrics WHERE country = 'US' ORDER BY engagement_rate DESC LIMIT 10;
SELECT * FROM category_count WHERE country = 'US' ORDER BY num_categories DESC;
SELECT * FROM tag_count WHERE country = 'US' ORDER BY num_tags DESC LIMIT 20;
```

## Database Schema

### engagement_metrics
| Column | Type | Description |
|--------|------|-------------|
| country | VARCHAR | Country code |
| trending_year | INT | Year video trended |
| upload_year | INT | Year video was uploaded |
| category | VARCHAR | Video category |
| title | VARCHAR | Video title |
| video_count | INT | Number of times video trended |
| avg_views | INT | Average views |
| avg_likes | INT | Average likes |
| avg_comments | INT | Average comments |
| engagement_rate | FLOAT | (likes + comments) / views |

### category_count
| Column | Type | Description |
|--------|------|-------------|
| country | VARCHAR | Country code |
| category | VARCHAR | Category name |
| num_categories | INT | Frequency count |

### tag_count
| Column | Type | Description |
|--------|------|-------------|
| country | VARCHAR | Country code |
| upload_year | INT | Upload year |
| tag | VARCHAR | Tag text |
| num_tags | INT | Frequency count |

## Key Findings

### Engagement Patterns
- **Music and Entertainment** dominate trending lists globally
- **Engagement rate** (0.23-0.33) matters more than raw view counts
- K-pop content (BTS, j-hope) shows exceptionally high engagement
- Active fan communities drive engagement regardless of total views

### Regional Differences
- **US, CA, IN**: High frequency of humor-related tags ("funny", "comedy")
- **JP, RU**: Language-specific tags dominate
- **US, GB, IN**: Higher engagement for Music and Entertainment categories

### Performance Insights
- **1 Worker**: 8.47s (full dataset)
- **2 Workers**: 0.78s (full dataset) - **91% improvement**
- Spark's distributed processing significantly reduces runtime on large datasets
- Startup overhead dominates performance on small datasets (<10K rows)

## Web Interface Features

### Interactive Map
- Click country markers to view statistics
- Collapsible sections for Engagement, Categories, and Tags
- Tooltips show country names on hover

### Search Panel
- **Search Query**: Find specific videos by title, category, or tag
- **Type Filter**: Choose between Engagement Rate, Categories, or Tags
- **Country Filter**: Select specific country or view all
- **Results Display**: Ranked results with engagement metrics

### Top-N Selector
- Toggle between top 5, 10, or 15 results
- Dynamically updates map popups

## Performance Benchmarks

### Dataset Sizes
- All Countries: 488 MB
- United States Only: 60 MB

### Execution Times (Hashtag Computation)
| Configuration | Time |
|--------------|------|
| US (small), 1 worker | 1.24s |
| All Countries, 1 worker | 3.20s |
| All Countries, 2 workers | 2.20s |

## Project Structure

```
youtube-trends-analysis/
├── dataset/
│   ├── CAvideos.csv
│   ├── CA_category_id.json
│   ├── DEvideos.csv
│   ├── DE_category_id.json
│   └── ... (other countries)
├── part_2/
│   └── part_2.py          # Spark data processing
├── part_3/
│   └── app.py             # Flask web application
├── README.md
└── requirements.txt
```

## Challenges & Solutions

### Data Cleaning
- **Challenge**: Inconsistent date formats across countries
- **Solution**: Standardized parsing with fallback formats

### Tag Processing
- **Challenge**: Mixed delimiters and special characters in tags
- **Solution**: Regex-based cleaning and normalization

### Database Integration
- **Challenge**: Schema mismatches between Spark and MySQL
- **Solution**: Pre-validation of DataFrame schemas before JDBC writes

### Performance
- **Challenge**: Slow queries on single-worker configuration
- **Solution**: Implemented multi-worker Spark setup with caching

## Future Improvements

- Add time-series analysis for trending patterns over time
- Implement sentiment analysis on video titles and descriptions
- Include recommendation system based on engagement patterns
- Add real-time data updates from YouTube API
- Expand to more countries and regions
