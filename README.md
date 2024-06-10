# youtube-data-analysis


## Project Overview
This project involves developing a data pipeline to analyze YouTube channel performance metrics, with the goal of understanding engagement patterns and optimizing content strategies for data science-related YouTube channels.

## Data Collection and Preparation
- **Data Sources**: Collected data from popular data science YouTube channels including Corey Schafer, Sentdex, Krish Naik, DatascienceDoJo, Luke Barousse, Ken Jee, Alex the Analyst, Tina Huang, Data School, and codebasics.
- **Data Extraction**: Utilized the YouTube API to fetch real-time data.
- **Automation**: Employed Google Cloud Functions and Cloud Pub/Sub to automate data extraction at 5-minute intervals.
- **Storage**: Stored raw data in Google Cloud Storage in CSV format.

## Data Pipeline
- **Processing**: Utilized PySpark on Google Cloud Dataproc to clean and transform the data.
- **Loading**: Processed data was loaded into Google BigQuery for efficient querying and analysis.
- **Transformation**: Handled null values, cleaned data, added new features, normalized data into different tables, and pushed the data into BigQuery tables.

## Visualization and Tools
- **Visualization**: Created interactive dashboards using Looker Studio.
- **Tools Used**:
  - Google Cloud Functions
  - Google Cloud Pub/Sub
  - Google Cloud Storage
  - Google Cloud Dataproc
  - PySpark
  - Google BigQuery
  - Looker Studio

## Key Insights
- Identified channels with the highest average views.
- Determined the most viewed videos per channel.
- Analyzed the least engaging videos for each channel.
- Examined average view counts by day and hour.
- Identified favorite days for YouTubers to upload videos.

## Recommendations
- Optimize video release schedules based on engagement patterns.
- Focus on content types that drive higher engagement.
- Tailor strategies to leverage the peak viewing times and days.
