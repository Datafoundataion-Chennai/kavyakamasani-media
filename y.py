import pandas as pd 
from google.cloud import bigquery

df=pd.read_csv("youtube_videos.csv")
df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")


schema = [
    bigquery.SchemaField("video_id", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("channel_title", "STRING"),
    bigquery.SchemaField("category_id", "INTEGER"),
    bigquery.SchemaField("publish_time", "DATETIME"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("tags", "STRING"),
    bigquery.SchemaField("thumbnail_link", "STRING"),
    bigquery.SchemaField("video_link", "STRING"),
    bigquery.SchemaField("views", "INTEGER"),
    bigquery.SchemaField("likes", "INTEGER"),
    bigquery.SchemaField("comment_count", "INTEGER"),
    bigquery.SchemaField("comments_disabled", "BOOLEAN"),
    bigquery.SchemaField("channel_name", "STRING"),
    bigquery.SchemaField("subscribers_count", "INTEGER"),
    bigquery.SchemaField("total_videos", "INTEGER")
]


client = bigquery.Client()


dataset_id = "mediaanalyticsplatform.media_analytics"
table_id = dataset_id + ".youtube_videos"


job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()  
print(f"Uploaded {df.shape[0]} rows to {table_id}")
