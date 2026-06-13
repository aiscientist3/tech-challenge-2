"""
Connections for the streaming ingestion pipeline.

Import submodules directly to avoid loading BigQuery deps in the Kafka consumer:
  - ingestion.streaming.connections.aws_credentials
  - ingestion.streaming.connections.spark_s3
  - ingestion.streaming.connections.bigquery_client  (producer only)
"""
