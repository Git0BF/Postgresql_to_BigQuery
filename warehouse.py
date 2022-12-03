import os
from google.cloud import bigquery
from sqlalchemy import create_engine

# Set environment variable for authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "<path-to-service-account-credentials>"

# Connect to BigQuery
bigquery_client = bigquery.Client()

# Set up Postgres SQL database connection
engine = create_engine("postgresql://<user>:<password>@<host>:<port>/<database-name>")

# Retrieve the list of tables from the Postgres SQL database
tables = engine.table_names()

# Iterate over the tables and warehouse each one
for table in tables:
  # Warehouse the data from BigQuery to Postgres SQL
  bigquery_client.query(f"SELECT * FROM {table}").to_dataframe().to_sql(table, engine)

# Generate matching tables on BigQuery
for table in tables:
  schema = []
  # Retrieve the columns and data types from the Postgres SQL table
  columns = engine.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'").fetchall()
  # Create a schema for the table on BigQuery
  for column in columns:
    schema.append(bigquery.SchemaField(column[0], column[1]))
  # Create the table on BigQuery
  bigquery_table = bigquery_client.create_table(bigquery.Table(f"{bigquery_client.project}.{table}", schema=schema))
  # Load the data from the Postgres SQL table into the BigQuery table
  bigquery_client.load_table_from_sql(engine.execute(f"SELECT * FROM {table}"), bigquery_table)
