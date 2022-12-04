from google.oauth2.credentials import Credentials
from google.cloud import bigquery

# Set the path to the credentials file
credentials_file = "path/to/credentials.json"

# Load the credentials from the file
credentials = Credentials.from_authorized_user_file(credentials_file)

# Set the project and dataset ID
project_id = "your-project-id"
dataset_id = "your-dataset-id"

# Create a new BigQuery client using the credentials
client = bigquery.Client(credentials=credentials, project=project_id)

# Set the SQL query
sql_query = "SELECT * FROM `your-table-id`"

# Execute the query and retrieve the results
query_results = client.query(sql_query).to_dataframe()

# Print the results
print(query_results)
