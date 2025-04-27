import os
import json
import boto3
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(level=logging.INFO, force=True)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = 'https://www.governor.ny.gov/past-executive-orders'
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'politicai-ny-past-etl')
SRC_VALUE = 'governor.ny.gov'
DB_SECRET_NAME = os.environ.get('DB_SECRET_NAME')

# Initialize AWS clients
s3_client = boto3.client('s3')
secretsmanager_client = boto3.client('secretsmanager')

def get_db_credentials():
    """
    Retrieve database credentials from AWS Secrets Manager.
    """
    if not DB_SECRET_NAME:
        logger.error("DB_SECRET_NAME environment variable not set")
        return None

    try:
        secret_response = secretsmanager_client.get_secret_value(SecretId=DB_SECRET_NAME)
        secret = json.loads(secret_response['SecretString'])
        return secret
    except ClientError as e:
        logger.error(f"Failed to retrieve database credentials: {e}")
        return None

def get_db_connection():
    """
    Create a database connection using SQLAlchemy.
    """
    creds = get_db_credentials()
    if not creds:
        logger.error("Database credentials not available")
        return None

    try:
        db_user = creds['username']
        db_password = creds['password']
        db_host = creds['host']
        db_port = creds.get('port', '5432')
        db_name = creds['dbname']

        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string, echo=False)
        # Test the connection
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.info("Database connection established successfully")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def db_upsert(engine, table_name, data, conflict_key, schema=None):
    """
    Perform an upsert operation into the specified table with retries.
    """
    if not engine:
        logger.error("No database engine provided for upsert")
        return False

    try:
        # Construct the table name with schema if provided
        if schema:
            full_table_name = f"{schema}.{table_name}"
        else:
            full_table_name = table_name

        # Prepare the insert statement
        columns = list(data.keys())
        values = [data[col] for col in columns]
        placeholders = ', '.join([f':{col}' for col in columns])
        insert_query = f"INSERT INTO {full_table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        # Prepare the update statement for conflict
        conflict_columns = conflict_key if isinstance(conflict_key, list) else [conflict_key]
        conflict_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns])
        on_conflict_query = f" ON CONFLICT ({', '.join(conflict_columns)}) DO UPDATE SET {conflict_clause}"

        # Combine the full query
        full_query = f"{insert_query} {on_conflict_query}"

        with engine.connect() as connection:
            with connection.begin():  # Ensure transaction
                connection.execute(text(full_query), data)
        logger.info(f"Successfully upserted data into {full_table_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to upsert into {full_table_name}: {e}")
        raise  # Re-raise for retry

def save_to_s3(data, bucket_name):
    """
    Save the data to S3 as a JSON file with a timestamped filename.
    """
    try:
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        file_name = f"historical_orders_{timestamp}.json"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        logger.info(f"Successfully saved data to S3: {file_name}")
        return file_name
    except ClientError as e:
        logger.error(f"Failed to save to S3: {e}")
        return None

def get_data_from_s3(bucket_name, file_name):
    """
    Retrieve data from an S3 file.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        data = json.loads(response['Body'].read().decode('utf-8'))
        logger.info(f"Successfully retrieved data from S3: {file_name}")
        return data, file_name
    except ClientError as e:
        logger.error(f"Failed to retrieve data from S3: {e}")
        return None, None

def get_latest_file_from_s3(bucket_name=S3_BUCKET_NAME):
    """
    Retrieve the most recent file from S3 based on the timestamp in the filename.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='historical_orders_')
        if 'Contents' not in response:
            logger.warning("No files found in S3 bucket")
            return None

        # Sort files by LastModified date
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        latest_file = files[0]['Key']
        logger.info(f"Found latest file in S3: {latest_file}")
        return latest_file
    except ClientError as e:
        logger.error(f"Failed to list files in S3: {e}")
        return None