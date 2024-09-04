import snowflake
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from prefect import task, Flow, flow
from prefect_snowflake import SnowflakeConnector
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import pandas as pd
import os
import snowflake.connector


# Function to connect to BOTO3 client which lets you connect to S3 bucket
# and download CSV files from AirBnb for hosts, listings, and reviews.
@task
def extract_data_from_s3(bucket_name: str, file_key: str) -> str:
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    local_path = f'C:\\Users\\Benny\\AirBnbProject\\{file_key}'
    s3.download_file(bucket_name, file_key, local_path)
    return local_path


@task
def load_csv_to_pandas(local_path: str):
    df = pd.read_csv(local_path)

    # Use REGEX to replace all $ with ''
    df['price'] = df['price'].replace('[\$,]', '', regex=True).astype(float)

    # Filter for min_nights = 0 and replace value for 1.
    # If it's not 1 then keep original value.
    df.loc[df['minimum_nights'] == 0, 'minimum_nights'] = 1

    return df

# @task
# def print_spark_df_head(df, name: str):
#     print(f"Head of {name}")
#     df.show(10)


@task
def upload_to_snowflake(df: pd.DataFrame, table_name: str, local_path: str):
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        account='pgsxyoh-ny09181',
        user=os.getenv('sf_user'),
        password=os.getenv('sf_password'),
        warehouse='COMPUTE_WH',
        database='AIRBNB_PREFECT',
        schema='DEV'
    )
    cursor = conn.cursor()

    # Create table in snowflake if it doesn't exist
    create_table_query = f"""
    CREATE OR REPLACE TABLE {table_name}
                    (id integer,
                     listing_url string,
                     name string,
                     room_type string,
                     minimum_nights integer,
                     host_id integer,
                     price NUMBER(10, 2),
                     created_at datetime,
                     updated_at datetime);
    """
    cursor.execute(create_table_query)

    # Put and Copy commands to SF
    df.to_csv(local_path, index=False, header=False)

    # Put and Copy commands to upload transformed file to Snowflake
    put_query = f"PUT 'file://{local_path}' @%{table_name};"
    copy_query = (
        f"COPY INTO {table_name} \n" 
        "(id, listing_url, name,room_type, minimum_nights, host_id, price, created_at, updated_at)\n"
        f"FROM @%{table_name} \n"
        f"FILE_FORMAT = ("
            f"TYPE='CSV', \n"
            f"skip_header = 1 \n"
            f"FIELD_OPTIONALLY_ENCLOSED_BY='\"' \n"
        f")\n"
        "ON_ERROR=CONTINUE;"
    )

    cursor.execute(put_query)
    cursor.execute(copy_query)

    cursor.close()
    conn.close()


# Start the Prefect Flow for each task above
# @flow(name="Airbnb_ETL")
@flow(name="airbnb-deployment")
def airbnb_etl_flow():

    # Let's fill in key values needed to get the correct file from S3
    bucket_name = "dbtlearn"
    listings_key = "listings.csv"

    # Read CSV from S3 Bucket using params from above
    listings_data = extract_data_from_s3(bucket_name, listings_key)

    # Perform Transformations
    transformed_listings_df = load_csv_to_pandas(listings_data)

    # Print Spark DF
    # print_spark_df_head(transformed_listings_df, "listings")

    local_path = r'C:\\Users\\Benny\\AirBnbProject\\transformed_listings.csv'

    # Upload to Snowflake
    upload_to_snowflake(transformed_listings_df, "listings", local_path)


if __name__ == "__main__":
    # airbnb_etl_flow()
    airbnb_etl_flow()
