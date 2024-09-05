import pandas as pd
import pytest
import os
from sqlalchemy import create_engine
import snowflake.connector

snowflake_user = os.getenv('sf_user'),
snowflake_password = os.getenv('sf_password'),
snowflake_account = 'pgsxyoh-ny09181'
snowflake_warehouse = 'COMPUTE_WH'
snowflake_database = 'AIRBNB_PREFECT'
snowflake_schema = 'DEV'
sf_table_name = 'LISTINGS'

conn = snowflake.connector.connect(
    account='pgsxyoh-ny09181',
    user=os.getenv('sf_user'),
    password=os.getenv('sf_password'),
    warehouse='COMPUTE_WH',
    database='AIRBNB_PREFECT',
    schema='DEV'
)


def load_data_from_snowflake():
    try:
        query = f"select * from {snowflake_database}.{snowflake_schema}.{sf_table_name}"
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(e)
    finally:
        conn.close()


@pytest.fixture(scope="module")
def snowflake_data():
    return load_data_from_snowflake()


def test_price_column_numeric(snowflake_data):
    assert not snowflake_data['PRICE'].astype(str).str.contains(r'\$', regex=True,).any(), \
        "Price Column contains $ values"


def test_min_nights_greater_than_one(snowflake_data):
    assert (snowflake_data['MINIMUM_NIGHTS'] >= 1).all(), \
        "Not all values in minimum nights are greater than or equal to 1"


if __name__ == "__main__":
    pytest.main()
