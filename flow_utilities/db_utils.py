import pandas as pd
from prefect.client import Secret
from sqlalchemy import create_engine
from prefect import resource_manager

## postgres
"""
Using local DB in docker:
docker run -d --name demo_postgres -v dbdata:/var/lib/postgresql/data -p 5433:5432 -e POSTGRES_PASSWORD=xyz postgres:11

export PREFECT__CONTEXT__SECRETS__POSTGRES_USER=postgres
export PREFECT__CONTEXT__SECRETS__POSTGRES_PASS=your_password_1234
"""

def get_db_connection_string() -> str:
    user = Secret("POSTGRES_USER").get()
    pwd = Secret("POSTGRES_PASS").get()
    return f"postgresql://{user}:{pwd}@localhost:5433/postgres"


def get_df_from_sql_query(table_or_query: str) -> pd.DataFrame:
    db = get_db_connection_string()
    engine = create_engine(db)
    return pd.read_sql(table_or_query, engine)


def load_df_to_db(df: pd.DataFrame, table_name: str, schema: str = "jaffle_shop") -> None:
    conn_string = get_db_connection_string()
    db_engine = create_engine(conn_string)
    conn = db_engine.connect()
    conn.execute("LOCK TABLE pg_catalog.pg_namespace;") #not recommended
    conn.execute("CREATE SCHEMA IF NOT EXISTS jaffle_shop;")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;")
    df.to_sql(table_name, schema=schema, con=db_engine, index=False)
    conn.close()

## snowflake
"""
export PREFECT__CONTEXT__SECRETS__SNOWFLAKE_USER=
export PREFECT__CONTEXT__SECRETS__SNOWFLAKE_PASS=
export PREFECT__CONTEXT__SECRETS__SNOWFLAKE_ACCOUNT_ID=
export PREFECT__CONTEXT__SECRETS__SNOWFLAKE_DATABASE=
export PREFECT__CONTEXT__SECRETS__SNOWFLAKE_WAREHOUSE=
export PREFECT__CONTEXT__SECRETS__SNOWFLAKE_ROLE=
"""


def get_snowflake_connection_string(database: str = "DEV") -> str:
    user = Secret("SNOWFLAKE_USER").get()
    pwd = Secret("SNOWFLAKE_PASS").get()
    account_id = Secret("SNOWFLAKE_ACCOUNT_ID").get()
    database = Secret("SNOWFLAKE_DATABASE").get()
    warehouse = Secret("SNOWFLAKE_WAREHOUSE").get()
    role = Secret("SNOWFLAKE_ROLE").get()
    return f"snowflake://{user}:{pwd}@{account_id}/{database}/JAFFLE_SHOP?warehouse={warehouse}&role={role}"


def get_df_from_sql_query(table_or_query: str) -> pd.DataFrame:
    db = get_snowflake_connection_string()
    engine = create_engine(db)
    return pd.read_sql(table_or_query, engine)


def load_df_to_snowflake(df: pd.DataFrame, table_name: str, schema: str = "JAFFLE_SHOP") -> None:
    database = Secret("SNOWFLAKE_DATABASE").get()
    conn_string = get_snowflake_connection_string()
    db_engine = create_engine(conn_string)
    conn = db_engine.connect()
    conn.execute(f"USE DATABASE {database}")
    df.to_sql(table_name, schema=schema, con=db_engine, if_exists="replace", index=False)
    conn.close()


@resource_manager
class SnowflakeConnection:
    def __init__(self, database: str = "DEV"):
        self.database = database

    def setup(self):
        db_conn_string = get_snowflake_connection_string(self.database)
        db_engine = create_engine(db_conn_string)
        return db_engine.connect()

    @staticmethod
    def cleanup(conn):
        conn.close()