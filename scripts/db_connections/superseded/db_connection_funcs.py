"""Module with functions to connect to
PostgreSQL
"""

import psycopg2
from sqlalchemy import create_engine

def create_postgresql_engine(
    host="localhost",
    database="web_spider_data",
    user="postgres",
    password="postgres",
    port="5432",
):
    """Creates a connection to the target PostgreSQL database

    Parameters:
    1. host: PostgreSQL host
    2. database: Database within PostgreSQL to connect to
    3. user: Database username
    4. password: Database password

    Returns:
    1. engine: sqlalchemy engine to use to connect to PostgreSQL
    """

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )
    return engine
