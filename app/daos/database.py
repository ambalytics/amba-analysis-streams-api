"""Database Setup
 get configs from enviroment and initialize the connections to
    - postgresql
    - influxdb
"""
import os
import urllib
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from event_stream.models.model import *
from influxdb_client import InfluxDBClient

# read in db config
host_server = os.environ.get('POSTGRES_HOST', 'postgres')
db_server_port = urllib.parse.quote_plus(str(os.environ.get('POSTGRES_PORT', '5432')))
database_name = os.environ.get('POSTGRES_DB', 'amba')
db_username = urllib.parse.quote_plus(str(os.environ.get('POSTGRES_USER', 'streams')))
db_password = urllib.parse.quote_plus(str(os.environ.get('POSTGRES_PASSWORD', 'REPLACE_ME')))

DATABASE_URL = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(db_username, db_password, host_server,
                                                             db_server_port, database_name)
print(DATABASE_URL)

# setup postgreql
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# setup inlux db
org = os.environ.get('INFLUXDB_V2_ORG', 'ambalytics')

client = InfluxDBClient.from_env_properties()
write_api = client.write_api()
query_api = client.query_api()
