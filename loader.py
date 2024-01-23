import configparser
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import json

# PostgreSQL connection
config = configparser.ConfigParser()
config.read('config.ini')
host = config['PostgreSQL']['host']
db = config['PostgreSQL']['database']
user = config['PostgreSQL']['user']
password = config['PostgreSQL']['password']

connection_string = 'postgresql+psycopg2://%s:%s@%s/%s' % (user, password, host, db)
engine = create_engine(connection_string)

init_sql = '''
  CREATE SCHEMA IF NOT EXISTS craft;
  DROP TABLE IF EXISTS craft.data;
'''

conn = engine.raw_connection()
cursor = conn.cursor()
cursor.execute(init_sql)
conn.commit()
cursor.close()
conn.close()

# Processing input files
sessions = pd.read_csv('input/Craft_DataAnalyst_homework.csv', index_col = 0)
# There is no unique idetifier
# sessions = sessions.drop_duplicates()
sessions.to_sql(schema = 'craft', name = 'sessions', con = engine, if_exists = 'replace')