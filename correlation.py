import configparser
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import seaborn as sb
import numpy as np
from sklearn.cluster import KMeans

# PostgreSQL connection
config = configparser.ConfigParser()
config.read('config.ini')
host = config['PostgreSQL']['host']
db = config['PostgreSQL']['database']
user = config['PostgreSQL']['user']
password = config['PostgreSQL']['password']

connection_string = 'postgresql+psycopg2://%s:%s@%s/%s' % (user, password, host, db)
engine = create_engine(connection_string)

ue = pd.read_sql_table('users_events', schema = 'dbt_craft', con = engine.connect())
ue = ue.loc[:, ~ue.columns.isin(['last_active_day'])]

sb.set(rc={'figure.figsize': (30, 30)})
sb.heatmap(ue.corr(), cmap = "YlGnBu", annot = True)

# recoding to bools
ueb = ue.select_dtypes(include='float').astype(bool).astype(int)
sb.heatmap(ueb.corr(), cmap = "YlGnBu", annot = True)