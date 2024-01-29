import configparser
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import seaborn as sb
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

uel = ue.loc[:, ~ue.columns.isin(['user_id',
                                  'signup_date',
                                  'signup_week',
                                  'signup_month',
                                  'last_active_day',
                                  'ae_0',
                                  'ae_1',
                                  'ae_7',
                                  'ae_28',
                                  'ae_91',
                                  'ae_365',
                                  'r_0',
                                  'r_1',
                                  'r_7',
                                  'r_28',
                                  'r_91',
                                  'r_365',
                                  ])]

uem = uel.values

# number of clusters
sse = []
for k in range(1, 10):
    km = KMeans(n_clusters=k, random_state=2)
    km.fit(uem)
    sse.append(km.inertia_)

sb.lineplot(x=range(1, 10), y=sse)

# 3 clusters
km = KMeans(n_clusters = 3, random_state = 2)
km.fit(uem)
ue['label'] = km.labels_

# cluster centers
centers = pd.DataFrame(np.around(km.cluster_centers_, 2), columns = uel.columns)

# cluster sizes
# 0    47123
# 1      104
# 2     1058
ue.groupby('label').user_id.count()

# clusters - all activities
low_activity_cluster = ue[ue.label == 0][['ae_0', 'ae_1', 'ae_7', 'ae_28', 'ae_91', 'ae_365']]
moderate_activity_cluster = ue[ue.label == 2][['ae_0', 'ae_1', 'ae_7', 'ae_28', 'ae_91', 'ae_365']]
high_activity_cluster = ue[ue.label == 1][['ae_0', 'ae_1', 'ae_7', 'ae_28', 'ae_91', 'ae_365']]