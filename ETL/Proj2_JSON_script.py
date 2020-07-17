#Documentation for yfinance https://pypi.org/project/yfinance/

import yfinance as yf
from sqlalchemy import create_engine
import pandas as pd
import json

#Connect to PostgreSQL

host = "localhost"
user = "postgres"
port = "5432"
passwd = "hawkeyes"
db = "Proj2_stock_data"

engine = create_engine(f'postgresql://{user}:{passwd}@{host}:{port}/{db}')
connection = engine.connect()

#Function to create a dataframe from a PostgreSQL query
def create_pandas_table(sql_query, database = connection):
    table = pd.read_sql_query(sql_query, database)
    return table

#Query each stock's PostgreSQL table and convert to dataframe

gspc_df = create_pandas_table('SELECT * FROM public."GSPC_hist"')
n225_df = create_pandas_table('SELECT * FROM public."N225_hist"')
hsi_df = create_pandas_table('SELECT * FROM public."HSI_hist"')
ssec_df = create_pandas_table('SELECT * FROM public."SSEC_hist"')

connection.close()

gspc_df = gspc_df.drop(['Dividends', 'Stock Splits'], axis=1)
gspc_df['Date'] = gspc_df['Date'].astype(str)
gspc_lst = gspc_df.values.tolist()
with open('gspc_hist.json', 'w') as F:
    F.write(json.dumps(gspc_lst))

hsi_df = hsi_df.drop(['Dividends', 'Stock Splits'], axis=1)
hsi_df['Date'] = hsi_df['Date'].astype(str)
hsi_lst = hsi_df.values.tolist()
with open('hsi_hist.json', 'w') as F:
    F.write(json.dumps(hsi_lst))

n225_df = n225_df.drop(['Dividends', 'Stock Splits'], axis=1)
n225_df['Date'] = n225_df['Date'].astype(str)
n225_lst = n225_df.values.tolist()
with open('n225_hist.json', 'w') as F:
    F.write(json.dumps(n225_lst))

ssec_df = ssec_df.drop(['Dividends', 'Stock Splits'], axis=1)
ssec_df['Date'] = ssec_df['Date'].astype(str)
ssec_lst = ssec_df.values.tolist()
with open('ssec_hist.json', 'w') as F:
    F.write(json.dumps(ssec_lst))