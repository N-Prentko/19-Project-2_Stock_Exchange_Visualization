#Documentation for yfinance https://pypi.org/project/yfinance/

import yfinance as yf
from sqlalchemy import create_engine
import pandas as pd

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

# Set the date as the dataframe index for each stock price dataframe

gspc_df = gspc_df.set_index('Date')
n225_df = n225_df.set_index('Date')
hsi_df = hsi_df.set_index('Date')
ssec_df = ssec_df.set_index('Date')

# Convert dataframe to JSON file. To be set at the URL destination for the relevant stock

gspc_df.to_json("gspc_hist.json", orient='index')
n225_df.to_json("n225_hist.json", orient='index')
hsi_df.to_json("hsi_hist.json", orient='index')
ssec_df.to_json("ssec_hist.json", orient='index')


