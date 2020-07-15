#Documentation for yfinance https://pypi.org/project/yfinance/

import yfinance as yf
from sqlalchemy import create_engine
import pandas as pd

# Use yfinance to read in information and Open/High/Low/Close/Volumen stock price history

gspc = yf.Ticker("^GSPC")
gspc_info = gspc.info
gspc_hist = gspc.history(period="3mo")

n225 = yf.Ticker("^N225")
n225_info = n225.info
n225_hist = n225.history(period="3mo")

hsi = yf.Ticker("^HSI")
hsi_info = hsi.info
hsi_hist = hsi.history(period="3mo")

ssec = yf.Ticker("000001.SS")
ssec_info = ssec.info
ssec_hist = ssec.history(period="3mo")

#Connect to PostgreSQL

host = "localhost"
user = "postgres"
port = "5432"
passwd = "hawkeyes"
db = "Proj2_stock_data"

engine = create_engine(f'postgresql://{user}:{passwd}@{host}:{port}/{db}')
connection = engine.connect()

#Write each stock history to a PostgreSQL table in the Proj2_stock_data database

gspc_hist.to_sql('GSPC_hist', con=connection, if_exists = 'replace', index=True)
n225_hist.to_sql('N225_hist', con=connection, if_exists = 'replace', index=True)
hsi_hist.to_sql('HSI_hist', con=connection, if_exists = 'replace', index=True)
ssec_hist.to_sql('SSEC_hist', con=connection, if_exists = 'replace', index=True)

connection.close()
