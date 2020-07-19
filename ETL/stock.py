#Dependencies, libraries, and imports

import pandas as pd
import yfinance as yf
import json

#SQLalchemy libraries and functions
import sqlalchemy
from sqlalchemy import create_engine

#BEGIN FLASK APP
from flask import Flask, jsonify, render_template, redirect

gspc = yf.Ticker("^GSPC")
gspc_info = gspc.info
gspc_hist = gspc.history(period="max")

n225 = yf.Ticker("^N225")
n225_info = n225.info
n225_hist = n225.history(period="max")

hsi = yf.Ticker("^HSI")
hsi_info = hsi.info
hsi_hist = hsi.history(period="max")

ssec = yf.Ticker("000001.SS")
ssec_info = ssec.info
ssec_hist = ssec.history(period="max")

#Connect to PostgreSQL

host = "localhost"
user = "postgres"
port = "5432"
passwd = "postgres"
db = "Proj2_stock_data"

engine = create_engine(f'postgresql://{user}:{passwd}@{host}:{port}/{db}')
connection = engine.connect()

#Write each stock history to a PostgreSQL table in the Proj2_stock_data database

gspc_hist.to_sql('GSPC_hist', con=connection, if_exists = 'replace', index=True)
n225_hist.to_sql('N225_hist', con=connection, if_exists = 'replace', index=True)
hsi_hist.to_sql('HSI_hist', con=connection, if_exists = 'replace', index=True)
ssec_hist.to_sql('SSEC_hist', con=connection, if_exists = 'replace', index=True)

#Function to create a dataframe from a PostgreSQL query
def create_pandas_table(sql_query, database = connection):
    table = pd.read_sql_query(sql_query, database)
    return table

app = Flask(__name__)

@app.route("/")
def home():    
    return render_template("index.html")

@app.route("/sp500/")
def sp500html():    
    return render_template("sp500.html")

@app.route("/hangseng/")
def hangsenghtml():    
    return render_template("hangseng.html")

@app.route("/nikkei225/")
def nikkei225html():    
    return render_template("nikkei225.html")

@app.route("/shanghai/")
def shanghaihtml():    
    return render_template("shanghai.html")


@app.route("/gspc/")
def gspc_json():

    #Query each stock's PostgreSQL table and convert to dataframe

    gspc_df = create_pandas_table('SELECT * FROM public."GSPC_hist"')

    gspc_df = gspc_df.drop(['Dividends', 'Stock Splits'], axis=1)
    gspc_df['Date'] = gspc_df['Date'].astype(str)
    gspc_lst = gspc_df.values.tolist()

    pass_json = jsonify(gspc_lst)

    return (pass_json)

@app.route("/hsi/")
def hsi_json():

    #Query each stock's PostgreSQL table and convert to dataframe

    hsi_df = create_pandas_table('SELECT * FROM public."HSI_hist"')

    hsi_df = hsi_df.drop(['Dividends', 'Stock Splits'], axis=1)
    hsi_df['Date'] = hsi_df['Date'].astype(str)
    hsi_lst = hsi_df.values.tolist()

    pass_json = jsonify(hsi_lst)

    return (pass_json)

@app.route("/n225/")
def n225_json():

    #Query each stock's PostgreSQL table and convert to dataframe

    n225_df = create_pandas_table('SELECT * FROM public."N225_hist"')

    n225_df = n225_df.drop(['Dividends', 'Stock Splits'], axis=1)
    n225_df['Date'] = n225_df['Date'].astype(str)
    n225_lst = n225_df.values.tolist()

    pass_json = jsonify(n225_lst)

    return (pass_json)

@app.route("/ssec/")
def ssec_json():

#     #Query each stock's PostgreSQL table and convert to dataframe

    ssec_df = create_pandas_table('SELECT * FROM public."SSEC_hist"')

    ssec_df = ssec_df.drop(['Dividends', 'Stock Splits'], axis=1)
    ssec_df['Date'] = ssec_df['Date'].astype(str)
    ssec_lst = ssec_df.values.tolist()

    pass_json = jsonify(ssec_lst)

    return (pass_json)

if __name__ == "__main__":
    app.run(debug=True)


