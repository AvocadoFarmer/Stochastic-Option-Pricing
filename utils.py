import urllib
from sqlalchemy import create_engine
import cx_Oracle
import pandas as pd
from xbbg import blp

def read_sql(sql_string, db_name = 'Name of your database'):

    conn = cx_Oracle.connect('', '', db_name, encoding = 'UTF-8', nencoding = 'UTF-8')

    query_result = pd.read_sql(sql_string, conn)

    conn.close()

    return query_result

def get_df_from_ms_db(query: str, server_name: str = 'Name of your server') -> pd.DataFrame:

    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
        + server_name
        + ";Trusted_Connection=yes;"
    )

    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    df = pd.read_sql(query, engine)
    return df

def save_df_to_ms_db(
    df,
    table_name,
    schema_name,
    db_name,
    server_name,
    chunksize=None,
    fast_executemany=True,
    if_exists="append",
    method=None,
    index_flag=False,
    dtype=None,
):
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
        + server_name
        + ";DATABASE="
        + db_name
        + ";Trusted_Connection=yes;"
    )

    engine = create_engine(
        "mssql+pyodbc:///?odbc_connect=%s" % params,
        fast_executemany = fast_executemany,
        echo=False
    )

    df.to_sql(
        table_name,
        engine,
        schema=schema_name,
        if_exists=if_exists,
        index=index_flag,
        chunksize=chunksize,
        dtype=dtype,
    )

def bbg_live_picing(Table, toVD):

    RefFlds = ["PX_Last"] # add more if needed
    Tickers = Table["Tickers"]

    Live_pricing_table = blp.bdp(Tickers, RefFlds)
    Live_pricing_table.columns = ["Current Price"] # add more if needed
    Bdh_pricing_table = blp.bdh(Tickers, RefFlds, toVD, toVD)
    
    Bdh_pricing_table = Bdh_pricing_table.droplevel(1,1)
    Bdh_pricing_table = Bdh_pricing_table.transpose()
    Bdh_pricing_table.columns = ["Last Close Price"]

    Live_pricing_table = Live_pricing_table.join(Bdh_pricing_table)

    Live_pricing_table["PnL"] = Live_pricing_table["Current Price"]/Live_pricing_table["Last Close Price"] -1

    return Live_pricing_table
