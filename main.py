import pandas as pd
from sqlite3 import Error

from utils import get_connection, execute_query, queries

DATABASE = "db/project.db"
raw_trades_data_path = "data/trades.json"
raw_valuedata_path = "data/valuedata.json"


def read_raw_data(raw_trades_data_path, raw_valuedata_path):
    """Read raw data from json file paths provided in arguments
        & return dataframes
    :param raw_trades_data_path: raw trades json file path
    :param raw_valuedata_path: raw valuedata json file path
    :return: trades & valuedata dataframes
    """
    trades = pd.read_json(
        raw_trades_data_path, lines=True, convert_dates=["event_timestamp"]
    )
    valuedata = pd.read_json(
        raw_valuedata_path, lines=True, convert_dates=["when_timestamp"]
    )
    return trades, valuedata


def write_to_database(trades, valuedata):
    """Write data in dataframes to SQLite database

    :param trades: trades Dataframe
    :param valuedata: valuedata Dataframe
    :return:"""
    try:
        trades.to_sql("trades", conn, if_exists="replace")
        valuedata.to_sql("valuedata", conn, if_exists="replace")
        print("Created tables in database with raw data successfully")
    except Error as e:
        print(e)


if __name__ == "__main__":
    trades, valuedata = read_raw_data(raw_trades_data_path, raw_valuedata_path)
    print("Data files read successfully")

    # create a database connection
    conn = get_connection(DATABASE)

    # create tables
    if conn is not None:
        print("Acqiured database connection successfully")
        write_to_database(trades, valuedata)
        print("Running qeries:: ")
        for query in queries:
            print(f"Executing query-> {query}")
            results = execute_query(conn, query)
            print("Executed query successfully!")

        print("All queries executed")
        print(results.fetchall())
        print("Closing database connection")
        conn.close()
    else:
        print("Can't create database connection")
