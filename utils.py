import sqlite3
from sqlite3 import Error


def get_connection(db_file):
    """Get a database connection to given SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        print("\nTrying to get database connection")
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def execute_query(conn, query):
    """create a table from the create_table_sql statement
    :param conn: Connection object
    :param query: an SQL statement to run
    :return:"""
    try:
        c = conn.cursor()
        c.execute(query)
        return c
    except Error as e:
        print(e)
        return None


queries = []

queries.append(
    """
    create view if not exists JOINED as
            select *,
            ROUND(
                (
                    julianday(when_timestamp)-julianday(event_timestamp)
                ) * 86400000
            ) as diff
        from trades t
        left join valuedata as v
            on t.instrument_id=v.instrument_id
        where diff>=0;
    """
)
queries.append(
    """
        create view if not exists fs as
            select * from (
                select *,
                    ROW_NUMBER()
                    OVER(PARTITION by instrument_id order by diff asc)
                as row_num
                from joined where diff >= (5*1000)
            ) where row_num=1;
"""
)
queries.append(
    """
        create view if not exists om as
            select * from (
                select *,
                    ROW_NUMBER()
                    OVER(PARTITION by instrument_id order by diff asc)
                as row_num
                from joined where diff >= (60*1000)
            ) where row_num=1;"""
)


queries.append(
    """
        create view if not exists tm as
            select * from (
                select *,
                    ROW_NUMBER()
                    OVER(PARTITION by instrument_id order by diff asc)
                as row_num
                from joined where diff >= (30*60*1000)
            ) where row_num=1;

"""
)

queries.append(
    """
        create view if not exists oh as
            select * from (
                select *,
                    ROW_NUMBER()
                    OVER(PARTITION by instrument_id order by diff asc)
                as row_num
                from joined where diff >= (60*60*1000)
            ) where row_num=1;
    """
)
queries.append(
    """
    select
        fs.instrument_id,
        fs.when_timestamp,
        fs.gamma as gamma_5s,
        om.gamma as gamma_1m,
        tm.gamma as gamma_30m,
        oh.gamma as gamma_60m,
        fs.vega as vega_5s,
        om.vega as vega_1m,
        tm.vega as vega_30m,
        oh.vega as vega_60m,
        fs.theta as theta_5s,
        om.theta as theta_1m,
        tm.theta as theta_30m,
        oh.theta as theta_60m
    from
        fs inner join om on fs.instrument_id=om.instrument_id
        inner join tm on fs.instrument_id=tm.instrument_id
        inner join oh on fs.instrument_id=oh.instrument_id;
"""
)
