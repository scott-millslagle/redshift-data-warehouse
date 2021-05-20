import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """This function takes in a DB connection and a cursor as arguments. It then
    loops the list of copy queiries to copy data from s3 buckets to staging tables."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """This function takes in a DB connection and a cursor as arguments. It then
    loops the list of insert queiries to insert data from staging tables to final db tables."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Uses config file values to create a conneciton to a database and creates a cursor object.
    Calls load_staging_tables and insert_tables funcitons. Closes connection to the database."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
