import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """This function takes in a DB connection and a cursor as arguments. It then
    loops the list of drop table queiries to drop the tables."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """This function takes in a DB connection and a cursor as arguments. It then
    loops the list of table creation quieres to create the tables."""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Uses config file values to create a conneciton to a database and creates a cursor object.
    Calls drop_tables and create_tables funcitons. Closes connection to the database."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
   
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()