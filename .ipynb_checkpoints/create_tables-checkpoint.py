import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Method that deletes the tables from the database to be able to recreate them
    """
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Method that creates the tables in the database
    """
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Main method: Calls the drop and create tables method
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print('Conn string:' + "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Dropping tables')
    drop_tables(cur, conn)
    print('Creating tables')
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()