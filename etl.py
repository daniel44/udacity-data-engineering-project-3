import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data into the redshift from ythe files stored in S3
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Reads data from the staging tables and loads them into the analytics tables
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function: ETL process that extracts data from S3, transforms and load into redshift
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Loading data from S3')
    load_staging_tables(cur, conn)
    print('Inserting data into redshift')
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()