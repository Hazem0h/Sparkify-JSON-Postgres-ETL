import psycopg2
import json
from sql_queries import create_table_queries, drop_table_queries


    

def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    with open("../config.json") as f:
        config = json.load(f)
    
    # connect to default database
    #conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn = psycopg2.connect(host = config["host"],
                            dbname = config["defaultdb"],
                            user = config["username"],
                            password = config["password"])
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute(f"DROP DATABASE IF EXISTS {config['dbname']}")
    cur.execute(f"CREATE DATABASE {config['dbname']} WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect(host = config["host"],
                            dbname = config["dbname"],
                            user = config["username"],
                            password = config["password"])
    cur = conn.cursor()
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    cur.close()
    conn.close()
    


if __name__ == "__main__":
    main()