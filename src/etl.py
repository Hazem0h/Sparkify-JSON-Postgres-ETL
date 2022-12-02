import extract_transform
import load_pg
import create_tables

if __name__ == "__main__":
    create_tables.main()
    print("Database Tables Reset")
    print("*"*40)
    print("Extracting and Transforming...")
    extract_transform.main()
    print("*"*40)
    print("Loading into the Postgres Database...")
    load_pg.main()