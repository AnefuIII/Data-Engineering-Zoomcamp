import duckdb

conn = duckdb.connect("taxi_pipeline.duckdb")

# This will show us every table and which schema it belongs to
print("--- All Tables in Database ---")
print(conn.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall())

# This will show us the columns in the taxi table (replace 'taxi_trips' if needed)
print("\n--- Columns in the table ---")
try:
    # dlt usually puts data in a schema named after your dataset_name
    # Let's try to find the one that isn't 'main'
    res = conn.execute("SELECT table_schema FROM information_schema.tables WHERE table_name = 'taxi_trips'").fetchone()
    if res:
        schema = res[0]
        print(conn.execute(f"PRAGMA table_info('{schema}.taxi_trips')").fetchall())
    else:
        print("Table 'taxi_trips' not found. Check the list above.")
except Exception as e:
    print(f"Error: {e}")