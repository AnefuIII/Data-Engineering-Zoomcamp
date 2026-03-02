import dlt
import os
from dlt.sources.rest_api import rest_api_source

def load_data():
    # Get absolute path for the current directory
    current_dir = os.path.abspath(".")
    db_path = os.path.join(current_dir, "taxi_data.duckdb")
    
    print(f"Current directory: {current_dir}")
    print(f"Database will be created at: {db_path}")
    
    # API configuration
    config = {
        "client": {"base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/"},
        "resources": [{
            "name": "taxi_trips",
            "endpoint": {
                "path": "data_engineering_zoomcamp_api",
                "paginator": {
                    "type": "offset",
                    "limit": 1000,
                    "offset_param": "offset"
                }
            }
        }]
    }

    # Initialize the source
    source = rest_api_source(config)

    # Initialize the pipeline with explicit path
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="taxi_data"
    )

    print("🚀 PIPELINE STARTED: Fetching and saving data...")
    
    # Run the pipeline
    load_info = pipeline.run(source, table_name="taxi_trips", write_disposition="replace")
    
    print("✅ PIPELINE FINISHED!")
    print(f"Load info: {load_info}")
    
    # Verify the database was created
    if os.path.exists(db_path):
        print(f"✅ Database file created: {db_path}")
        print(f"File size: {os.path.getsize(db_path)} bytes")
        
        # Try to connect and verify data
        try:
            import duckdb
            conn = duckdb.connect(db_path)
            tables = conn.execute("SHOW TABLES").fetchall()
            print(f"Tables in database: {tables}")
            
            if tables:
                count = conn.execute("SELECT COUNT(*) FROM taxi_trips").fetchone()[0]
                print(f"Rows in taxi_trips: {count}")
        except Exception as e:
            print(f"Error verifying database: {e}")
    else:
        print("❌ Database file was NOT created!")

if __name__ == "__main__":
    load_data()



# import dlt
# from dlt.sources.rest_api import rest_api_source

# def load_data():
#     # API configuration
#     config = {
#         "client": {"base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/"},
#         "resources": [{
#             "name": "taxi_trips",
#             "endpoint": {
#                 "path": "data_engineering_zoomcamp_api",
#                 "paginator": {
#                     "type": "offset",
#                     "limit": 1000,
#                     "offset_param": "offset"
#                 }
#             }
#         }]
#     }

#     # Initialize the source
#     source = rest_api_source(config)

#     # Initialize the pipeline
#     pipeline = dlt.pipeline(
#         pipeline_name="taxi_pipeline",
#         destination="duckdb",
#         dataset_name="taxi_data"
#     )

#     print("🚀 PIPELINE STARTED: Fetching and saving data...")
#     print(f"Pipeline will save to: {pipeline.pipelines_dir}")
    
#     # Run the pipeline
#     load_info = pipeline.run(source, table_name="taxi_trips", write_disposition="replace")
    
#     print("✅ PIPELINE FINISHED!")
#     print(f"Load info: {load_info}")
    
#     # Show where the database is
#     import os
#     import glob
#     duckdb_files = glob.glob(os.path.expanduser("~/.dlt/pipelines/**/*.duckdb"), recursive=True)
#     if duckdb_files:
#         print(f"\n📁 Database file location: {duckdb_files[0]}")

# if __name__ == "__main__":
#     load_data()