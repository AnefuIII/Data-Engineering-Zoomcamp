import dlt
import requests
from pathlib import Path

@dlt.resource(write_disposition="replace") # Use 'replace' for your first clean run
def nyc_taxi_data():
    url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
    page = 1
    
    while True:
        # Construct the URL with pagination parameters
        # Most APIs use ?page=X or &offset=X
        params = {'page': page}
        response = requests.get(url, params=params)
        response.raise_for_status() # Stop if the API is down
        
        data = response.json()
        
        # STOP CONDITION: If the page is empty, we are done
        if not data:
            break
            
        yield data
        
        print(f"Fetched page {page}...")
        page += 1

if __name__ == "__main__":
    # Setup paths in WSL home
    working_dir = Path(__file__).parent
    db_path = working_dir / "data" / "nyc_taxi_api.duckdb"

    # Define the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="nyc_taxi_pipeline",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="taxi_api_data",
    )

    # Run the pipeline
    load_info = pipeline.run(nyc_taxi_data(), table_name="rides")

    print(load_info)

    
############3 working for csv

# from pathlib import Path 
# import dlt
# import os
# import pandas as pd

# @dlt.resource(write_disposition="append")
# def load_resource(file_path: str, **kwargs):
#     # It's safer to check if the file exists first
#     df = pd.read_csv(file_path)
#     yield df

# if __name__ == "__main__":
#     # Get the directory where THIS script is located
#     working_dir = Path(__file__).parent
    
#     # Define paths using pathlib (cleaner than os.chdir)
#     path = working_dir / "data" / "data.csv"
#     db_path = working_dir / "data" / "yellow_taxi_database.duckdb"

#     data = load_resource(path)

#     pipeline = dlt.pipeline(
#         pipeline_name="my_pipeline",
#         destination=dlt.destinations.duckdb(str(db_path)), # dlt likes strings for paths
#         dataset_name="yellow_taxi_data",
#     )

#     load_info = pipeline.run(data, table_name="yellow_taxi_table")

#     print(load_info)