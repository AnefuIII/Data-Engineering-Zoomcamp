import pandas as pd
from sqlalchemy import create_engine
from time import time

def main():
    # Connection string for your Docker Postgres
    # user:password@hostname:port/database
    engine = create_engine('postgresql://root:root@localhost:5433/ny_taxi')
    

    # 1. Load Zones
    print("Loading taxi zones...")
    df_zones = pd.read_csv('taxi_zone_lookup.csv')
    df_zones.to_sql(name='taxi_zone_lookup', con=engine, if_exists='replace')

    # 2. Load Green Taxi Data (Parquet)
    print("Loading green taxi data...")
    t_start = time()
    df_iter = pd.read_parquet('green_tripdata_2025-11.parquet')
    
    # In a real scenario, we'd chunk this, but for this size, 
    # we can push it in one go or use head(0) to create schema first.
    df_iter.to_sql(name='green_tripdata', con=engine, if_exists='replace')
    t_end = time()

    print(f'Finished ingesting data, took {t_end - t_start:.3f} seconds')

if __name__ == '__main__':
    main()