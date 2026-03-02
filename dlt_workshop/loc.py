import duckdb
import os
import glob

print("🔍 Comprehensive database search...")

# Search all possible locations
search_paths = [
    '.',
    './.dlt',
    os.path.expanduser('~/.dlt'),
    os.path.expanduser('~/.dlt/pipelines'),
    '/tmp',
    os.getcwd()
]

found_files = []
for path in search_paths:
    if os.path.exists(path):
        print(f"\nSearching in: {path}")
        for file in glob.glob(os.path.join(path, "**/*.duckdb"), recursive=True):
            print(f"  Found: {file}")
            found_files.append(file)
        
        # Also check for any .db files
        for file in glob.glob(os.path.join(path, "**/*.db"), recursive=True):
            print(f"  Found (db): {file}")
            found_files.append(file)

if found_files:
    print("\n📊 Checking each database file:")
    for db_file in found_files:
        print(f"\n--- {db_file} ---")
        try:
            conn = duckdb.connect(db_file)
            tables = conn.execute("SHOW TABLES").fetchall()
            if tables:
                print(f"Tables: {[t[0] for t in tables]}")
                for table in tables:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
                    print(f"  {table[0]}: {count} rows")
            else:
                print("No tables found")
        except Exception as e:
            print(f"Error: {e}")
else:
    print("\n❌ No database files found!")
    
    # Check if dlt is configured correctly
    print("\nChecking dlt configuration...")
    try:
        import dlt
        print(f"dlt version: {dlt.__version__}")
        
        # Create a test pipeline
        test_pipeline = dlt.pipeline(
            pipeline_name="test_pipeline",
            destination="duckdb",
            dataset_name="test_data"
        )
        print(f"Test pipeline created")
        print(f"Pipelines dir: {test_pipeline.pipelines_dir}")
        
        # Check if directory exists
        if os.path.exists(test_pipeline.pipelines_dir):
            print(f"Directory exists: {test_pipeline.pipelines_dir}")
            print(f"Contents: {os.listdir(test_pipeline.pipelines_dir)}")
        else:
            print(f"Directory does not exist: {test_pipeline.pipelines_dir}")
            
    except Exception as e:
        print(f"Error checking dlt: {e}")