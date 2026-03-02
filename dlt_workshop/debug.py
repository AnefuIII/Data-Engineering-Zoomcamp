# First, remove the old debug.py
rm debug.py

# Now create the new clean version
cat > debug.py << 'EOF'
import dlt
import os
import json

# Load the existing pipeline
pipeline = dlt.pipeline(pipeline_name="taxi_pipeline")

print(f"Pipeline name: {pipeline.pipeline_name}")
print(f"Pipeline directory: {pipeline.pipelines_dir}")
print(f"Destination type: {pipeline.destination}")
print(f"Dataset name: {pipeline.dataset_name}")

# Check what's in the pipeline state
print("\nPipeline state location:")
state_path = os.path.join(pipeline.pipelines_dir, pipeline.pipeline_name, "state.json")
if os.path.exists(state_path):
    with open(state_path) as f:
        state = json.load(f)
    print(f"State keys: {list(state.keys())}")
    if 'destination' in state:
        print(f"Destination from state: {state['destination']}")
    if 'dataset_name' in state:
        print(f"Dataset from state: {state['dataset_name']}")
else:
    print(f"State file not found at: {state_path}")

# Search for any database files in the pipeline directory
print("\nSearching for database files:")
pipeline_dir = os.path.join(pipeline.pipelines_dir, pipeline.pipeline_name)
for root, dirs, files in os.walk(pipeline_dir):
    for file in files:
        if file.endswith(('.db', '.duckdb', '.sqlite')):
            file_path = os.path.join(root, file)
            print(f"Found: {file_path}")
            print(f"Size: {os.path.getsize(file_path)} bytes")

# Check the new_jobs directory for data files
print("\nChecking new_jobs directories:")
normalize_dir = os.path.join(pipeline_dir, "normalize")
if os.path.exists(normalize_dir):
    for root, dirs, files in os.walk(normalize_dir):
        if "new_jobs" in dirs:
            new_jobs_path = os.path.join(root, "new_jobs")
            job_files = os.listdir(new_jobs_path)
            if job_files:
                print(f"\nFound {len(job_files)} files in: {new_jobs_path}")
                for job_file in job_files[:5]:  # Show first 5
                    file_path = os.path.join(new_jobs_path, job_file)
                    size = os.path.getsize(file_path)
                    print(f"  - {job_file} ({size} bytes)")
                    
                    # Peek at the first few lines of the first file
                    if job_file == job_files[0]:
                        try:
                            with open(file_path, 'r') as f:
                                content = f.read()[:500]  # First 500 chars
                                print(f"    Content preview: {content[:200]}...")
                        except:
                            print(f"    Could not read file content")
else:
    print(f"Normalize directory not found: {normalize_dir}")

# Also check if there's any data in the load directory
print("\nChecking load directory:")
load_dir = os.path.join(pipeline_dir, "load")
if os.path.exists(load_dir):
    for root, dirs, files in os.walk(load_dir):
        for file in files:
            if file.endswith('.jsonl') or file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                size = os.path.getsize(file_path)
                print(f"Found data file: {file_path} ({size} bytes)")
EOF