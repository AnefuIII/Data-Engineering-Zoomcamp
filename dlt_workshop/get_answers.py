import duckdb
import os
import glob

def find_db_file():
    """Find the taxi_data.duckdb file in current directory"""
    current_dir = os.path.abspath(".")
    db_path = os.path.join(current_dir, "taxi_data.duckdb")
    
    if os.path.exists(db_path):
        return db_path
    
    # Also check for any .duckdb files
    duckdb_files = glob.glob("*.duckdb")
    if duckdb_files:
        return duckdb_files[0]
    
    return None


print("🔍 Looking for database file...")
db_file = find_db_file()

if not db_file:
    print("❌ No database file found in current directory!")
    print(f"Current directory: {os.path.abspath('.')}")
    print("\nFiles in current directory:")
    for f in os.listdir('.'):
        print(f"  - {f}")
else:
    print(f"✅ Found database: {db_file}")
    
    try:
        conn = duckdb.connect(db_file)
        
        # Show schemas
        schemas = conn.execute("SHOW SCHEMAS").fetchall()
        print(f"\nSchemas found: {[s[0] for s in schemas]}")
        
        # Check tables inside taxi_data schema
        tables = conn.execute("SHOW TABLES FROM taxi_data").fetchall()
        print(f"Tables found in taxi_data schema: {[t[0] for t in tables]}")
        
        if tables:
            table_name = tables[0][0]
            
            # Get row count
            count = conn.execute(
                f"SELECT COUNT(*) FROM taxi_data.{table_name}"
            ).fetchone()[0]
            
            print(f"\nTotal rows: {count}")
            
            if count > 0:
                print("\n📊 ANSWERS:")
                
                # Q1: Date range
                try:
                    dates = conn.execute(f"""
                        SELECT 
                            MIN(Trip_Pickup_DateTime), 
                            MAX(Trip_Pickup_DateTime)
                        FROM taxi_data.{table_name}
                    """).fetchone()
                    
                    print(f"Q1: {dates[0]} to {dates[1]}")
                except Exception as e:
                    print(f"Q1 Error: {e}")
                
                # Q2: Credit card percentage
                try:
                    cc = conn.execute(f"""
                        SELECT 
                            COUNT(CASE 
                                WHEN UPPER(Payment_Type) LIKE '%CREDIT%' 
                                THEN 1 
                            END) * 100.0 / COUNT(*)
                        FROM taxi_data.{table_name}
                    """).fetchone()
                    
                    print(f"Q2: {cc[0]:.2f}%")
                except Exception as e:
                    print(f"Q2 Error: {e}")
                
                # Q3: Total tips
                try:
                    tips = conn.execute(f"""
                        SELECT SUM(Tip_Amt) 
                        FROM taxi_data.{table_name}
                    """).fetchone()
                    
                    print(f"Q3: ${tips[0]:,.2f}")
                except Exception as e:
                    print(f"Q3 Error: {e}")
                
                # Show sample row
                print("\nSample row:")
                sample = conn.execute(
                    f"SELECT * FROM taxi_data.{table_name} LIMIT 1"
                ).fetchone()
                print(sample)
            
            else:
                print("Table is empty!")
        
        else:
            print("No tables found in taxi_data schema!")
            
    except Exception as e:
        print(f"Error connecting to database: {e}")