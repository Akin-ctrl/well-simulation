import json
import random
import time
import os
import psycopg2
from datetime import datetime

# Database connection details from environment variables
DB_HOST = os.getenv('POSTGRES_HOST', 'db')
DB_PORT = os.getenv('POSTGRES_PORT', 5432)
DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')

def get_simulation_metadata():
    """Fetches wellhead and parameter info from the database."""
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME)
    cursor = conn.cursor()
    
    # Fetch all active wellheads and their associated parameter types
    query = """
    SELECT wh.wellhead_id, pt.code, pt.normal_min, pt.normal_max, pt.data_type
    FROM wellHead wh
    JOIN device d ON wh.device_id = d.device_id
    JOIN deviceParameterMapping dpm ON d.device_id = dpm.device_id
    JOIN parameterType pt ON dpm.parameter_type_id = pt.parameter_type_id
    WHERE wh.status = 'active' AND dpm.active = TRUE;
    """
    cursor.execute(query)
    metadata = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # Structure the metadata for easy use
    sim_config = {}
    for row in metadata:
        wellhead_id, param_code, min_val, max_val, data_type = row
        if wellhead_id not in sim_config:
            sim_config[wellhead_id] = []
        sim_config[wellhead_id].append({
            "code": param_code,
            "min": min_val,
            "max": max_val,
            "type": data_type
        })
    return sim_config

def run_simulation(config, interval_seconds=30):
    """Runs the simulation based on the provided config."""
    print("Starting Wellhead Simulator with metadata from database...")
    while True:
        all_data = []
        for wellhead_id, params in config.items():
            data_point = {
                "timestamp": datetime.utcnow().isoformat(),
                "wellhead_id": wellhead_id,
                "parameters": {}
            }
            for param in params:
                # Generate a value with a 10% chance of being outside thresholds
                if random.random() < 0.1:
                    value = random.uniform(param["min"] * 0.8, param["max"] * 1.2)
                else:
                    value = random.uniform(param["min"], param["max"])
                
                if param["type"] == 'float':
                    data_point["parameters"][param["code"]] = round(value, 2)
                elif param["type"] == 'boolean':
                    data_point["parameters"][param["code"]] = random.choice([0, 1])
                else: # integer
                    data_point["parameters"][param["code"]] = int(value)
            all_data.append(data_point)
        
        # Print as a single JSON line 
        print(json.dumps(all_data))
        time.sleep(interval_seconds)

if __name__ == "__main__":
    print("Simulator waiting for database to be ready...")
    time.sleep(10) # Simple delay to wait for DB initialization
    
    try:
        simulation_config = get_simulation_metadata()
        if not simulation_config:
            print("Error: No simulation metadata found in the database. Is it seeded?")
        else:
            run_simulation(simulation_config)
    except psycopg2.OperationalError as e:
        print(f"Database connection failed: {e}")
        print("Please ensure the database is running and accessible.")