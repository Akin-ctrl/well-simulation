
# import json
# import random
# import time
# from datetime import datetime

# class Wellhead:
#     """Represents a single wellhead with its parameters."""

#     def __init__(self, wellhead_id, parameters_config):
#         self.wellhead_id = wellhead_id
#         self.parameters = parameters_config

#     def get_data(self):
#         """Generates randomized data for all parameters."""
#         data = {
#             "timestamp": datetime.utcnow().isoformat(),
#             "wellhead_id": self.wellhead_id,
#             "parameters": {}
#         }
#         for param in self.parameters:
#             name = param["name"]
#             low = param["low"]
#             high = param["high"]
#             param_type = param["type"]

#             if param_type == "float":
#                 # Generate a value with a 10% chance of being outside the thresholds for alarm testing
#                 if random.random() < 0.1:
#                     value = random.uniform(low - (high-low)*0.2, high + (high-low)*0.2)
#                 else:
#                     value = random.uniform(low, high)
#                 data["parameters"][name] = round(value, 2)
#             elif param_type == "integer":
#                 value = random.randint(low, high)
#                 data["parameters"][name] = value
#             elif param_type == "boolean":
#                 value = random.choice([0, 1])
#                 data["parameters"][name] = value
#         return data

# class Simulator:
#     """Manages multiple wellheads and runs the simulation."""

#     def __init__(self, num_wellheads, config_file):
#         self.wellheads = []
#         try:
#             with open(config_file, 'r') as f:
#                 parameters_config = json.load(f)
#         except FileNotFoundError:
#             print(f"Error: Configuration file '{config_file}' not found.")
#             exit()
            
#         for i in range(1, num_wellheads + 1):
#             self.wellheads.append(Wellhead(f"WH-{str(i).zfill(3)}", parameters_config))

#     def run(self, interval_seconds=30):
#         """Runs the simulation, printing data at specified intervals."""
#         print("Starting Wellhead Simulator...")
#         while True:
#             all_data = [wh.get_data() for wh in self.wellheads]
#             # In a real system, this might write to a file, a message queue, or a direct socket.
#             # For this simulation, we print to stdout in a machine-readable format (JSON).
#             print(json.dumps(all_data))
#             time.sleep(interval_seconds)

# if __name__ == "__main__":
#     # Configuration
#     NUMBER_OF_WELLHEADS = 5  # Easily change the number of wellheads to simulate
#     PARAMETERS_CONFIG_FILE = "parameters.json"
#     SIMULATION_INTERVAL = 30  # seconds

#     simulator = Simulator(NUMBER_OF_WELLHEADS, PARAMETERS_CONFIG_FILE)
#     simulator.run(SIMULATION_INTERVAL)

import json
import random
import time
import os
import psycopg2
from datetime import datetime

# Database connection details from environment variables
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'wellhead_data')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'your_password')

def get_simulation_metadata():
    """Fetches wellhead and parameter info from the database."""
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
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