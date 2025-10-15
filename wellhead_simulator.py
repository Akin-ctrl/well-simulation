import json
import random
import time
from datetime import datetime

# --- Configuration ---
WELLHEADS_CONFIG = {
    "WH-001": {
        "location": "Field A, Platform 1",
        "type": "Oil Producer"
    },
    "WH-002": {
        "location": "Field A, Platform 2",
        "type": "Gas Injector"
    },
    "WH-003": {
        "location": "Field B, Subsea",
        "type": "Oil Producer"
    }
}

PARAMETERS_CONFIG = [
    {"name": "Tubing Pressure", "low": 500, "high": 3000, "type": "float"},
    {"name": "Casing Pressure", "low": 200, "high": 2000, "type": "float"},
    {"name": "Annulus Pressure", "low": 50, "high": 500, "type": "float"},
    {"name": "Wellhead Temperature", "low": 60, "high": 250, "type": "float"},
    {"name": "Choke Valve Position", "low": 0, "high": 100, "type": "integer"},
    {"name": "Flow Rate", "low": 100, "high": 5000, "type": "float"},
    {"name": "Gas Oil Ratio", "low": 500, "high": 3000, "type": "float"},
    {"name": "Water Cut", "low": 0, "high": 80, "type": "float"},
    {"name": "Sand Detector", "low": 0, "high": 100, "type": "float"},
    {"name": "Corrosion Rate", "low": 0, "high": 10, "type": "float"},
    {"name": "H2S Level", "low": 0, "high": 50, "type": "float"},
    {"name": "CO2 Level", "low": 0, "high": 5, "type": "float"},
    {"name": "Vibration", "low": 0, "high": 5, "type": "float"},
    {"name": "Master Valve Status", "low": 0, "high": 1, "type": "boolean"},
    {"name": "Wing Valve Status", "low": 0, "high": 1, "type": "boolean"},
    {"name": "Swab Valve Status", "low": 0, "high": 1, "type": "boolean"},
    {"name": "Emergency Shutdown", "low": 0, "high": 1, "type": "boolean"},
    {"name": "Pump Status", "low": 0, "high": 1, "type": "boolean"}
]

OUTPUT_FILE = "wellhead_data.json"
SIMULATION_INTERVAL_SECONDS = 30

def generate_wellhead_data(wellhead_id, wellhead_info):
    """Generates a single data point for a given wellhead."""
    data = {
        "wellhead_id": wellhead_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "location": wellhead_info["location"],
        "type": wellhead_info["type"],
        "parameters": {}
    }
    for param, config in PARAMETERS_CONFIG.items():
        if param.endswith(("_status", "_control", "_mode")): # Digital/Integer values
            value = random.randint(config["min"], config["max"])
        else: # Analog/Float values
            value = round(random.uniform(config["min"], config["max"]), 2)
        data["parameters"][param] = value
    return data

def main():
    """Main simulation loop."""
    print("--- Wellhead Simulator Started ---")
    print(f"Simulating {len(WELLHEADS_CONFIG)} wellheads.")
    print(f"Data will be written to {OUTPUT_FILE} every {SIMULATION_INTERVAL_SECONDS} seconds.")

    while True:
        try:
            all_wellheads_data = []
            for wh_id, wh_info in WELLHEADS_CONFIG.items():
                data_point = generate_wellhead_data(wh_id, wh_info)
                all_wellheads_data.append(data_point)
                print(f"Generated data for {wh_id} at {data_point['timestamp']}")

            # Write the current state to a file
            with open(OUTPUT_FILE, 'w') as f:
                json.dump(all_wellheads_data, f, indent=4)

            time.sleep(SIMULATION_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\n--- Wellhead Simulator Stopped ---")
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(SIMULATION_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
