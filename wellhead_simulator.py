
import json
import random
import time
from datetime import datetime

class Wellhead:
    """Represents a single wellhead with its parameters."""

    def __init__(self, wellhead_id, parameters_config):
        self.wellhead_id = wellhead_id
        self.parameters = parameters_config

    def get_data(self):
        """Generates randomized data for all parameters."""
        data = {
            "timestamp": datetime.utcnow().isoformat(),
            "wellhead_id": self.wellhead_id,
            "parameters": {}
        }
        for param in self.parameters:
            name = param["name"]
            low = param["low"]
            high = param["high"]
            param_type = param["type"]

            if param_type == "float":
                # Generate a value with a 10% chance of being outside the thresholds for alarm testing
                if random.random() < 0.1:
                    value = random.uniform(low - (high-low)*0.2, high + (high-low)*0.2)
                else:
                    value = random.uniform(low, high)
                data["parameters"][name] = round(value, 2)
            elif param_type == "integer":
                value = random.randint(low, high)
                data["parameters"][name] = value
            elif param_type == "boolean":
                value = random.choice([0, 1])
                data["parameters"][name] = value
        return data

class Simulator:
    """Manages multiple wellheads and runs the simulation."""

    def __init__(self, num_wellheads, config_file):
        self.wellheads = []
        try:
            with open(config_file, 'r') as f:
                parameters_config = json.load(f)
        except FileNotFoundError:
            print(f"Error: Configuration file '{config_file}' not found.")
            exit()
            
        for i in range(1, num_wellheads + 1):
            self.wellheads.append(Wellhead(f"WH-{str(i).zfill(3)}", parameters_config))

    def run(self, interval_seconds=30):
        """Runs the simulation, printing data at specified intervals."""
        print("Starting Wellhead Simulator...")
        while True:
            all_data = [wh.get_data() for wh in self.wellheads]
            # In a real system, this might write to a file, a message queue, or a direct socket.
            # For this simulation, we print to stdout in a machine-readable format (JSON).
            print(json.dumps(all_data))
            time.sleep(interval_seconds)

if __name__ == "__main__":
    # Configuration
    NUMBER_OF_WELLHEADS = 5  # Easily change the number of wellheads to simulate
    PARAMETERS_CONFIG_FILE = "parameters.json"
    SIMULATION_INTERVAL = 30  # seconds

    simulator = Simulator(NUMBER_OF_WELLHEADS, PARAMETERS_CONFIG_FILE)
    simulator.run(SIMULATION_INTERVAL)