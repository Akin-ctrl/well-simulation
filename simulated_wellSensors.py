import random
from datetime import datetime

# Step 1: Parameters (dynamic, can add more easily)
parameters = [
    "Tubing Head Pressure",
    "Flowline Pressure",
    "Casing Pressure",
    "Flowline Temperature",
    "Tubing Head Temperature",
    "Annulus B",
    "Downhole Pressure",
    "Downhole Temperature",
    "SDV Status",
    "SSV Status",
    "Vibration Monitoring",
    "Ambient Temperature",
    "Water Cut",
    "SSV Control",
    "SDV Control",
    "Wing Valve Control",
    "Choke Valve Control"
]

# Step 1a: Simulated wells
num_wells = 5  # can increase easily
wells = {f"Well_{i+1}": {param: 0 for param in parameters} for i in range(num_wells)}

# Function to simulate a sensor reading
def simulate_sensor_reading(param_name):
    if "Pressure" in param_name:
        return round(random.uniform(50, 300), 2)  # psi
    elif "Temperature" in param_name:
        return round(random.uniform(10, 120), 2)  # °C
    elif "Status" in param_name or "Control" in param_name:
        return random.choice([0, 1])  # On/Off
    elif "Vibration" in param_name:
        return round(random.uniform(0, 5), 2)  # mm/s
    elif "Water Cut" in param_name:
        return round(random.uniform(0, 100), 2)  # %
    else:
        return round(random.random(), 2)

# Step 1b: Update wells dynamically
for well_name, sensors in wells.items():
    for param in sensors:
        sensors[param] = simulate_sensor_reading(param)

# Example print
for well, data in wells.items():
    print(well, data)
