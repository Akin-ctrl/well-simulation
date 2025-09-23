import pandas as pd
from datetime import datetime

signals = [
    # --- Measured parameters ---
    ("P01", "THP", "Tubing Head Pressure", "psi", "float", 1, 0, 5000, "Pressure at tubing head", "pressure"),
    ("P02", "FLP", "Flowline Pressure", "psi", "float", 1, 0, 5000, "Pressure in flowline", "pressure"),
    ("P03", "CSP", "Casing Pressure", "psi", "float", 1, 0, 5000, "Casing annulus pressure", "pressure"),
    ("P04", "FLT", "Flowline Temperature", "°C", "float", 1, -40, 200, "Temperature in flowline", "temperature"),
    ("P05", "THT", "Tubing Head Temperature", "°C", "float", 1, -40, 200, "Temperature at tubing head", "temperature"),
    ("P06", "ANB", "Annulus B", "psi", "float", 1, 0, 5000, "Annulus B pressure", "pressure"),
    ("P07", "DHP", "Downhole Pressure", "psi", "float", 1, 0, 10000, "Reservoir downhole pressure", "pressure"),
    ("P08", "DHT", "Downhole Temperature", "°C", "float", 1, 0, 250, "Reservoir downhole temperature", "temperature"),
    ("P09", "SDV_ST", "SDV Status", "boolean", "bool", 0, None, None, "Shutdown valve status", "status"),
    ("P10", "SSV_ST", "SSV Status", "boolean", "bool", 0, None, None, "Surface safety valve status", "status"),
    ("P11", "VIB", "Vibration Monitoring", "g", "float", 2, 0, 50, "Wellhead vibration monitoring", "vibration"),
    ("P12", "AMB_T", "Ambient Temperature", "°C", "float", 1, -50, 80, "External environment temperature", "temperature"),
    ("P13", "WCUT", "Water Cut", "%", "float", 1, 0, 100, "Percentage of water in produced fluids", "composition"),

    # --- Control Signals ---
    ("P14", "SSV_CTRL", "SSV Control", "boolean", "bool", 0, None, None, "Command to operate SSV", "control"),
    ("P15", "SDV_CTRL", "SDV Control", "boolean", "bool", 0, None, None, "Command to operate SDV", "control"),
    ("P16", "WING_CTRL", "Wing Valve Control", "boolean", "bool", 0, None, None, "Command to operate wing valve", "control"),
    ("P17", "CHOKE_CTRL", "Choke Valve Control", "boolean", "bool", 0, None, None, "Command to operate choke valve", "control"),
]

# Build dataframe
parameter_type = pd.DataFrame(signals, columns=[
    "parameter_id", "code", "display_name", "canonical_unit",
    "data_type", "precision", "normal_min", "normal_max",
    "description", "category"
])

# Add timestamps
now = datetime.utcnow()
parameter_type["created_at"] = now
parameter_type["updated_at"] = now

print(parameter_type.head(10))
