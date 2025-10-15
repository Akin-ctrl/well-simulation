import subprocess
import json
import threading
import time
from pymodbus.server.sync import StartTcpServer
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.constants import Endian

# --- Configuration ---
MODBUS_HOST = 'localhost'
MODBUS_PORT = 5020
# Number of registers to allocate for each wellhead.
# Must be large enough for all parameters. (18 params * 2 registers/param = 36. 50 is safe)
REGISTERS_PER_WELLHEAD = 50 
# Path to the simulator script
SIMULATOR_SCRIPT = 'wellhead_simulator.py'

# Global variable to hold the server context for updating
server_context = None

def load_parameter_config(config_file="parameters.json"):
    """Loads the parameter configuration to maintain order."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Parameter config file '{config_file}' not found.")
        exit()

def data_updater_thread(parameter_config):
    """
    Runs the simulator, reads its output, and updates the Modbus data store.
    """
    global server_context
    param_names = [p['name'] for p in parameter_config]
    param_types = {p['name']: p['type'] for p in parameter_config}

    print("Starting data updater thread...")
    print(f"Launching subprocess: python {SIMULATOR_SCRIPT}")
    
    # Start the wellhead_simulator.py script as a subprocess
    process = subprocess.Popen(['python', SIMULATOR_SCRIPT], stdout=subprocess.PIPE, text=True)

    while True:
        # Read the output from the simulator line by line
        output = process.stdout.readline()
        if output:
            try:
                wellhead_data_list = json.loads(output.strip())
                
                for wellhead_data in wellhead_data_list:
                    wellhead_id_str = wellhead_data['wellhead_id'] # e.g., "WH-001"
                    wellhead_index = int(wellhead_id_str.split('-')[1]) - 1 # 0-based index
                    
                    # Calculate the starting register address for this wellhead
                    base_address = wellhead_index * REGISTERS_PER_WELLHEAD
                    
                    # Use a payload builder to encode data correctly
                    builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=Endian.Little)
                    
                    # Iterate through parameters in the order defined in parameters.json
                    for param_name in param_names:
                        value = wellhead_data['parameters'].get(param_name)
                        param_type = param_types[param_name]

                        if value is None:
                            continue # Skip if parameter is missing

                        # Add values to the builder based on type
                        if param_type in ['float']:
                            builder.add_32bit_float(float(value))
                        elif param_type in ['integer', 'boolean']:
                            # For simplicity, we store all as 32-bit integers.
                            # This keeps the 2-register-per-parameter structure consistent.
                            builder.add_32bit_int(int(value))
                    
                    # Get the payload (a list of 16-bit register values)
                    payload = builder.to_registers()
                    
                    # Set the values in the Modbus data store
                    # The address must be the 0-based register address
                    server_context.setValues(3, base_address, payload) # 3 = Holding Registers

                # Optional: print a confirmation
                # print(f"Updated Modbus registers for {len(wellhead_data_list)} wellheads.")

            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error processing data from simulator: {e}")
        
        # If the subprocess terminates, break the loop
        if process.poll() is not None:
            print("Simulator process has terminated.")
            break
        
        time.sleep(0.1) # Small sleep to prevent busy-waiting

def run_modbus_server():
    """
    Initializes and runs the Modbus TCP server.
    """
    global server_context
    
    # Initialize the data store with all zeros.
    # We create a single block large enough for all potential wellheads.
    # Let's pre-allocate for 20 wellheads for this example.
    num_wellheads_alloc = 20
    store = ModbusSlaveContext(
        hr=ModbusSequentialDataBlock(0, [0] * (num_wellheads_alloc * REGISTERS_PER_WELLHEAD))
    )
    server_context = ModbusServerContext(slaves=store, single=True)

    print(f"Starting Modbus TCP server on {MODBUS_HOST}:{MODBUS_PORT}...")
    # The server runs in the main thread
    StartTcpServer(context=server_context, address=(MODBUS_HOST, MODBUS_PORT))


if __name__ == "__main__":
    # Load the parameter configuration to understand data structure
    parameters = load_parameter_config()

    # Start the data updater in a background thread
    updater = threading.Thread(target=data_updater_thread, args=(parameters,), daemon=True)
    updater.start()

    # The Modbus server runs in the main thread, blocking execution
    run_modbus_server()