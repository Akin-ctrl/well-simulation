import subprocess
import json
import threading
import time
import os
import psycopg2
from pymodbus.server.sync import StartTcpServer
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.constants import Endian

# Config
MODBUS_HOST = os.getenv('MODBUS_HOST', 'modbus')  # Assuming the database and simulator are in the same Docker network
MODBUS_PORT = int(os.getenv('MODBUS_PORT', 5020))
SIMULATOR_SCRIPT = 'wellhead_simulator.py'
DB_HOST = os.getenv('POSTGRES_HOST', 'db')
DB_PORT = int(os.getenv('POSTGRES_PORT', 5432))
DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')

server_context = None
register_map = {}

def build_register_map():
    """Fetches the Modbus mapping from the database."""
    global register_map
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME)
    cursor = conn.cursor()
    
    query = """
    SELECT wh.wellhead_id, pt.code, dpm.modbus_register, pt.data_type
    FROM deviceParameterMapping dpm
    JOIN parameterType pt ON dpm.parameter_type_id = pt.parameter_type_id
    JOIN device d ON dpm.device_id = d.device_id
    JOIN wellHead wh ON d.device_id = wh.device_id
    WHERE dpm.active = TRUE;
    """
    cursor.execute(query)
    mappings = cursor.fetchall()
    
    temp_map = {}
    for row in mappings:
        wellhead_id, param_code, register, data_type = row
        if wellhead_id not in temp_map:
            temp_map[wellhead_id] = {}
        temp_map[wellhead_id][param_code] = {"register": register, "type": data_type}
    
    register_map = temp_map
    print("Successfully built Modbus register map from database.")
    cursor.close()
    conn.close()

def data_updater_thread():
    """Runs simulator and updates Modbus data store based on the register map."""
    global server_context, register_map
    
    print("Starting data updater thread...")
    process = subprocess.Popen(['python', SIMULATOR_SCRIPT], stdout=subprocess.PIPE, text=True)

    while True:
        output = process.stdout.readline()
        if output:
            try:
                wellhead_data_list = json.loads(output.strip())
                
                for data_point in wellhead_data_list:
                    wellhead_id = data_point['wellhead_id']
                    
                    if wellhead_id not in register_map:
                        continue

                    for param_code, value in data_point['parameters'].items():
                        if param_code in register_map[wellhead_id]:
                            mapping_info = register_map[wellhead_id][param_code]
                            register_addr = mapping_info['register']
                            param_type = mapping_info['type']
                            
                            builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=Endian.Little)
                            
                            if param_type == 'float':
                                builder.add_32bit_float(float(value))
                            elif param_type in ['integer', 'boolean']:
                                builder.add_32bit_int(int(value))
                            
                            payload = builder.to_registers()
                            server_context[0x00].setValues(3, register_addr, payload)

            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error processing data from simulator: {e}")
        
        if process.poll() is not None:
            print("Simulator process has terminated.")
            break
        time.sleep(0.1)

def run_modbus_server():
    """Initializes and runs the Modbus TCP server."""
    global server_context
    # Initialize with a large enough block, e.g., 1000 registers
    store = ModbusSlaveContext(hr=ModbusSequentialDataBlock(0, [0] * 1000))
    server_context = ModbusServerContext(slaves=store, single=True)

    print(f"Starting Modbus TCP server on {MODBUS_HOST}:{MODBUS_PORT}...")
    StartTcpServer(context=server_context, address=(MODBUS_HOST, MODBUS_PORT))

if __name__ == "__main__":
    print("Modbus Gateway waiting for database to be ready...")
    time.sleep(10)
    try:
        build_register_map()
        updater = threading.Thread(target=data_updater_thread, daemon=True)
        updater.start()
        run_modbus_server()
    except psycopg2.OperationalError as e:
        print(f"Database connection failed: {e}")