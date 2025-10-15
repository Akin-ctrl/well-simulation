import time
import json
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian

# --- Configuration ---
# Modbus Server Configuration
MODBUS_HOST = 'localhost'
MODBUS_PORT = 5020
REGISTERS_PER_WELLHEAD = 50 # Must match the gateway script

# Database Configuration
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'wellhead_data'
DB_USER = 'postgres' # Change to your username
DB_PASSWORD = 'your_password' # Change to your password

# Simulation Configuration
NUMBER_OF_WELLHEADS = 5 # Must match the simulator script
POLL_INTERVAL = 30 # seconds

def load_parameter_config(config_file="parameters.json"):
    """Loads the parameter configuration to ensure consistent data decoding."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Parameter config file '{config_file}' not found.")
        exit()

def main():
    """Main function to run the polling and ingestion loop."""
    print("Starting Database Ingestion Service...")
    
    parameter_config = load_parameter_config()
    
    # Create a Modbus client
    client = ModbusTcpClient(MODBUS_HOST, port=MODBUS_PORT)
    
    # Construct the SQL INSERT statement dynamically
    # This makes it robust to parameter changes
    column_names = [p['name'].replace(' ', '_').lower() for p in parameter_config]
    sql_columns = ", ".join(column_names)
    sql_placeholders = ", ".join(["%s"] * len(column_names))
    insert_sql = f"""
        INSERT INTO wellhead_readings (time, wellhead_id, {sql_columns})
        VALUES (%s, %s, {sql_placeholders})
    """

    while True:
        try:
            # Connect to the database
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            cursor = conn.cursor()
            print("Successfully connected to the database.")

            client.connect()
            print("Successfully connected to the Modbus gateway.")

            while True:
                start_time = time.time()
                
                records_to_insert = []
                current_timestamp = datetime.utcnow()

                for i in range(NUMBER_OF_WELLHEADS):
                    wellhead_index = i
                    wellhead_id = f"WH-{str(i+1).zfill(3)}"
                    base_address = wellhead_index * REGISTERS_PER_WELLHEAD
                    
                    # Read the block of holding registers for the current wellhead
                    result = client.read_holding_registers(base_address, len(parameter_config) * 2, slave=1)
                    
                    if result.isError():
                        print(f"Modbus Error reading {wellhead_id}: {result}")
                        continue

                    # Decode the registers back into data
                    decoder = BinaryPayloadDecoder.fromRegisters(result.registers, byteorder=Endian.Big, wordorder=Endian.Little)
                    
                    decoded_data = {}
                    for param in parameter_config:
                        param_name = param['name']
                        param_type = param['type']
                        sql_col_name = param_name.replace(' ', '_').lower()

                        if param_type == 'float':
                            decoded_data[sql_col_name] = decoder.decode_32bit_float()
                        elif param_type in ['integer', 'boolean']:
                            val = decoder.decode_32bit_int()
                            if param_type == 'boolean':
                                decoded_data[sql_col_name] = bool(val)
                            else:
                                decoded_data[sql_col_name] = val
                    
                    # Prepare the record for batch insertion
                    record_values = [decoded_data[col] for col in column_names]
                    records_to_insert.append((current_timestamp, wellhead_id) + tuple(record_values))

                # Insert all records for this poll cycle in a single transaction
                if records_to_insert:
                    execute_batch(cursor, insert_sql, records_to_insert)
                    conn.commit()
                    print(f"[{datetime.now()}] Inserted {len(records_to_insert)} records into the database.")

                # Wait for the next poll cycle
                time_to_wait = POLL_INTERVAL - (time.time() - start_time)
                if time_to_wait > 0:
                    time.sleep(time_to_wait)

        except Exception as e:
            print(f"An error occurred: {e}. Reconnecting in 10 seconds...")
            if 'client' in locals() and client.is_socket_open():
                client.close()
            if 'conn' in locals() and not conn.closed:
                conn.close()
            time.sleep(10)

if __name__ == "__main__":
    main()