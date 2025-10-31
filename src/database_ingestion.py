import time
import os
import psycopg2
from datetime import datetime, timezone
from psycopg2.extras import execute_batch
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian

# Config from environment variables
MODBUS_HOST = os.getenv('MODBUS_HOST', 'modbus')
MODBUS_PORT = os.getenv('MODBUS_PORT', 5020)
DB_HOST = os.getenv('POSTGRES_HOST', 'db')
DB_PORT = os.getenv('POSTGRES_PORT', 5432)
DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POLL_INTERVAL = 5  # seconds

def get_ingestion_metadata():
    """Fetches metadata needed for polling and inserting."""
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME)
    cursor = conn.cursor()
    
    query = """
    SELECT dpm.mapping_id, wh.wellhead_id, pt.parameter_type_id, dpm.modbus_register, pt.data_type
    FROM deviceParameterMapping dpm
    JOIN parameterType pt ON dpm.parameter_type_id = pt.parameter_type_id
    JOIN device d ON dpm.device_id = d.device_id
    JOIN wellHead wh ON d.device_id = wh.device_id
    WHERE dpm.active = TRUE ORDER BY dpm.modbus_register;
    """
    cursor.execute(query)
    metadata = cursor.fetchall()
    cursor.close()
    conn.close()
    
    # Convert list of tuples to a more usable list of dicts
    ingestion_map = [
        {"mapping_id": r[0], "wellhead_id": r[1], "param_type_id": r[2], "register": r[3], "type": r[4]}
        for r in metadata
    ]
    return ingestion_map

def main():
    print("Starting Database Ingestion Service...")
    print("Waiting for dependent services to start...")
    time.sleep(15) # Wait for DB and Modbus server to be fully up

    try:
        ingestion_map = get_ingestion_metadata()
        if not ingestion_map:
            print("Error: No ingestion metadata found in database.")
            return
        print(f"Loaded {len(ingestion_map)} parameter mappings for ingestion.")
    except psycopg2.OperationalError as e:
        print(f"Database connection failed: {e}")
        return

    client = ModbusTcpClient(MODBUS_HOST, port=MODBUS_PORT)
    
    insert_sql = """
        INSERT INTO parameterReading (timestamp_utc, wellhead_id, parameter_type_id, mapping_id, raw_value)
        VALUES (%s, %s, %s, %s, %s)
    """

    while True:
        try:
            conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
            cursor = conn.cursor()
            client.connect()

            while True:
                start_time = time.time()
                records_to_insert = []
                current_timestamp = datetime.now(timezone.utc)

                for item in ingestion_map:
                    # Each parameter is 2 registers (32-bit)
                    result = client.read_holding_registers(item['register'], 2, slave=1)
                    
                    if not result.isError():
                        decoder = BinaryPayloadDecoder.fromRegisters(result.registers, byteorder=Endian.Big, wordorder=Endian.Little)
                        value = None
                        if item['type'] == 'float':
                            value = decoder.decode_32bit_float()
                        elif item['type'] in ['integer', 'boolean']:
                            value = float(decoder.decode_32bit_int())
                        
                        if value is not None:
                            records_to_insert.append((
                                current_timestamp,
                                item['wellhead_id'],
                                item['param_type_id'],
                                item['mapping_id'],
                                value
                            ))
                
                if records_to_insert:
                    print(f"[{datetime.now(timezone.utc)}] {len(records_to_insert)} records ready for insert.")

                    for rec in records_to_insert:
                        if rec[0] is None:
                            raise ValueError("Time stamp is none before insert. Check the time generation")
                        if rec[0].tzinfo is None:
                            raise ValueError(f"Naive datetime detected: {rec[0]}. Use datetime.now(timezone.utc).")

                    execute_batch(cursor, insert_sql, records_to_insert)
                    conn.commit()
                    print(f"[{datetime.now(timezone.utc)}] Inserted {len(records_to_insert)} records.")

                time_to_wait = POLL_INTERVAL - (time.time() - start_time)
                if time_to_wait > 0:
                    time.sleep(time_to_wait)

        except Exception as e:
            print(f"An error occurred: {e}. Reconnecting in 10 seconds...")
            if client.is_socket_open(): client.close()
            if 'conn' in locals() and not conn.closed: conn.close()
            time.sleep(10)

if __name__ == "__main__":
    main()