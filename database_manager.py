# database_manager.py
import logging
import os
import json
import psycopg2
from psycopg2 import pool
from datetime import datetime
from config_reader import ConfigReader

CONFIG_DIR = 'config'
DATABASE_CONFIG_FILE = 'database_config.ini'
BUFR_KEYS_FILE = 'BUFRKeys.ini'

class DatabaseManager:
    def __init__(self):
        # Initialize the database connection pool
        db_config = ConfigReader.read_config(os.path.join(CONFIG_DIR, DATABASE_CONFIG_FILE), 'Database')
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            1,
            10000,
            **db_config
        )

    def insert_message_data(self, data_list):
        # Insert message data into the 'message' table
        query = """
            INSERT INTO message (topic, publication_timestamp, data_id, canonical_url, wigos_station_identifier)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
        """
        connection = self.connection_pool.getconn()
        try:
            with connection, connection.cursor() as cursor:
                for data in data_list:
                    cursor.execute(query, (data['topic'], data['publication_timestamp'], data['data_id'], data['canonical_url'], data['wigos_station_identifier']))
                    message_id = cursor.fetchone()[0]
                    return message_id
        finally:
            self.connection_pool.putconn(connection)

    def insert_bufr_data(self, message_id, decoded_data):
        # Insert BUFR data into the 'bufr' table
        if message_id is None:
            raise ValueError("message_id cannot be null.")

        keys_config = ConfigReader.read_config(os.path.join(CONFIG_DIR, BUFR_KEYS_FILE), 'BUFRKeys')
        required_columns = keys_config.get('required_columns', '').split(',')
        additional_columns = keys_config.get('additional_columns', '').split(',')
        keys = required_columns

        # Create the list of columns for the INSERT query
        columns = ", ".join(keys + ["raw_data"])

        # Create the list of placeholders for the values
        values_placeholders = ", ".join(["%s"] * len(keys + ["raw_data"]))

        # Build the INSERT query with dynamic columns
        query = f"""
            INSERT INTO bufr (
                message_id, {columns}
            ) VALUES (
                %s, {values_placeholders}
            );
        """

        connection = self.connection_pool.getconn()

        try:
            with connection, connection.cursor() as cursor:
                for message_data in decoded_data['messages']:
                    # Reset the values list for each message
                    values = [message_id]

                    for key in keys:
                        value = message_data.get(key.strip(), None)
                        values.append(value)

                    # Add the 'raw_data' column with all data in JSON format
                    keys = required_columns + additional_columns
                    raw_data = {k: message_data.get(k.strip(), None) for k in keys}
                    values.append(json.dumps(raw_data))

                    sql_query = cursor.mogrify(query, values)
                    cursor.execute(sql_query)

        finally:
            self.connection_pool.putconn(connection)
