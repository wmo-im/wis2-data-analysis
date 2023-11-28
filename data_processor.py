import paho.mqtt.client as mqtt
import logging
import time
import ssl
import configparser
import json
import re
import psycopg2
import traceback
import os
import requests
from psycopg2 import pool
from datetime import datetime
from eccodes import *

class ConfigReader:
    @staticmethod
    def read_config(file_path, section):
        config = configparser.ConfigParser()
        config.read(file_path)
        return config[section]

class BufrFileManager:
    def __init__(self, db_manager, download_directory):
        self.db_manager = db_manager
        self.download_directory = download_directory

    def decode_bufr_file(self, file_path):
        keys = ConfigReader.read_config('config/keys.ini', 'BUFRKeys')
        keys = keys.get('keys', '').split(',')
        decoded_data = {'message_count': 0, 'messages': []}

        with open(file_path, 'rb') as bufr_file:
            while True:
                bufr = codes_bufr_new_from_file(bufr_file)
                if bufr is None:
                    break

                codes_set(bufr, "unpack", 1)

                message_data = {'message_number': decoded_data['message_count']}

                for key in keys:
                    try:
                        message_data[key] = codes_get(bufr, key)
                    except CodesInternalError as err:
                        logging.error('Error with key="%s" : %s' % (key, err.msg))

                decoded_data['messages'].append(message_data)
                decoded_data['message_count'] += 1

                codes_release(bufr)

        return decoded_data

    def download_file(self, data_list, constructed_link, message_id):
        data_entry = data_list[-1]
        download_url = data_entry['canonical_url']
        file_name = os.path.basename(download_url)
        local_path = os.path.join(os.path.join(self.download_directory, constructed_link), file_name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        try:
            logging.debug(f"Attemping to download from URL: {download_url}")
            response = requests.get(download_url)
            response.raise_for_status()

            with open(local_path, 'wb') as file:
                file.write(response.content)

            logging.info(f"File downloaded successfully: {local_path}")

            # Decode the downloaded file
            decoded_data = self.decode_bufr_file(local_path)

            # <! -- 3 -- !>
            bufr_id = self.db_manager.insert_bufr_data(message_id, decoded_data)

            print(decoded_data)

        except Exception as e:
            logging.error(f"Error during file download or decoding: {e}")
            logging.debug(traceback.format_exc())

class DatabaseManager:
    def __init__(self, db_config, download_directory):
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            1,
            10000,
            **db_config
        )
        self.download_manager = BufrFileManager(self, download_directory)

    def insert_message_data(self, data_list):
        query = """
            INSERT INTO message (topic, publication_timestamp, data_id, canonical_url, wigos_station_identifier)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
        """
        connection = self.connection_pool.getconn()
        try:
            with connection:
                with connection.cursor() as cursor:
                    for data in data_list:
                        cursor.execute(query, (data['topic'], data['publication_timestamp'], data['data_id'], data['canonical_url'], data['wigos_station_identifier']))
                        message_id = cursor.fetchone()[0]
                        return message_id
        finally:
            self.connection_pool.putconn(connection)

    def insert_bufr_data(self, message_id, decoded_data):
        # Check that message_id is not null
        if message_id is None:
            raise ValueError("message_id cannot be null.")

        query = """
            INSERT INTO bufr (
                message_id, 
                year, 
                month, 
                day, 
                hour, 
                minute, 
                wigos_identifier_series_number, 
                wigos_identifier_issuer, 
                wigos_identifier_issue_number, 
                wigos_identifier_local_identifier,
                block_number,
                station_number,
                latitude,
                longitude,
                station_elevation,
                barometer_height_above_sealevel
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
        """

        connection = self.connection_pool.getconn()

        try:
            with connection:
                with connection.cursor() as cursor:
                    for message_data in decoded_data['messages']:
                        values = (
                            message_id,
                            message_data.get('year', None),
                            message_data.get('month', None),
                            message_data.get('day', None),
                            message_data.get('hour', None),
                            message_data.get('minute', None),
                            message_data.get('wigosIdentifierSeries', None),
                            message_data.get('wigosIssuerOfIdentifier', None),
                            message_data.get('wigosIssueNumber', None),
                            message_data.get('wigosLocalIdentifierCharacter', None),
                            message_data.get('blockNumber', None),
                            message_data.get('stationNumber', None),
                            message_data.get('latitude', None),
                            message_data.get('longitude', None),
                            message_data.get('elevation', None),
                            message_data.get('heightOfBarometerAboveMeanSeaLevel', None),
                        )
                        sql_query = cursor.mogrify(query, values)
                        cursor.execute(sql_query)

        finally:
            self.connection_pool.putconn(connection)

    @staticmethod
    def decode_bufr_file(file_path):
        return BufrFileManager.download_manager.decode_bufr_file(file_path)

    def download_file(self, data_list, constructed_link, message_id):
        self.download_manager.download_file(data_list, constructed_link, message_id)

class MqttHandler:
    def __init__(self, gs, mqtt_config, db_manager, download_directory):
        self.client = mqtt.Client()
        self.client.tls_set(ca_certs=None, certfile=None, keyfile=None,
                            cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS,
                            ciphers=None)

        self.client.username_pw_set(username=mqtt_config['Username'], password=mqtt_config['Password'])

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.broker_address = mqtt_config['BrokerAddress']
        self.port = int(mqtt_config['Port'])
        self.db_manager = db_manager
        self.download_directory = download_directory

        self.connect()

    def connect(self):
        self.client.connect(self.broker_address, self.port, keepalive=60)
        self.client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to the MQTT broker")
            client.subscribe("cache/a/wis2/+/+/+/core/+/surface-based-observations/#")

        else:
            logging.error("Connection failed, return code = %d", rc)

    def on_message(self, client, userdata, msg):
        data_list = []
        constructed_link = None
        message_id = None
        try:
            payload = json.loads(msg.payload.decode())

            # specific topic
            topic_pattern = re.compile(r'wis2/[^/]+/[^/]+/[^/]+/core/[^/]+/surface-based-observations/')
            match = topic_pattern.search(msg.topic)
            if match:
                topic = match.group(0)
            else:
                topic = 'N/A'

            # other params
            publication_timestamp = payload['properties'].get('pubtime', 'N/A')
            data_id = payload['properties'].get('data_id', 'N/A')
            canonical_url = next((link['href'] for link in payload.get('links', []) if link.get('rel') == 'canonical'), 'N/A')
            wigos_station_identifier = payload['properties'].get('wigos_station_identifier', 'N/A')

            # Add variables to data_list
            data_list.append({
                'topic': topic,
                'publication_timestamp': publication_timestamp,
                'data_id': data_id,
                'canonical_url': canonical_url,
                'wigos_station_identifier': wigos_station_identifier
            })

            # <! -- 1 -- !>
            blacklist = ConfigReader.read_config('config/general_config.ini', 'Blacklist')
            blacklist = [[key, value] for key, value in blacklist.items()]
            # check 1 blacklist center
            if all(key not in data_id for key, value in blacklist):
                not_allowed_extensions = ['.png', '.jpeg', '.jpg']
                # check 2 format not suported
                if not any(canonical_url.lower().endswith(ext) for ext in not_allowed_extensions):
                    message_id = self.db_manager.insert_message_data(data_list)
                    if message_id:
                        publication_timestamp_obj = datetime.strptime(publication_timestamp, "%Y-%m-%dT%H:%M:%SZ") if '.' not in publication_timestamp else datetime.strptime(publication_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                        date_directory = publication_timestamp_obj.strftime("%Y%m%d")
                        constructed_link = f"{topic}/{date_directory}"
                        # <! -- 2 -- !>
                        self.db_manager.download_file(data_list, constructed_link, message_id)

        except json.JSONDecodeError as e:
            logging.error("JSON decoding error: %s", e)
            logging.debug("Original JSON payload: %s", msg.payload)
            logging.debug(traceback.format_exc())
        except Exception as e:
            logging.error("An unexpected error occurred: %s", e)
            logging.debug(traceback.format_exc())

def main():
    # logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(level=logging.CRITICAL)
    general_config = ConfigReader.read_config('config/general_config.ini', 'General')
    gs = general_config['GS']
    download_directory = general_config['DownloadDirectory']

    mqtt_config = ConfigReader.read_config('config/mqtt_config.ini', gs)
    db_config = ConfigReader.read_config('config/database_config.ini', 'Database')

    db_manager = DatabaseManager(db_config, download_directory)
    mqtt_handler = MqttHandler(gs, mqtt_config, db_manager, download_directory)

if __name__ == "__main__":
    main()
