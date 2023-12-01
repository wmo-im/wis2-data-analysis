# bufr_manager.py
import os
import logging
import traceback
import requests
from eccodes import *

from config_reader import ConfigReader
from database_manager import DatabaseManager

CONFIG_DIR = 'config'
GENERAL_CONFIG_FILE = 'general_config.ini'
BUFR_KEYS_FILE = 'BUFRKeys.ini'

class BufrFileManager:
    def __init__(self):
        # Initialize BufrFileManager with configuration and database manager
        general_config = ConfigReader.read_config(os.path.join(CONFIG_DIR, GENERAL_CONFIG_FILE), 'General')
        self.download_directory = general_config['DownloadDirectory']
        self.db_manager = DatabaseManager()

    def download_file(self, data_list, constructed_link, message_id):
        # Download a file from a given URL, save it locally, and decode
        data_entry = data_list[-1]
        download_url = data_entry['canonical_url']
        file_name = os.path.basename(download_url)
        local_path = os.path.join(os.path.join(self.download_directory, constructed_link), file_name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        try:
            logging.debug(f"Attempting to download from URL: {download_url}")
            response = requests.get(download_url)
            response.raise_for_status()

            with open(local_path, 'wb') as file:
                file.write(response.content)

            logging.info(f"File downloaded successfully: {local_path}")

            # Decode the downloaded file
            decoded_data = self.decode_bufr_file(local_path)

            # Insert decoded BUFR data into the database
            self.db_manager.insert_bufr_data(message_id, decoded_data)

            print(decoded_data)

        except requests.RequestException as e:
            logging.error(f"Error during file download: {e}")
            logging.debug(traceback.format_exc())
        except Exception as e:
            logging.error(f"Error during file decoding: {e}")
            logging.debug(traceback.format_exc())

    def decode_bufr_file(self, file_path):
        # Decode a BUFR file and return the decoded data
        keys_config = ConfigReader.read_config(os.path.join(CONFIG_DIR, BUFR_KEYS_FILE), 'BUFRKeys')
        required_columns = keys_config.get('required_columns', '').split(',')
        additional_columns = keys_config.get('additional_columns', '').split(',')
        keys = required_columns + additional_columns
        
        decoded_data = {'message_count': 0, 'messages': []}

        with open(file_path, 'rb') as bufr_file:
            while True:
                bufr = codes_bufr_new_from_file(bufr_file)
                if bufr is None:
                    break

                codes_set(bufr, "unpack", 1)

                message_data = {'message_number': decoded_data['message_count']}

                for key in keys:
                    message_data[key] = codes_get(bufr, key)
                    if message_data[key] in (CODES_MISSING_DOUBLE, CODES_MISSING_LONG):
                        message_data[key] = None

                decoded_data['messages'].append(message_data)
                decoded_data['message_count'] += 1

                codes_release(bufr)

        return decoded_data
