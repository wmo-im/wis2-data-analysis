# data_processor.py
import logging
from queue import Empty
import multiprocessing
import os
import re
import time

from datetime import datetime
from config_reader import ConfigReader
from mqtt_subscriber import MqttSubscriber
from database_manager import DatabaseManager
from bufr_manager import BufrFileManager

CONFIG_DIR = 'config'
DATABASE_CONFIG_FILE = 'database_config.ini'  # Utilisation du nom correct du fichier de configuration
GENERAL_CONFIG_FILE = 'general_config.ini'

class DataProcessor:
    def __init__(self, queue):
        # Initialize DataProcessor with a message queue and necessary managers
        self.queue = queue
        self.db_manager = DatabaseManager()
        self.bufr_manager = BufrFileManager()
        self.BATCH_SIZE = 50

    def process_batch(self, bufr_copy):
        for message in bufr_copy:
            self.process_message(message)

    def process_messages(self):
        # Continuously process messages from the queue
        # process_batch is called every 5 seconds or when the batch size is reached

        buffer = []  # Initialize an empty buffer
        last_batch_time = time.time()  # Record the time when the last batch was processed

        while True:
            try:
                payload = self.queue.get(timeout=1)
                buffer.append(payload) # Add the payload to the buffer
                # Check if BATCH_SIZE is reached
                if len(buffer) >= self.BATCH_SIZE:
                    # Process the batch if BATCH_SIZE is reached
                    multiprocessing.Process(target=self.process_batch, args=(buffer.copy(),)).start()
                    buffer = []  # Clear the original buffer
                    last_batch_time = time.time()  # Update last_batch_time
                    continue
                # Check if 5 seconds have elapsed since the last batch processing
                current_time = time.time()
                if current_time - last_batch_time >= 5:
                    # Process the current buffer and reset last_batch_time
                    multiprocessing.Process(target=self.process_batch, args=(buffer.copy(),)).start()
                    buffer = []  # Clear the original buffer
                    last_batch_time = current_time  # Update last_batch_time
                
            except Empty:
                pass

    def process_message(self, payload):
        # Process individual message payload
        data_list = []

        # Extract information from the payload
        topic_pattern = re.compile(r'wis2/[^/]+/[^/]+/[^/]+/[^/]+/[^/]+/[^/]+/synop')
        match = topic_pattern.search(payload['topic'])
        topic = match.group(0) if match else 'N/A'

        publication_timestamp = payload['data']['properties'].get('pubtime', 'N/A')
        data_id = payload['data']['properties'].get('data_id', 'N/A')
        canonical_url = next((link['href'] for link in payload['data'].get('links', []) if link.get('rel') == 'canonical'), 'N/A')
        wigos_station_identifier = payload['data']['properties'].get('wigos_station_identifier', 'N/A')

        # Add variables to data_list
        data_list.append({
            'topic': topic,
            'publication_timestamp': publication_timestamp,
            'data_id': data_id,
            'canonical_url': canonical_url,
            'wigos_station_identifier': wigos_station_identifier
        })

        # Check blacklist and file format restrictions
        blacklist = ConfigReader.read_config(os.path.join(CONFIG_DIR, GENERAL_CONFIG_FILE), 'Blacklist')
        blacklist = [[key, value] for key, value in blacklist.items()]

        if all(key not in data_id for key, value in blacklist):
            not_allowed_extensions = ['.png', '.jpeg', '.jpg']
            if not any(canonical_url.lower().endswith(ext) for ext in not_allowed_extensions):
                # Insert message data into the database
                message_id = self.db_manager.insert_message_data(data_list)
                publication_timestamp_obj = datetime.strptime(publication_timestamp, "%Y-%m-%dT%H:%M:%SZ") if '.' not in publication_timestamp else datetime.strptime(publication_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                date_directory = publication_timestamp_obj.strftime("%Y%m%d")
                constructed_link = f"{topic}/{date_directory}"

                # Download the file locally
                self.bufr_manager.download_file(data_list, constructed_link, message_id)

def main():
    # Create a multiprocessing queue for communication between processes
    message_queue = multiprocessing.Queue()

    # Start the MQTT Subscriber in a separate process
    general_config = ConfigReader.read_config(os.path.join(CONFIG_DIR, 'general_config.ini'), 'General')
    gs = general_config['GS']
    mqtt_config = ConfigReader.read_config(os.path.join(CONFIG_DIR, 'mqtt_config.ini'), gs)
    mqtt_subscriber_process = multiprocessing.Process(target=MqttSubscriber(gs, mqtt_config, message_queue).start)
    mqtt_subscriber_process.start()

    # Start the Data Processor in the main process
    data_processor = DataProcessor(message_queue)
    data_processor.process_messages()

    # Wait for the MQTT Subscriber to finish before terminating the main process
    mqtt_subscriber_process.join()

if __name__ == "__main__":
    main()
