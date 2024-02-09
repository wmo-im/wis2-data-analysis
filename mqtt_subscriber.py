# mqtt_subscriber.py
import paho.mqtt.client as mqtt
import logging
import time
import ssl
import json
import re
import os
from queue import Queue
from config_reader import ConfigReader

CONFIG_DIR = 'config'
GENERAL_CONFIG_FILE = 'general_config.ini'
MQTT_CONFIG_FILE = 'mqtt_config.ini'

class MqttSubscriber:
    def __init__(self, gs, mqtt_config, queue):
        # Initialize MQTT client
        self.client = mqtt.Client()
        # Set up TLS configuration
        self.client.tls_set(ca_certs=None, certfile=None, keyfile=None,
                            cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS,
                            ciphers=None)
        # Set MQTT username and password
        self.client.username_pw_set(username=mqtt_config['Username'], password=mqtt_config['Password'])

        # Set callback functions
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        # Set MQTT broker address, port, and other attributes
        self.broker_address = mqtt_config['BrokerAddress']
        self.port = int(mqtt_config['Port'])
        self.queue = queue
        self.is_running = False

    def connect(self):
        # Connect to the MQTT broker
        self.client.connect(self.broker_address, self.port, keepalive=60)
        # Start the MQTT loop in a blocking manner
        self.client.loop_forever()
        self.is_running = True

    def on_connect(self, client, userdata, flags, rc):
        # Callback function when the client connects to the MQTT broker
        if rc == 0:
            logging.info("Connected to the MQTT broker")
            # Subscribe to a specific MQTT topic
            client.subscribe("cache/a/wis2/+/+/+/core/+/surface-based-observations/synop")
        else:
            logging.error("Connection failed, return code = %d", rc)

    def on_message(self, client, userdata, msg):
        # Callback function when a message is received
        try:
            # Decode JSON payload
            data = json.loads(msg.payload.decode())
            payload = {
                'data': data,
                'topic': msg.topic
            }
            logging.info(f"Received message: {payload}")
            # Put the payload into the message queue
            self.queue.put(payload)
        
        except json.JSONDecodeError as e:
            logging.error("JSON decoding error: %s", e)
            logging.debug("Original JSON payload: %s", msg.payload)
            logging.debug(traceback.format_exc())
        except Exception as e:
            logging.error("An unexpected error occurred: %s", e)
            logging.debug(traceback.format_exc())

    def start(self):
        # Start the MQTT client
        self.connect()

    def stop(self):
        # Disconnect and stop the MQTT client loop
        self.client.disconnect()
        self.client.loop_stop()
        self.is_running = False

def main():
    # Configure logging level
    logging.basicConfig(level=logging.CRITICAL)
    
    # Read general configuration
    general_config = ConfigReader.read_config(os.path.join(CONFIG_DIR, GENERAL_CONFIG_FILE), 'General')
    gs = general_config['GS']

    # Read MQTT configuration
    mqtt_config = ConfigReader.read_config(os.path.join(CONFIG_DIR, MQTT_CONFIG_FILE), gs)
    
    # Create a message queue
    message_queue = Queue()

    # Create an instance of MqttSubscriber
    mqtt_subscriber = MqttSubscriber(gs, mqtt_config, message_queue)
    
    # Start the MQTT subscriber
    mqtt_subscriber.start()

    # Wait for the MQTT subscriber to finish
    while mqtt_subscriber.is_running:
        time.sleep(1)  

    print("MQTT Subscriber stopped.")

if __name__ == "__main__":
    main()
