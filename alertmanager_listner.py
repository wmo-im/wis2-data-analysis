from flask import Flask, request, jsonify
import json
import uuid
import datetime
import configparser
import paho.mqtt.publish as publish
import logging
import requests

app = Flask(__name__)

# Logging configuration
logging.basicConfig(level=logging.INFO)

# Load configuration information from the INI file
config = configparser.ConfigParser()
config.read('config/access.ini')

MQTT_BROKER = config['MQTT']['broker']
MQTT_PORT = int(config['MQTT']['port'])
MQTT_USERNAME = config['MQTT']['username']
MQTT_PASSWORD = config['MQTT']['password']

# Jira authentication
JIRA_API_URL = config['JIRA']['url']
JIRA_AUTH_TOKEN = config['JIRA']['token']

def send_mqtt_notification(centre_id, report_by, alertname, severity, starts_at):
    # Build MQTT topic with centre_id
    mqtt_topic = f"monitor/a/wis2/ma-marocmeteo-global-monitor/{centre_id}"

    # Construct the JSON payload for the MQTT message
    json_payload = {
        "specversion": "1.0",
        "type": "int.wmo.codes.performance",
        "source": "ma-marocmeteo-global-monitor",
        "subject": "some-subject",  # Adapt this as necessary
        "id": str(uuid.uuid4()),
        "time": starts_at,
        "datacontenttype": "application/json",
        "dataschema": "int.wmo.codes.event.data.v1",
        "data": {
            "level": severity,
            "text": f"{alertname} in {centre_id} reported by {report_by}"
        }
    }

    # Publish the JSON payload to the MQTT topic
    publish.single(mqtt_topic, json.dumps(json_payload), hostname=MQTT_BROKER, port=MQTT_PORT,
                   auth={'username': MQTT_USERNAME, 'password': MQTT_PASSWORD})
    logging.info(f"MQTT notification sent for {centre_id}")

def create_jira_ticket(summary, description, issue_type="Bug"):
    # Construct the POST request payload for Jira
    jira_payload = {
        "fields": {
            "project": {
                "key": "WI"  #Jira project key
            },
            "summary": summary,
            "description": description,
            "issuetype": {
                "name": issue_type  # Bug, Task, etc.
            }
        }
    }

    # Headers for Bearer authentication
    headers = {
        "Authorization": f"Bearer {JIRA_AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    # Send the POST request to the Jira API
    response = requests.post(JIRA_API_URL, headers=headers, json=jira_payload, verify=False)

    # Check Jira's response
    if response.status_code == 201:
        logging.info("Jira ticket created successfully")
        return response.json()
    else:
        logging.error(f"Error creating Jira ticket: {response.text}")
        return None

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    try:
        for alert in data.get('alerts', []):
            centre_id = alert['labels'].get('centre_id')
            report_by = alert['labels'].get('report_by')
            alertname = alert['labels'].get('alertname')
            severity = alert['labels'].get('severity')
            starts_at = alert.get('startsAt')

            # Validate required fields
            if not all([centre_id, report_by, alertname, severity, starts_at]):
                logging.error('Missing required fields in alert data')
                return jsonify({'status': 'error', 'message': 'Missing required fields'}), 400

            # Send an MQTT notification
            send_mqtt_notification(centre_id, report_by, alertname, severity, starts_at)

            # Create a Jira ticket for each alert
            summary = f"Alert {alertname} in {centre_id}"
            description = f"{alertname} reported by {report_by} with a severity of {severity}."

            jira_response = create_jira_ticket(summary, description)

            if jira_response:
                logging.info(f"Jira ticket created with ID {jira_response['id']}")

        return jsonify({'status': 'success'})
    
    except Exception as e:
        logging.error(f"Error processing webhook: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)
