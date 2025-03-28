from dotenv import load_dotenv
load_dotenv()

import json
import ssl
import socket
import csv
import pytz
import os
import glob
import re
from datetime import datetime, timedelta
from tzlocal import get_localzone
from paho.mqtt import client as mqtt_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# MQTT and InfluxDB settings
MQTT_BROKER = "broker.emqx.io"
MQTT_PORT = 8883
MQTT_TOPIC = "sensor/data"
CLIENT_ID = f"mqtt_client_{socket.gethostname()}"

alert_recipients = [email.strip() for email in os.getenv("ALERT_RECIPIENTS_BIN_LEVEL", "").split(",")]
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = os.getenv("SMTP_PORT")

INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "8bcq81ihsLq5g7G81u940C_5-OVhX1h3zZSNzc_6CLQPxEaTrNCqcrZyLvJPnEfHeaNKaYlOVPvEKRW3GhR4Cg=="
INFLUXDB_ORG = "AQS"
INFLUXDB_BUCKET = "mqtt_data"


# Initialize InfluxDB Client
influxdb_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print(f"Failed to connect, return code {rc}\n")

    client = mqtt_client.Client(CLIENT_ID)
    client.on_connect = on_connect
    client.tls_set(tls_version=ssl.PROTOCOL_TLS)
    client.connect(MQTT_BROKER, MQTT_PORT)
    return client

# Timezone Handling
local_timezone = get_localzone()

# Dictionary to track bins that have already triggered an alert
alerted_bins = {}

FULL_BIN_THRESHOLD = 80.00
CLEARED_BIN_THRESHOLD = 50.00 

def convert_to_system_timezone():
    utc_now = datetime.now(pytz.utc)
    local_time = utc_now.astimezone(local_timezone)
    return local_time.isoformat()

def send_email(subject, body):
    for dest in alert_recipients:
        try:
            # Create an SMTP session
            s = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            # Create the email object
            msg = MIMEMultipart()
            msg['From'] = SMTP_USER
            msg['To'] = dest
            msg['Subject'] = subject
            # Email body
            msg.attach(MIMEText(body, 'plain'))
            # Send the email
            s.sendmail(msg['From'], msg['To'], msg.as_string())
            # Terminate the session
            s.quit()
            print(f"Email sent: {subject}")
        except Exception as e:
            print(f"Error sending email alert: {e}")

def send_email_alert(bin_id, bin_level):
    global alerted_bins

    # Check if the bin was previously full
    was_full = alerted_bins.get(bin_id) == "Full"

    # If the bin is now full and wasn't previously full, send a "full" email
    if bin_level >= FULL_BIN_THRESHOLD and not was_full:
        subject = f"Bin {bin_id} Alert: High Fill Level!"
        body = f"The bin with ID {bin_id} has reached a level of {bin_level}%. Please schedule waste collection."
        send_email(subject, body)
        alerted_bins[bin_id] = "Full"  # Mark bin as full

    # If the bin was previously full and is now cleared, send a "cleared" email
    elif was_full and bin_level < CLEARED_BIN_THRESHOLD:
        subject = f"Bin {bin_id} Alert: Bin Cleared!"
        body = f"The bin with ID {bin_id} has been cleared and is now at {bin_level}%."
        send_email(subject, body)
        alerted_bins[bin_id] = "Cleared"
    
# Function to filter messages before processing
def filter_unwanted_messages(raw_data):
    sensor_keys = {"gyroX", "gyroY", "gyroZ", "accX", "accY", "accZ", "pitch", "roll"}
    return any(key in raw_data for key in sensor_keys)  # Returns True if the message should be ignored

def standardize_message_format_save_csv(message):
    try:
        raw_data = json.loads(message)
        if filter_unwanted_messages(raw_data):
            print("Skipping motion sensor data message.")
            return None
        timestamp = raw_data.get('timestamp', convert_to_system_timezone())

        bin_id = raw_data.get("BinID")
        bin_level = raw_data.get("bin_level")

        # Ensure both BinID and bin_level are present before proceeding
        if bin_id is None or bin_level is None:
            return None  # Skip saving if data is incomplete

        standardized_message = {
            "measurement": "bin_levels_data",
            "fields": {
                "BinID": bin_id,
                "bin_level": bin_level,
                "Time": timestamp,
            }
        }
        return standardized_message
    except json.JSONDecodeError:
        return None

def standardize_message_format_save_influxdb(message):
    try:
        raw_data = json.loads(message)
        if filter_unwanted_messages(raw_data):
            print("Skipping motion sensor data message.")
            return None
        timestamp = raw_data.get('timestamp', convert_to_system_timezone())

        standardized_message = {
            "measurement": "bin_levels_data",
            "fields": {
                "BinID": raw_data.get("BinID"),
                "bin_level": raw_data.get("bin_level"),
                "bin_status": raw_data.get("bin_status"),
                "Time": timestamp,
                "Latitude": raw_data.get('Latitude'),
                "Longitude": raw_data.get('Longitude'),
                "Address": raw_data.get("Address"),
            }
        }

        return standardized_message
    except json.JSONDecodeError:
        print(f"Error parsing JSON message: {message}")
        return None

def create_bucket_if_not_exists(bucket_name):
    try:
        buckets_api = influxdb_client.buckets_api()
        existing_buckets = buckets_api.find_buckets()

        if not any(bucket.name == bucket_name for bucket in existing_buckets.buckets):
            buckets_api.create_bucket(bucket_name=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
    except Exception as e:
        print(f"Error creating bucket '{bucket_name}': {e}")

def save_message_to_influxdb(message):
    json_data = standardize_message_format_save_influxdb(message)
    if json_data:
        try:
            bin_id = json_data['fields']['BinID']
            bin_level = json_data['fields']['bin_level']
            dt = datetime.fromisoformat(json_data['fields']['Time'])
            dynamic_bucket = f"bin_levels_{dt.strftime('%Y-%m')}"
            create_bucket_if_not_exists(dynamic_bucket)

            point = (
                Point(json_data['measurement'])
                .tag("source", "mqtt")
                .tag("BinID", str(json_data['fields']['BinID']))
                .field("bin_level", json_data['fields']['bin_level'])
                .field("bin_status", json_data['fields']['bin_status'])
                .field("Latitude", json_data['fields']['Latitude'])
                .field("Longitude", json_data['fields']['Longitude'])
                .field("Address", json_data['fields']['Address'])
                .time(dt, WritePrecision.NS)
            )

            with influxdb_client.write_api() as write_api:
                write_api.write(bucket=dynamic_bucket, org=INFLUXDB_ORG, record=point)
            print(f"Message written to InfluxDB (Bucket: {dynamic_bucket}): {json.dumps(json_data, indent=4)}")

            send_email_alert(bin_id, bin_level)

        except Exception as e:
            print(f"Failed to write message to InfluxDB: {e}")

def delete_old_csv_files(bin_id):
    """Delete CSV files older than 3 months in the current directory for a specific bin."""
    current_time = datetime.now()
    csv_files = glob.glob(f"{bin_id}_data_*.csv")  # Look for matching CSV files

    for file in csv_files:
        try:
            # Use regex to extract year and month, ignoring anything after (like (2))
            match = re.search(r'_data_(\d{4})_(\d{2})', file)
            if match:
                year, month = map(int, match.groups())
                file_date = datetime(year, month, 1)

                # Delete if older than 3 months (using 90 days for true 3 months)
                if file_date < current_time - timedelta(days=90):
                    os.remove(file)
                    print(f"Deleted old CSV file: {file}")
        except Exception as e:
            print(f"Error deleting file {file}: {e}")

def save_message_to_csv(message):
    json_data = standardize_message_format_save_csv(message)
    if json_data and json_data["fields"].get("bin_level") is not None and json_data["fields"].get("BinID") is not None:
        bin_id = json_data["fields"]["BinID"]
        dt = datetime.fromisoformat(json_data['fields']['Time'])
        csv_file = f"{bin_id}_data_{dt.strftime('%Y_%m')}.csv"
        try:
            delete_old_csv_files(bin_id)
            with open(csv_file, mode='a', newline='') as file:
                writer = csv.writer(file)
                if file.tell() == 0:
                    writer.writerow(json_data['fields'].keys())
                writer.writerow(json_data['fields'].values())
            print(f"Message saved to CSV: {json.dumps(json_data['fields'], indent=4)}")
        except Exception as e:
            print(f"Failed to save message to CSV: {e}")
def subscribe(client: mqtt_client):
    def on_mqtt_message(client, userdata, msg):
        raw_message = msg.payload.decode()
        print(f"Raw message received: {raw_message}")
        try:
            json_data = json.loads(raw_message)
            if isinstance(json_data, dict):
                save_message_to_influxdb(raw_message)
                save_message_to_csv(raw_message)

        except json.JSONDecodeError:
            print(f"Invalid JSON format: {raw_message}")
    client.subscribe(MQTT_TOPIC, qos=1)
    print(f"Subscribed to topic: {MQTT_TOPIC}")
    client.on_message = on_mqtt_message

def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()

if __name__ == '__main__':
    run()