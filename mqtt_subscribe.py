# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Import required libraries
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

# MQTT and InfluxDB Configuration
MQTT_BROKER = "broker.emqx.io"  # MQTT broker address
MQTT_PORT = 8883                # MQTT broker port (SSL)
MQTT_TOPIC = "sensor/data"      # Topic to subscribe to
CLIENT_ID = f"mqtt_client_{socket.gethostname()}"  # Unique client ID

# Email alert configuration from environment variables
alert_recipients = [email.strip() for email in os.getenv("ALERT_RECIPIENTS_BIN_LEVEL", "").split(",")]
SMTP_USER = os.getenv("SMTP_USER")  # SMTP username
SMTP_PASS = os.getenv("SMTP_PASS")  # SMTP password
SMTP_SERVER = os.getenv("SMTP_SERVER")  # SMTP server address
SMTP_PORT = os.getenv("SMTP_PORT")      # SMTP port

# InfluxDB configuration
INFLUXDB_URL = "http://localhost:8086"  # InfluxDB server URL
INFLUXDB_TOKEN = "8bcq81ihsLq5g7G81u940C_5-OVhX1h3zZSNzc_6CLQPxEaTrNCqcrZyLvJPnEfHeaNKaYlOVPvEKRW3GhR4Cg=="  # InfluxDB token
INFLUXDB_ORG = "AQS"          # InfluxDB organization
INFLUXDB_BUCKET = "mqtt_data" # Default InfluxDB bucket

# Initialize InfluxDB Client
influxdb_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

def connect_mqtt() -> mqtt_client:
    """
    Connect to MQTT broker with SSL/TLS encryption.
    
    Returns:
        mqtt_client: Connected MQTT client instance
    """
    def on_connect(client, userdata, flags, rc):
        """Callback for connection events"""
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print(f"Failed to connect, return code {rc}\n")

    client = mqtt_client.Client(CLIENT_ID)  # Create client instance
    client.on_connect = on_connect          # Set connection callback
    client.tls_set(tls_version=ssl.PROTOCOL_TLS)  # Enable TLS
    client.connect(MQTT_BROKER, MQTT_PORT)  # Connect to broker
    return client

# Timezone Handling
local_timezone = get_localzone()  # Get system's local timezone

# Dictionary to track bins that have triggered alerts
alerted_bins = {}

# Thresholds for bin level alerts
FULL_BIN_THRESHOLD = 80.00    # Level at which bin is considered full
CLEARED_BIN_THRESHOLD = 50.00 # Level below which bin is considered cleared

def convert_to_system_timezone():
    """
    Convert current UTC time to local system timezone.
    
    Returns:
        str: ISO formatted local time string
    """
    utc_now = datetime.now(pytz.utc)
    local_time = utc_now.astimezone(local_timezone)
    return local_time.isoformat()

def send_email(subject, body):
    """
    Send email alert to all recipients.
    
    Args:
        subject (str): Email subject
        body (str): Email body content
    """
    for dest in alert_recipients:
        try:
            # Create SMTP session
            s = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
            s.starttls()  # Enable TLS
            s.login(SMTP_USER, SMTP_PASS)  # Authenticate
            
            # Create email message
            msg = MIMEMultipart()
            msg['From'] = SMTP_USER
            msg['To'] = dest
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))  # Add body
            
            # Send email
            s.sendmail(msg['From'], msg['To'], msg.as_string())
            s.quit()  # End session
            print(f"Email sent: {subject}")
        except Exception as e:
            print(f"Error sending email alert: {e}")

def send_email_alert(bin_id, bin_level):
    """
    Send appropriate email alert based on bin level changes.
    
    Args:
        bin_id (str): Identifier for the bin
        bin_level (float): Current fill level percentage
    """
    global alerted_bins

    # Check if bin was previously full
    was_full = alerted_bins.get(bin_id) == "Full"

    # Send "full" alert if threshold crossed
    if bin_level >= FULL_BIN_THRESHOLD and not was_full:
        subject = f"Bin {bin_id} Alert: High Fill Level!"
        body = f"The bin with ID {bin_id} has reached a level of {bin_level}%. Please schedule waste collection."
        send_email(subject, body)
        alerted_bins[bin_id] = "Full"  # Update status

    # Send "cleared" alert if bin was emptied
    elif was_full and bin_level < CLEARED_BIN_THRESHOLD:
        subject = f"Bin {bin_id} Alert: Bin Cleared!"
        body = f"The bin with ID {bin_id} has been cleared and is now at {bin_level}%."
        send_email(subject, body)
        alerted_bins[bin_id] = "Cleared"  # Update status
    
def filter_unwanted_messages(raw_data):
    """
    Filter out messages containing motion sensor data.
    
    Args:
        raw_data (dict): Parsed message data
        
    Returns:
        bool: True if message should be ignored
    """
    sensor_keys = {"gyroX", "gyroY", "gyroZ", "accX", "accY", "accZ", "pitch", "roll"}
    return any(key in raw_data for key in sensor_keys)

def standardize_message_format_save_csv(message):
    """
    Format message for CSV storage and filter unwanted data.
    
    Args:
        message (str): Raw MQTT message
        
    Returns:
        dict: Standardized message or None if invalid
    """
    try:
        raw_data = json.loads(message)
        if filter_unwanted_messages(raw_data):
            print("Skipping motion sensor data message.")
            return None
            
        # Get timestamp or use current time
        timestamp = raw_data.get('timestamp', convert_to_system_timezone())
        bin_id = raw_data.get("BinID")
        bin_level = raw_data.get("bin_level")

        # Skip if essential data missing
        if bin_id is None or bin_level is None:
            return None

        # Create standardized format
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
    """
    Format message for InfluxDB storage and filter unwanted data.
    
    Args:
        message (str): Raw MQTT message
        
    Returns:
        dict: Standardized message or None if invalid
    """
    try:
        raw_data = json.loads(message)
        if filter_unwanted_messages(raw_data):
            print("Skipping motion sensor data message.")
            return None
            
        # Get timestamp or use current time    
        timestamp = raw_data.get('timestamp', convert_to_system_timezone())

        # Create comprehensive standardized format
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
    """
    Create InfluxDB bucket if it doesn't exist.
    
    Args:
        bucket_name (str): Name of bucket to create
    """
    try:
        buckets_api = influxdb_client.buckets_api()
        existing_buckets = buckets_api.find_buckets()

        if not any(bucket.name == bucket_name for bucket in existing_buckets.buckets):
            buckets_api.create_bucket(bucket_name=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
    except Exception as e:
        print(f"Error creating bucket '{bucket_name}': {e}")

def save_message_to_influxdb(message):
    """
    Save message to InfluxDB with dynamic monthly buckets.
    
    Args:
        message (str): Raw MQTT message
    """
    json_data = standardize_message_format_save_influxdb(message)
    if json_data:
        try:
            bin_id = json_data['fields']['BinID']
            bin_level = json_data['fields']['bin_level']
            dt = datetime.fromisoformat(json_data['fields']['Time'])
            
            # Create dynamic bucket name based on month
            dynamic_bucket = f"bin_levels_{dt.strftime('%Y-%m')}"
            create_bucket_if_not_exists(dynamic_bucket)

            # Create InfluxDB point with all fields
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

            # Write to InfluxDB
            with influxdb_client.write_api() as write_api:
                write_api.write(bucket=dynamic_bucket, org=INFLUXDB_ORG, record=point)
            print(f"Message written to InfluxDB (Bucket: {dynamic_bucket}): {json.dumps(json_data, indent=4)}")

            # Trigger email alerts if needed
            send_email_alert(bin_id, bin_level)

        except Exception as e:
            print(f"Failed to write message to InfluxDB: {e}")

def delete_old_csv_files(bin_id):
    """
    Delete CSV files older than 3 months for a specific bin.
    
    Args:
        bin_id (str): Bin identifier for file naming pattern
    """
    current_time = datetime.now()
    csv_files = glob.glob(f"{bin_id}_data_*.csv")  # Find matching files

    for file in csv_files:
        try:
            # Extract date from filename
            match = re.search(r'_data_(\d{4})_(\d{2})', file)
            if match:
                year, month = map(int, match.groups())
                file_date = datetime(year, month, 1)

                # Delete if older than 90 days
                if file_date < current_time - timedelta(days=90):
                    os.remove(file)
                    print(f"Deleted old CSV file: {file}")
        except Exception as e:
            print(f"Error deleting file {file}: {e}")

def save_message_to_csv(message):
    """
    Save message to monthly CSV file with cleanup of old files.
    
    Args:
        message (str): Raw MQTT message
    """
    json_data = standardize_message_format_save_csv(message)
    if json_data and json_data["fields"].get("bin_level") is not None and json_data["fields"].get("BinID") is not None:
        bin_id = json_data["fields"]["BinID"]
        dt = datetime.fromisoformat(json_data['fields']['Time'])
        csv_file = f"{bin_id}_data_{dt.strftime('%Y_%m')}.csv"  # Monthly CSV files
        
        try:
            delete_old_csv_files(bin_id)  # Cleanup old files
            
            # Append to CSV file
            with open(csv_file, mode='a', newline='') as file:
                writer = csv.writer(file)
                if file.tell() == 0:  # Write header if new file
                    writer.writerow(json_data['fields'].keys())
                writer.writerow(json_data['fields'].values())
                
            print(f"Message saved to CSV: {json.dumps(json_data['fields'], indent=4)}")
        except Exception as e:
            print(f"Failed to save message to CSV: {e}")

def subscribe(client: mqtt_client):
    """
    Subscribe to MQTT topic and set up message handler.
    
    Args:
        client (mqtt_client): Connected MQTT client
    """
    def on_mqtt_message(client, userdata, msg):
        """Callback for incoming MQTT messages"""
        raw_message = msg.payload.decode()
        print(f"Raw message received: {raw_message}")
        try:
            json_data = json.loads(raw_message)
            if isinstance(json_data, dict):
                # Save to both InfluxDB and CSV
                save_message_to_influxdb(raw_message)
                save_message_to_csv(raw_message)

        except json.JSONDecodeError:
            print(f"Invalid JSON format: {raw_message}")
            
    client.subscribe(MQTT_TOPIC, qos=1)
    print(f"Subscribed to topic: {MQTT_TOPIC}")
    client.on_message = on_mqtt_message  # Set message handler

def run():
    """Main function to start MQTT client and processing loop."""
    client = connect_mqtt()  # Connect to broker
    subscribe(client)        # Set up MQTT Subscription
    client.loop_forever()   # Start processing loop

if __name__ == '__main__':
    run()  # Start application