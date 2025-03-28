from dotenv import load_dotenv
load_dotenv()

import serial
import json
import ssl
import socket
import time
import threading
import os
from datetime import datetime, timedelta
from tzlocal import get_localzone
from paho.mqtt import client as mqtt_client
from geopy.geocoders import Nominatim
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# MQTT and InfluxDB settings
MQTT_BROKER = "broker.emqx.io"
MQTT_PORT = 8883
MQTT_TOPIC = "sensor/data"
CLIENT_ID = f"mqtt_client_{socket.gethostname()}"

alert_recipients = [email.strip() for email in os.getenv("ALERT_RECIPIENTS_NO_DATA", "").split(",")]
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = os.getenv("SMTP_PORT")

ALERT_DURATION_MINUTES = 20

# Initialize Serial Connection
ser = serial.Serial('COM3', 115200, timeout=1800)

# Timezone Handling
local_timezone = get_localzone()

# Variable to track the last published time
last_published_time = datetime.now()

# Variable to store the current BinID
current_bin_id = os.getenv("BinID")

def get_address_from_coordinates(latitude, longitude):
    geolocator = Nominatim(user_agent="GetLoc")
    try:
        location = geolocator.reverse((latitude, longitude), addressdetails=True)
        if location and 'address' in location.raw:
            address = location.raw['address']
            house_number = address.get('house_number', '')
            street = address.get('road', '')
            district = address.get('suburb', '') 
            country = address.get('country', '') 

            # Format the address correctly
            formatted_address = f"{house_number} {street}, {district}, {country}".strip()
            return formatted_address
        return "Unknown Location"
    except Exception as e:
        print(f"Geocoding error: {e}")
        return "Unknown Location"

def connect_mqtt():
    client = mqtt_client.Client(CLIENT_ID)
    client.on_connect = lambda client, userdata, flags, rc: print("Connected to MQTT Broker!" if rc == 0 else f"Failed to connect, code {rc}")
    client.tls_set(tls_version=ssl.PROTOCOL_TLS)
    client.connect(MQTT_BROKER, MQTT_PORT)
    return client

def send_email(bin_id):
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
            msg['Subject'] = f"No Data published for {bin_id}"
            # Email body
            body = f"No data has been published for the last {ALERT_DURATION_MINUTES} minutes for {bin_id}. Please Check Connection!"
            msg.attach(MIMEText(body, 'plain'))
            # Send the email
            s.sendmail(msg['From'], msg['To'], msg.as_string())
            # Terminate the session
            s.quit()
            print(f"Email sent for {bin_id}")
        except Exception as e:
            print(f"Error sending email alert: {e}")

def check_last_published_time():
    global last_published_time, current_bin_id
    while True:
        time_since_last_publish = datetime.now() - last_published_time
        if time_since_last_publish > timedelta(minutes=ALERT_DURATION_MINUTES) and current_bin_id:
            send_email(current_bin_id)
            last_published_time = datetime.now()  # Reset the timer after sending the email
        time.sleep(60)  # Check every minute

def read_serial_and_publish(client):
    global last_published_time
    while True:
        try:
            line = ser.readline().decode('utf-8').strip()
            if line:
                json_data = json.loads(line)
                json_data['timestamp'] = datetime.now(local_timezone).isoformat()

                latitude = json_data.get("Latitude")
                longitude = json_data.get("Longitude")

                if latitude is not None and longitude is not None:
                    # Get the address from latitude and longitude
                    json_data['Address'] = get_address_from_coordinates(latitude, longitude)
                else:
                    json_data['Address'] = "No Location Data"

                client.publish(MQTT_TOPIC, json.dumps(json_data), qos=1)
                last_published_time = datetime.now()
                print(f"Published to MQTT: {json_data}")
        except json.JSONDecodeError:
            print(f"Invalid JSON received: {line}")
        except Exception as e:
            print(f"Error: {e}")

def run():
    client = connect_mqtt()
    client.loop_start()

    threading.Thread(target=check_last_published_time, daemon=True).start()
    read_serial_and_publish(client)

if __name__ == '__main__':
    run()