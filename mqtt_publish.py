# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Import required libraries
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
MQTT_BROKER = "broker.emqx.io"  # MQTT broker address
MQTT_PORT = 8883  # MQTT broker port (SSL)
MQTT_TOPIC = "sensor/data"  # Topic to publish sensor data to
CLIENT_ID = f"mqtt_client_{socket.gethostname()}"  # Unique client ID based on hostname

# Get email alert recipients from environment variables
alert_recipients = [email.strip() for email in os.getenv("ALERT_RECIPIENTS_NO_DATA", "").split(",")]

# Email server configuration from environment variables
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = os.getenv("SMTP_PORT")

# Duration after which to send alert if no data is received (in minutes)
ALERT_DURATION_MINUTES = 20

# Initialize Serial Connection to COM3 at 115200 baud with 30 minute timeout
# Replace COM3 with the respective Serial Port that you've set it in Arduino IDE.
ser = serial.Serial('COM3', 115200, timeout=1800)

# Timezone Handling - get the local timezone
local_timezone = get_localzone()

# Variable to track the last published time
last_published_time = datetime.now()

# Variable to store the current BinID from environment variables
current_bin_id = os.getenv("BinID")

def get_address_from_coordinates(latitude, longitude):
    """
    Convert latitude and longitude coordinates to a human-readable address using Nominatim geocoding service.
    
    Args:
        latitude: Latitude coordinate
        longitude: Longitude coordinate
    
    Returns:
        Formatted address string or "Unknown Location" if geocoding fails
    """
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
    """
    Connect to the MQTT broker with SSL/TLS encryption.
    
    Returns:
        MQTT client object
    """
    client = mqtt_client.Client(CLIENT_ID)
    # Define callback for connection events
    client.on_connect = lambda client, userdata, flags, rc: print("Connected to MQTT Broker!" if rc == 0 else f"Failed to connect, code {rc}")
    # Set up TLS encryption
    client.tls_set(tls_version=ssl.PROTOCOL_TLS)
    client.connect(MQTT_BROKER, MQTT_PORT)
    return client

def send_email(bin_id):
    """
    Send an alert email when no data has been received for the specified duration.
    
    Args:
        bin_id: The ID of the bin that's not sending data
    """
    for dest in alert_recipients:
        try:
            # Create an SMTP session
            s = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
            s.starttls()  # Enable TLS encryption
            s.login(SMTP_USER, SMTP_PASS)  # Authenticate with SMTP server
            
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
    """
    Background thread that checks if data hasn't been published in the specified duration.
    Sends an email alert if no data has been published for ALERT_DURATION_MINUTES.
    """
    global last_published_time, current_bin_id
    while True:
        time_since_last_publish = datetime.now() - last_published_time
        if time_since_last_publish > timedelta(minutes=ALERT_DURATION_MINUTES) and current_bin_id:
            send_email(current_bin_id)
            last_published_time = datetime.now()  # Reset the timer after sending the email
        time.sleep(60)  # Check every minute

def read_serial_and_publish(client):
    """
    Main function that reads from serial port, processes the data, and publishes to MQTT.
    
    Args:
        client: MQTT client object
    """
    global last_published_time
    while True:
        try:
            # Read a line from serial port and decode it
            line = ser.readline().decode('utf-8').strip()
            if line:
                # Parse the JSON data
                json_data = json.loads(line)
                # Add current timestamp with timezone information
                json_data['timestamp'] = datetime.now(local_timezone).isoformat()

                # Get GPS coordinates if available
                latitude = json_data.get("Latitude")
                longitude = json_data.get("Longitude")

                if latitude is not None and longitude is not None:
                    # Get the address from latitude and longitude
                    json_data['Address'] = get_address_from_coordinates(latitude, longitude)
                else:
                    json_data['Address'] = "No Location Data"

                # Publish the data to MQTT broker
                client.publish(MQTT_TOPIC, json.dumps(json_data), qos=1)
                # Update last published time
                last_published_time = datetime.now()
                print(f"Published to MQTT: {json_data}")
        except json.JSONDecodeError:
            print(f"Invalid JSON received: {line}")
        except Exception as e:
            print(f"Error: {e}")

def run():
    """
    Main function that starts the MQTT client and all threads.
    """
    # Connect to MQTT broker
    client = connect_mqtt()
    client.loop_start()  # Start the MQTT network loop

    # Start the background thread that checks for missing data
    threading.Thread(target=check_last_published_time, daemon=True).start()
    
    # Start the main serial reading and publishing loop
    read_serial_and_publish(client)

if __name__ == '__main__':
    run() # Start application