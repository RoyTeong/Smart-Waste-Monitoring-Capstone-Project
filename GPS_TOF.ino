// Include necessary libraries
#include "Adafruit_VL53L1X.h"  // For VL53L1X distance sensor
#include <NMEAGPS.h>           // For GPS parsing
#include <NeoSWSerial.h>       // For software serial communication with GPS
#include <GPSport.h>           // GPS port definitions

// VL53L1X Pins and Object
#define IRQ_PIN 1      // Interrupt pin for VL53L1X
#define XSHUT_PIN 2    // Reset pin for VL53L1X
Adafruit_VL53L1X vl53 = Adafruit_VL53L1X(XSHUT_PIN, IRQ_PIN);  // Distance sensor object

// GPS Configuration
static const int RXPin = 4, TXPin = 3;  // GPS module RX/TX pins
static const uint32_t GPSBaud = 9600;   // GPS module baud rate
NMEAGPS gps;                            // GPS parser object
NeoSWSerial gpsSerial(RXPin, TXPin);    // Software serial for GPS
static gps_fix fix;                     // Stores GPS fix data

// Unique Bin Identifier
String BinID = "BIN_001";

// Timing variables
unsigned long previousMillis = 0;   // Stores last time data was collected
const unsigned long interval = 600000;  // 10 minute interval (600,000 ms)

// GPS status flag
bool locationRetrieved = false;  // Tracks if we've gotten a GPS fix

void setup() {
  Serial.begin(115200);  // Initialize serial for debugging output with baud rate of 115200

  // Initialize VL53L1X distance sensor
  Wire.begin();  // Start I2C communication
  Serial.println(F("Initializing VL53L1X sensor..."));
  if (!vl53.begin(0x29, &Wire)) {  // Initialize sensor at I2C address 0x29
    Serial.print(F("VL53L1X init failed: "));
    Serial.println(vl53.vl_status);
    while (1) delay(10);  // Halt if initialization fails
  }
  Serial.println(F("VL53L1X sensor initialized"));

  // Print sensor ID for verification
  Serial.print(F("Sensor ID: 0x"));
  Serial.println(vl53.sensorID(), HEX);

  // Start distance ranging
  if (!vl53.startRanging()) {
    Serial.print(F("Couldn't start ranging: "));
    Serial.println(vl53.vl_status);
    while (1) delay(10);  // Halt if ranging fails
  }
  Serial.println(F("Ranging started"));
  
  // Configure sensor timing
  vl53.startRanging();
  vl53.setTimingBudget(50);  // Set measurement timing budget to 50ms
  Serial.println(vl53.getTimingBudget());

  // Initialize GPS module
  Serial.println(F("Initializing GPS module..."));
  gpsSerial.begin(GPSBaud);  // Start software serial for GPS

  // Configuration checks for GPS library
  #ifndef NMEAGPS_RECOGNIZE_ALL
    #error You must define NMEAGPS_RECOGNIZE_ALL in NMEAGPS_cfg.h!
  #endif
  #ifdef NMEAGPS_INTERRUPT_PROCESSING
    #error You must *NOT* define NMEAGPS_INTERRUPT_PROCESSING in NMEAGPS_cfg.h!
  #endif
  
  Serial.println(F("GPS module initialized"));
}

void loop() {
  unsigned long currentMillis = millis();  // Get current time
  
  // Read distance and calculate fill level
  int16_t distance = vl53.distance();     // Get distance in mm
  int16_t capacity = 150;                 // Maximum capacity in mm
  double level = (double(capacity - distance) / double(capacity)) * 100.00;  // Calculate fill level in percentage
  String bin_status = "";                 // String to store bin status
  
  // Check if interval has passed for regular data collection
  if (currentMillis - previousMillis >= interval) {
    previousMillis = currentMillis;  // Reset timer
    
    // Check if distance sensor has new data
    if (vl53.dataReady()) {
      // Validate level reading
      if (level == -1 || level < 0.00 || level > 100.00) {
        Serial.print(F("Level error: "));
        Serial.println(vl53.vl_status);
      } else {
        // Determine bin status based on fill level
        if (level >= 80.00) {
          bin_status = "Full";
        }
        else if (level >= 50.00 && level < 80.00) {
          bin_status = "Partially Full";
        }
        else {
          bin_status = "Empty";
        }
        
        // Create JSON string with bin data
        String binLevelData = "{\"BinID\": \"" + String(BinID) + 
                          "\", \"bin_level\": " + String(level, 2) + 
                          ", \"bin_status\": \"" + String(bin_status) + "\"}";

        // Output JSON to serial
        Serial.println(binLevelData);
      }
      vl53.clearInterrupt();  // Clear sensor interrupt
    }
  }

  // Process GPS data if we haven't gotten a fix yet
  while (gps.available(gpsSerial) && !locationRetrieved) {
    fix = gps.read();  // Get latest GPS fix
    
    // Check if we have valid location data when GPS satelite signal is good in outdoors but poor in indoors
    if(fix.valid.location) {
      // Determine bin status (same logic as above)
      if (level >= 80.00) {
        bin_status = "Full";
      }
      else if (level >= 50.00 && level < 80.00) {
        bin_status = "Partially Full";
      }
      else {
        bin_status = "Empty";
      }
      
      // Create JSON string with location data
      String locationData = "{\"BinID\": \"" + String(BinID) + 
                      "\", \"Latitude\": " + String(fix.latitude(), 6) + 
                      ", \"Longitude\": " + String(fix.longitude(), 6) + 
                      ", \"bin_level\": " + String(level, 2) + 
                      ", \"bin_status\": \"" + String(bin_status) + "\"}";

      // Output JSON to serial
      Serial.println(locationData);
      locationRetrieved = true;  // Mark location as retrieved
    } else {
      // No valid GPS signal yet
      Serial.println("Invalid GPS signal, waiting for GPS fix");
    }
  }
}