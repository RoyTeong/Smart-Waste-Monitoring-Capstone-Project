#include "Adafruit_VL53L1X.h"
#include <NMEAGPS.h>
#include <NeoSWSerial.h>
#include <GPSport.h>

// VL53L1X Pins and Object
#define IRQ_PIN 1
#define XSHUT_PIN 2
Adafruit_VL53L1X vl53 = Adafruit_VL53L1X(XSHUT_PIN, IRQ_PIN);

// GPS Pins and Baud Rate
static const int RXPin = 4, TXPin = 3; // Adjusted GPS RX/TX for compatibility
static const uint32_t GPSBaud = 9600;
NMEAGPS gps;
NeoSWSerial gpsSerial(RXPin, TXPin);
static gps_fix fix;


String BinID = "BIN_001";

unsigned long previousMillis = 0; // Stores the last time data was collected
const unsigned long interval = 600000;

bool locationRetrieved = false;

void setup() {
  Serial.begin(115200); // Debugging output

  // Setup VL53L1X
  Wire.begin();
  Serial.println(F("Initializing VL53L1X sensor..."));
  if (!vl53.begin(0x29, &Wire)) {
    Serial.print(F("VL53L1X init failed: "));
    Serial.println(vl53.vl_status);
    while (1) delay(10);
  }
  Serial.println(F("VL53L1X sensor initialized"));

  Serial.print(F("Sensor ID: 0x"));
  Serial.println(vl53.sensorID(), HEX);

  if (! vl53.startRanging()) {
    Serial.print(F("Couldn't start ranging: "));
    Serial.println(vl53.vl_status);
    while (1)
    {
      delay(10);
    }
  }
  Serial.println(F("Ranging started"));
  
  vl53.startRanging();
  vl53.setTimingBudget(50);
  Serial.println(vl53.getTimingBudget());

  // Setup GPS
  Serial.println(F("Initializing GPS module..."));
  gpsSerial.begin(GPSBaud);

  #ifndef NMEAGPS_RECOGNIZE_ALL
    #error You must define NMEAGPS_RECOGNIZE_ALL in NMEAGPS_cfg.h!
  #endif
  #ifdef NMEAGPS_INTERRUPT_PROCESSING
    #error You must *NOT* define NMEAGPS_INTERRUPT_PROCESSING in NMEAGPS_cfg.h!
  #endif
  Serial.println(F("GPS module initialized"));
}

void loop() {
  // Current time in milliseconds
  unsigned long currentMillis = millis();
  int16_t distance = vl53.distance();
  int16_t capacity = 150;
  double level = (double(capacity - distance) / double(capacity)) * 100.00;
  String bin_status = "";
  
  // Check if 2 hours have passed since the last data collection
  if (currentMillis - previousMillis >= interval) {
    previousMillis = currentMillis; // Update the time of the last data collection
    
    
    // VL53L1X Ranging
    if (vl53.dataReady()) {
      
      if (level == -1 || level < 0.00 || level > 100.00) {
        Serial.print(F("Level error: "));
        Serial.println(vl53.vl_status);
      } else {
        if (level >= 80.00)
        {
          bin_status = "Full";
        }
        else if (level >= 50.00 && level < 80.00)
        {
          bin_status = "Partially Full";
        }
        else
        {
          bin_status = "Empty";
        }
        
        String binLevelData = "{\"BinID\": \"" + String(BinID) + 
                      "\", \"bin_level\": " + String(level, 2) + 
                      ", \"bin_status\": \"" + String(bin_status) + "\"}";


        Serial.println(binLevelData);

        /*Serial.print(F("Distance: "));
        Serial.print(distance);
        Serial.println(" mm, ");
        Serial.print(F("Bin Level: "));
        Serial.print(level);
        Serial.println(" %");*/
      }
      vl53.clearInterrupt();
    }
  }

  // GPS Data Parsing
  while (gps.available(gpsSerial)&& !locationRetrieved) {
    fix = gps.read();
    
    if(fix.valid.location)
    {

      if (level >= 80.00)
        {
          bin_status = "Full";
        }
        else if (level >= 50.00 && level < 80.00)
        {
          bin_status = "Partially Full";
        }
        else
        {
          bin_status = "Empty";
        }
        
      String locationData = "{\"BinID\": \"" + String(BinID) + 
                      "\", \"Latitude\": " + String(fix.latitude(), 6) + 
                      ", \"Longitude\": " + String(fix.longitude(), 6) + 
                      ", \"bin_level\": " + String(level, 2) + 
                      ", \"bin_status\": \"" + String(bin_status) + "\"}";


      Serial.println(locationData);
      locationRetrieved = true;
    }
      /*
      Serial.print(F("Latitude: "));
      Serial.print(fix.latitude(), 6);
      Serial.print(F(", Longitude: "));
      Serial.println(fix.longitude(), 6);*/
    
    else
    {
        Serial.println("Invalid GPS signal, waiting for GPS fix");
    }
    
    
    
  }
}