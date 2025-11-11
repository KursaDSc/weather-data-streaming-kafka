#!/usr/bin/env python3
"""
Kafka Producer for Weather Data Streaming
This script fetches weather data from WeatherAPI and sends it to Kafka topic.
"""

import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherDataProducer:
    def __init__(self):
        """Initialize Kafka producer and configuration"""
        self.api_key = os.getenv('WEATHER_API_KEY')
        self.kafka_broker = os.getenv('KAFKA_BROKER_LOCAL', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'weather_data')
        self.city = "Amsterdam"
        
        # Validate required environment variables
        if not self.api_key:
            raise ValueError("WEATHER_API_KEY not found in environment variables")
        
        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.kafka_broker],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                retries=5,
                request_timeout_ms=30000
            )
            logger.info(f"✅ Kafka producer initialized successfully for broker: {self.kafka_broker}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka producer: {e}")
            raise
    
    def fetch_weather_data(self):
        """Fetch current weather data from WeatherAPI"""
        url = f"http://api.weatherapi.com/v1/current.json?key={self.api_key}&q={self.city}&aqi=no"
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            current_data = data.get('current', {})
            location_data = data.get('location', {})
            
            # Extract required fields
            weather_record = {
                'city': location_data.get('name', self.city),
                'temperature_c': current_data.get('temp_c'),
                'temperature_f': current_data.get('temp_f'),
                'humidity': current_data.get('humidity'),
                'wind_speed_kph': current_data.get('wind_kph'),
                'wind_speed_mph': current_data.get('wind_mph'),
                'local_time': location_data.get('localtime'),
                'last_updated': current_data.get('last_updated'),
                'timestamp': datetime.now().isoformat(),
                'condition': current_data.get('condition', {}).get('text', 'Unknown')
            }
            
            logger.info(f"🌤️ Weather data fetched for {weather_record['city']}: {weather_record['temperature_c']}°C")
            return weather_record
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error fetching weather data: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error parsing weather data: {e}")
            return None
    
    def send_to_kafka(self, weather_data):
        """Send weather data to Kafka topic"""
        if not weather_data:
            logger.warning("⚠️ No weather data to send")
            return False
        
        try:
            # Send data to Kafka
            future = self.producer.send(
                topic=self.topic,
                value=weather_data
            )
            
            # Wait for send to complete
            future.get(timeout=10)
            logger.info(f"📤 Weather data sent to Kafka topic: {self.topic}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error sending data to Kafka: {e}")
            return False
    
    def run(self):
        """Main loop to fetch and send weather data every 60 seconds"""
        logger.info("🚀 Starting Weather Data Producer...")
        logger.info(f"📍 Monitoring city: {self.city}")
        logger.info(f"⏰ Update interval: 60 seconds")
        logger.info("Press Ctrl+C to stop...")
        
        try:
            while True:
                # Fetch weather data
                weather_data = self.fetch_weather_data()
                
                if weather_data:
                    # Send to Kafka
                    self.send_to_kafka(weather_data)
                else:
                    logger.warning("⚠️ Skipping Kafka send due to missing weather data")
                
                # Wait for 60 seconds
                time.sleep(60)
                
        except KeyboardInterrupt:
            logger.info("🛑 Producer stopped by user")
        finally:
            # Close Kafka producer
            self.producer.close()
            logger.info("✅ Kafka producer closed")

def main():
    """Main function"""
    try:
        producer = WeatherDataProducer()
        producer.run()
    except Exception as e:
        logger.error(f"❌ Producer failed to start: {e}")

if __name__ == "__main__":
    main()