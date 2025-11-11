#!/usr/bin/env python3
"""
Kafka Consumer for Weather Data - Wind Speed Monitor
Continuous real-time monitoring version
"""

import json
import logging
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherDataConsumer:
    def __init__(self):
        """Initialize Kafka consumer for continuous operation"""
        self.kafka_broker = os.getenv('KAFKA_BROKER_LOCAL', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'weather_data')
        
        # Initialize Kafka consumer with proper settings for continuous operation
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=[self.kafka_broker],
                auto_offset_reset='latest',  # Only new messages
                enable_auto_commit=True,
                group_id='weather_consumer_group_continuous',  # New group ID
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                # No timeout - run continuously
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_interval_ms=300000
            )
            logger.info(f"✅ Kafka consumer initialized successfully")
            logger.info(f"📡 Listening to topic: {self.topic}")
            logger.info(f"🔗 Broker: {self.kafka_broker}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka consumer: {e}")
            raise
    
    def display_wind_speed(self, weather_data, message_count):
        """Display wind speed information in a formatted way"""
        try:
            city = weather_data.get('city', 'Unknown')
            wind_speed_kph = weather_data.get('wind_speed_kph')
            wind_speed_mph = weather_data.get('wind_speed_mph')
            temperature = weather_data.get('temperature_c')
            humidity = weather_data.get('humidity')
            timestamp = weather_data.get('timestamp', 'Unknown')
            last_updated = weather_data.get('last_updated', 'Unknown')
            
            if wind_speed_kph is not None:
                print(f"\n🎯 REAL-TIME UPDATE #{message_count}")
                print(f"   📍 City: {city}")
                print(f"   🌡️  Temperature: {temperature}°C")
                print(f"   💧 Humidity: {humidity}%")
                print(f"   💨 Wind Speed: {wind_speed_kph} km/h ({wind_speed_mph} mph)")
                print(f"   ⏰ Local Time: {timestamp}")
                print(f"   🔄 Last Updated: {last_updated}")
                print(f"   {'='*60}")
            else:
                logger.warning("⚠️ Wind speed data missing in message")
                
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
    
    def start_consuming(self):
        """Start continuous consumption of messages from Kafka"""
        logger.info("🚀 Starting Weather Data Consumer...")
        logger.info("📊 Monitoring wind speed in real-time")
        logger.info("🔄 Consumer will run CONTINUOUSLY until manually stopped")
        logger.info("⏰ Waiting for new weather data messages...")
        logger.info("Press Ctrl+C to stop...")
        
        message_count = 0
        
        try:
            for message in self.consumer:
                message_count += 1
                weather_data = message.value
                
                logger.info(f"📥 New message received (#{message_count}) from {weather_data.get('city', 'Unknown')}")
                
                # Display wind speed information
                self.display_wind_speed(weather_data, message_count)
                
        except KeyboardInterrupt:
            logger.info("🛑 Consumer stopped by user")
        except Exception as e:
            logger.error(f"❌ Error in consumer: {e}")
        finally:
            self.consumer.close()
            logger.info("✅ Kafka consumer closed")

def main():
    """Main function"""
    try:
        consumer = WeatherDataConsumer()
        consumer.start_consuming()
    except Exception as e:
        logger.error(f"❌ Consumer failed to start: {e}")

if __name__ == "__main__":
    main()