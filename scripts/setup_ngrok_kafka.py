#!/usr/bin/env python3
"""
Script to automatically setup Ngrok and update Kafka configuration
This script:
1. Starts Ngrok tunnel for Kafka
2. Updates docker-compose.yml with the new Ngrok address
3. Restarts Kafka container
"""

import subprocess
import time
import requests
import yaml
import re
from pathlib import Path
import os

def start_ngrok_tunnel(port=9093):
    """Start Ngrok tunnel for the specified port"""
    print("🚀 Starting Ngrok tunnel...")
    
    try:
        # Start Ngrok in background
        ngrok_process = subprocess.Popen(
            ['ngrok', 'tcp', str(port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print("✅ Ngrok process started")
        
        # Wait for Ngrok to initialize
        time.sleep(5)
        
        return ngrok_process
    except Exception as e:
        print(f"❌ Error starting Ngrok: {e}")
        return None

def get_ngrok_public_url():
    """Get the public URL from Ngrok API"""
    try:
        response = requests.get('http://localhost:4040/api/tunnels')
        data = response.json()
        
        for tunnel in data['tunnels']:
            if tunnel['proto'] == 'tcp':
                public_url = tunnel['public_url']
                print(f"✅ Ngrok Public URL: {public_url}")
                return public_url
        
        print("❌ No TCP tunnel found")
        return None
    except Exception as e:
        print(f"❌ Error getting Ngrok URL: {e}")
        return None

def update_docker_compose(ngrok_url):
    """Update docker-compose.yml with the new Ngrok address"""
    try:
        # Remove 'tcp://' prefix from ngrok URL
        ngrok_address = ngrok_url.replace('tcp://', '')
        
        compose_file = Path('docker-compose.yml')
        if not compose_file.exists():
            print("❌ docker-compose.yml not found")
            return False
        
        # Read the current content
        with open(compose_file, 'r') as file:
            content = file.read()
        
        # Update the KAFKA_ADVERTISED_LISTENERS
        old_pattern = r'KAFKA_ADVERTISED_LISTENERS:\s*.+'
        new_listeners = f'KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092,EXTERNAL://{ngrok_address}'
        
        updated_content = re.sub(old_pattern, new_listeners, content)
        
        # Write back the updated content
        with open(compose_file, 'w') as file:
            file.write(updated_content)
        
        print(f"✅ Updated docker-compose.yml with: EXTERNAL://{ngrok_address}")
        return True
        
    except Exception as e:
        print(f"❌ Error updating docker-compose.yml: {e}")
        return False

def restart_kafka():
    """Restart Kafka container"""
    try:
        print("🔄 Restarting Kafka container...")
        
        # Stop and remove existing containers
        subprocess.run(['docker-compose', 'down'], check=True)
        
        # Start services again
        subprocess.run(['docker-compose', 'up', '-d'], check=True)
        
        print("✅ Kafka container restarted successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error restarting Kafka: {e}")
        return False

def main():
    """Main function to setup Ngrok and Kafka"""
    print("🎯 Starting automated Ngrok + Kafka setup...")
    
    # Step 1: Start Ngrok
    ngrok_process = start_ngrok_tunnel()
    if not ngrok_process:
        return
    
    # Step 2: Get public URL
    public_url = get_ngrok_public_url()
    if not public_url:
        ngrok_process.terminate()
        return
    
    # Step 3: Update docker-compose
    if not update_docker_compose(public_url):
        ngrok_process.terminate()
        return
    
    # Step 4: Restart Kafka
    if not restart_kafka():
        ngrok_process.terminate()
        return
    
    print("\n🎉 Setup completed successfully!")
    print(f"📊 Kafka is now accessible at: {public_url}")
    print("🔗 Use this address in Microsoft Fabric")
    
    try:
        # Keep the script running to maintain Ngrok tunnel
        print("\n🔄 Ngrok tunnel is active. Press Ctrl+C to stop...")
        ngrok_process.wait()
    except KeyboardInterrupt:
        print("\n🛑 Stopping Ngrok...")
        ngrok_process.terminate()
        print("✅ Ngrok stopped")

if __name__ == "__main__":
    main()