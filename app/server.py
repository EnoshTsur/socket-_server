import socket
import time
import json
from faker import Faker
import random

def create_server():
    # Initialize Faker
    fake = Faker()
    
    # Create server socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('localhost', 9999))
    server_socket.listen(1)
    
    print("Waiting for Spark to connect...")
    
    try:
        while True:
            client_socket, address = server_socket.accept()
            print(f"Spark connected from {address}")
            
            try:
                while True:  # Continuously send data
                    # Generate random user data
                    user = {
                        'name': fake.name(),
                        'email': fake.email(),
                        'age': random.randint(18, 80),
                        'city': fake.city(),
                        'job': fake.job()
                    }
                    
                    # Convert to JSON string and add newline
                    json_data = json.dumps(user) + '\n'
                    print(f"Sending: {json_data.strip()}")
                    
                    # Send the data
                    client_socket.send(json_data.encode('utf-8'))
                    time.sleep(1)  # Wait 1 second between messages
                    
            except Exception as e:
                print(f"Error: {e}")
                client_socket.close()
                
    except KeyboardInterrupt:
        print("Shutting down server...")
    finally:
        server_socket.close()

if __name__ == "__main__":
    create_server()