import os
import sys
import socket
import subprocess
import time
from pymongo import MongoClient
from pprint import pprint
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB connection details
MONGO_DB_URI = "mongodb+srv://qa_ihs_mongo_svc:cwUo8Zzy52CBhUhf@nonprd-yucollect-cluster-pl-0.ax1ah.mongodb.net/qa_ihs"
MONGO_DB_NAME = "qa_ihs"

def check_dns_resolution(hostname):
    """Check if a hostname can be resolved to an IP address"""
    try:
        print(f"Attempting to resolve hostname: {hostname}")
        ip_address = socket.gethostbyname(hostname)
        print(f"✅ Successfully resolved {hostname} to {ip_address}")
        return True
    except socket.gaierror:
        print(f"❌ Failed to resolve hostname: {hostname}")
        return False

def check_port_connectivity(hostname, port):
    """Check if a port is open on a hostname"""
    try:
        print(f"Checking connectivity to {hostname}:{port}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((hostname, port))
        sock.close()
        
        if result == 0:
            print(f"✅ Port {port} is open on {hostname}")
            return True
        else:
            print(f"❌ Port {port} is closed on {hostname}")
            return False
    except socket.gaierror:
        print(f"❌ Failed to resolve hostname: {hostname}")
        return False

def try_direct_connection():
    """Try to connect directly to MongoDB"""
    print("\n=== Attempting Direct Connection ===")
    try:
        print(f"Connecting to MongoDB using URI: {MONGO_DB_URI}")
        client = MongoClient(MONGO_DB_URI, serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        print("✅ Successfully connected to MongoDB!")
        return client
    except Exception as e:
        print(f"❌ Direct connection failed: {str(e)}")
        return None

def try_connection_with_dns_override():
    """Try to connect with manual DNS resolution"""
    print("\n=== Attempting Connection with DNS Override ===")
    hostname = "nonprd-yucollect-cluster-pl-0.ax1ah.mongodb.net"
    
    # Try to manually resolve the hostname using nslookup
    try:
        print(f"Attempting to resolve {hostname} using nslookup...")
        result = subprocess.run(['nslookup', hostname], capture_output=True, text=True, timeout=10)
        print(result.stdout)
        
        # Extract IP addresses from nslookup output
        ip_addresses = []
        for line in result.stdout.splitlines():
            if "Address:" in line and not "10.250.9.1" in line:  # Exclude the DNS server address
                ip = line.split("Address:")[1].strip()
                ip_addresses.append(ip)
        
        if ip_addresses:
            print(f"Found IP addresses: {ip_addresses}")
            
            # Try connecting using the IP address
            for ip in ip_addresses:
                try:
                    print(f"Attempting to connect using IP: {ip}")
                    uri = f"mongodb://{os.getenv('MONGO_DB_USER')}:{os.getenv('MONGO_DB_PASSWORD')}@{ip}:27017/{MONGO_DB_NAME}"
                    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
                    client.admin.command('ping')
                    print("✅ Successfully connected to MongoDB using IP address!")
                    return client
                except Exception as e:
                    print(f"❌ Connection failed using IP {ip}: {str(e)}")
        else:
            print("❌ No IP addresses found in nslookup output")
    except Exception as e:
        print(f"❌ Error during nslookup: {str(e)}")
    
    return None

def connect_to_mongodb():
    """Try multiple methods to connect to MongoDB"""
    # Check DNS resolution
    hostname = "nonprd-yucollect-cluster-pl-0.ax1ah.mongodb.net"
    check_dns_resolution(hostname)
    
    # Try direct connection
    client = try_direct_connection()
    if client:
        return client
    
    # Try connection with DNS override
    client = try_connection_with_dns_override()
    if client:
        return client
    
    print("\n❌ All connection attempts failed")
    return None

def main():
    print("=== MongoDB Connection Diagnostic Tool ===")
    
    # Try to connect to MongoDB
    client = connect_to_mongodb()
    
    if client:
        try:
            # Get database
            db = client[MONGO_DB_NAME]
            
            # List all collections
            print("\nCollections in the database:")
            collections = db.list_collection_names()
            for i, collection in enumerate(collections, 1):
                print(f"{i}. {collection}")
            
            # Show document count for each collection
            print("\nDocument counts:")
            for collection_name in collections:
                count = db[collection_name].count_documents({})
                print(f"{collection_name}: {count} documents")
            
            # Check if borrower_details collection exists
            if "borrower_details" in collections:
                print("\nExamining borrower_details collection:")
                borrower_collection = db["borrower_details"]
                count = borrower_collection.count_documents({})
                print(f"Total documents: {count}")
                
                # Get a sample document
                if count > 0:
                    print("\nSample document structure:")
                    sample = borrower_collection.find_one()
                    pprint(sample)
        except Exception as e:
            print(f"Error working with MongoDB: {str(e)}")
        finally:
            client.close()
            print("\nMongoDB connection closed")
    
    print("\n=== Connectivity Recommendations ===")
    print("1. Check if you need to be on a specific VPN to access this MongoDB server")
    print("2. Verify with your IT team that the MongoDB server is operational")
    print("3. Confirm that your network allows outbound connections to MongoDB (port 27017)")
    print("4. Consider using an SSH tunnel if direct connection is not possible")
    print("5. In the meantime, you can use the PostgreSQL databases that are accessible")

if __name__ == "__main__":
    main()
