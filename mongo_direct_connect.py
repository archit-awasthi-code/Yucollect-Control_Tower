import os
from pymongo import MongoClient
from pprint import pprint
import dns.resolver
import socket

# MongoDB connection details
MONGO_DB_NAME = "qa_ihs"
MONGO_DB_USER = "qa_ihs_mongo_svc"
MONGO_DB_PASSWORD = "cwUo8Zzy52CBhUhf"
MONGO_DB_CLUSTER = "nonprd-yucollect-cluste.ax1ah.mongodb.net"

# Try different connection methods
print("Attempting to connect to MongoDB using multiple methods...")

# Method 1: Direct connection with SRV record
try:
    print("\nMethod 1: Direct connection with SRV record")
    mongo_uri = f"mongodb+srv://{MONGO_DB_USER}:{MONGO_DB_PASSWORD}@{MONGO_DB_CLUSTER}/?retryWrites=true&w=majority"
    print(f"URI: {mongo_uri}")
    
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    print("✅ Connection successful!")
    
    # Get database and list collections
    db = client[MONGO_DB_NAME]
    collections = db.list_collection_names()
    print(f"Collections in {MONGO_DB_NAME}: {collections}")
    client.close()
except Exception as e:
    print(f"❌ Connection failed: {str(e)}")

# Method 2: Try with direct connection string (no SRV)
try:
    print("\nMethod 2: Direct connection without SRV")
    mongo_uri = f"mongodb://{MONGO_DB_USER}:{MONGO_DB_PASSWORD}@{MONGO_DB_CLUSTER}:27017/{MONGO_DB_NAME}?retryWrites=true&w=majority"
    print(f"URI: {mongo_uri}")
    
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    print("✅ Connection successful!")
    
    # Get database and list collections
    db = client[MONGO_DB_NAME]
    collections = db.list_collection_names()
    print(f"Collections in {MONGO_DB_NAME}: {collections}")
    client.close()
except Exception as e:
    print(f"❌ Connection failed: {str(e)}")

# Method 3: Try with IP address if we can resolve it
try:
    print("\nMethod 3: Attempting to resolve hostname to IP")
    try:
        ip_address = socket.gethostbyname(MONGO_DB_CLUSTER)
        print(f"Resolved {MONGO_DB_CLUSTER} to IP: {ip_address}")
        
        mongo_uri = f"mongodb://{MONGO_DB_USER}:{MONGO_DB_PASSWORD}@{ip_address}:27017/{MONGO_DB_NAME}?retryWrites=true&w=majority"
        print(f"URI: {mongo_uri}")
        
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("✅ Connection successful!")
        
        # Get database and list collections
        db = client[MONGO_DB_NAME]
        collections = db.list_collection_names()
        print(f"Collections in {MONGO_DB_NAME}: {collections}")
        client.close()
    except socket.gaierror:
        print(f"❌ Could not resolve hostname {MONGO_DB_CLUSTER} to IP address")
except Exception as e:
    print(f"❌ Connection failed: {str(e)}")

# Method 4: Try with the http:// URL format converted to mongodb://
try:
    print("\nMethod 4: Using http:// URL format converted to mongodb://")
    cluster_url = "http://nonprd-yucollect-cluste.ax1ah.mongodb.net/"
    cluster_host = cluster_url.replace("http://", "").replace("/", "")
    
    mongo_uri = f"mongodb+srv://{MONGO_DB_USER}:{MONGO_DB_PASSWORD}@{cluster_host}/?retryWrites=true&w=majority"
    print(f"URI: {mongo_uri}")
    
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    print("✅ Connection successful!")
    
    # Get database and list collections
    db = client[MONGO_DB_NAME]
    collections = db.list_collection_names()
    print(f"Collections in {MONGO_DB_NAME}: {collections}")
    client.close()
except Exception as e:
    print(f"❌ Connection failed: {str(e)}")

print("\nAll connection methods attempted. If none succeeded, please check your network configuration or VPN settings.")
