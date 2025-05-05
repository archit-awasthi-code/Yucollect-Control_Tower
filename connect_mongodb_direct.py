import pymongo
from pymongo import MongoClient
import pprint
import os
import sys
import socket
import time

# MongoDB credentials from .env file
username = "qa_ihs_mongo_svc"
password = "cwUo8Zzy52CBhUhf"
db_name = "qa_ihs"

# Try different connection methods
connection_methods = [
    {
        "name": "Direct connection with IP",
        "connection_string": f"mongodb://{username}:{password}@20.204.235.231:27017/{db_name}?authSource=admin"
    },
    {
        "name": "Direct connection with IP (alternative port)",
        "connection_string": f"mongodb://{username}:{password}@20.204.235.231:27018/{db_name}?authSource=admin"
    },
    {
        "name": "Direct connection without DNS",
        "connection_string": f"mongodb://{username}:{password}@nonprd-yucollect-cluster-pl-0.ax1ah.mongodb.net:27017/{db_name}?authSource=admin"
    },
    {
        "name": "Standard MongoDB Atlas connection",
        "connection_string": f"mongodb+srv://{username}:{password}@nonprd-yucollect-cluster-pl-0.ax1ah.mongodb.net/{db_name}?retryWrites=true&w=majority"
    }
]

# Try each connection method
for method in connection_methods:
    print(f"\n\nTrying connection method: {method['name']}")
    print(f"Connection string: {method['connection_string']}")
    
    try:
        # Set a short timeout for connection attempts
        client = MongoClient(method['connection_string'], serverSelectionTimeoutMS=5000)
        
        # Force a connection attempt
        print("Attempting to connect...")
        client.admin.command('ping')
        
        print(f"✓ Successfully connected to MongoDB using {method['name']}")
        
        # Access the database
        db = client[db_name]
        
        # Get and print all collection names
        print("\n== COLLECTIONS IN DATABASE ==")
        collections = db.list_collection_names()
        for i, collection_name in enumerate(collections):
            print(f"{i+1}. {collection_name}")
        
        # For each collection, get sample documents and analyze structure
        print("\n== SAMPLE DOCUMENTS AND POTENTIAL METRICS ==")
        for collection_name in collections[:3]:  # Limit to first 3 collections for brevity
            print(f"\n=== COLLECTION: {collection_name} ===")
            
            # Count documents
            count = db[collection_name].count_documents({})
            print(f"Total documents: {count}")
            
            # Skip if empty collection
            if count == 0:
                print("Empty collection, skipping...")
                continue
            
            # Get one sample document to understand schema
            sample = db[collection_name].find_one()
            
            print("\nSample document structure:")
            if sample:
                # Just print the keys for top-level fields
                for key, value in sample.items():
                    if isinstance(value, dict):
                        nested_keys = ", ".join(list(value.keys())[:5])
                        print(f"  {key}: {{{nested_keys}{'...' if len(value.keys()) > 5 else ''}}}")
                    elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                        if len(value) > 0:
                            nested_keys = ", ".join(list(value[0].keys())[:5])
                            print(f"  {key}: [{{{nested_keys}{'...' if len(value[0].keys()) > 5 else ''}}}, ...]")
                        else:
                            print(f"  {key}: []")
                    else:
                        print(f"  {key}: {type(value).__name__}")
        
        # Successfully connected, no need to try other methods
        sys.exit(0)
        
    except Exception as e:
        print(f"✗ Failed to connect using {method['name']}")
        print(f"  Error: {str(e)}")
        
print("\n\nAll connection methods failed. Please check your VPN connection or MongoDB credentials.")
