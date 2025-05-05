import pymongo
from pymongo import MongoClient
import pprint
import os
import sys
import time

# MongoDB direct URI
mongo_uri = "mongodb+srv://qa_ihs_mongo_svc:cwUo8Zzy52CBhUhf@nonprd-yucollect-cluster-pl-0.ax1ah.mongodb.net/qa_ihs"

print("Attempting to connect to MongoDB using direct URI...")
try:
    # Set a short timeout for connection attempts
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    
    # Force a connection attempt
    print("Pinging server...")
    client.admin.command('ping')
    
    print("✓ Successfully connected to MongoDB!")
    
    # Access the database
    db = client.get_database("qa_ihs")
    
    # Get and print all collection names
    print("\n== COLLECTIONS IN DATABASE ==")
    collections = db.list_collection_names()
    for i, collection_name in enumerate(collections):
        print(f"{i+1}. {collection_name}")
    
    # For each collection, get sample documents and analyze structure
    print("\n== SAMPLE DOCUMENTS AND POTENTIAL METRICS ==")
    for collection_name in collections[:5]:  # Limit to first 5 collections for brevity
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
        
        # Suggest potential metrics based on the collection name and sample data
        print("\nPotential metrics:")
        
        # Generic metrics based on collection presence
        print(f"- {collection_name} count over time")
        
        # Identify potential metric fields based on common field names
        if sample:
            # Look for timestamp/date fields for time-series analysis
            time_fields = [k for k, v in sample.items() 
                          if any(time_word in k.lower() for time_word in ['time', 'date', 'created', 'updated', 'timestamp'])]
            if time_fields:
                print(f"- Time-series analysis based on fields: {', '.join(time_fields)}")
            
            # Look for numeric fields for quantity metrics
            numeric_fields = [k for k, v in sample.items() 
                             if isinstance(v, (int, float)) and not k.startswith('_')]
            if numeric_fields:
                print(f"- Quantity metrics using fields: {', '.join(numeric_fields)}")
            
            # Look for status or category fields for distribution metrics
            status_fields = [k for k, v in sample.items() 
                            if any(status_word in k.lower() for status_word in ['status', 'state', 'type', 'category'])]
            if status_fields:
                print(f"- Distribution analysis based on fields: {', '.join(status_fields)}")
    
    print("\n\n== SUMMARY OF POTENTIAL PRODUCT METRICS ==")
    print("Based on the database exploration, here are potential product metrics categories:")
    print("1. Volume metrics - counts and totals across collections")
    print("2. Time-based metrics - trends and patterns over time")
    print("3. Status distribution metrics - breakdowns by status, type, etc.")
    print("4. Performance metrics - processing times, throughput, etc.")
    print("5. Relationship metrics - connections between different data entities")
    
except Exception as e:
    print(f"✗ Failed to connect to MongoDB")
    print(f"  Error: {str(e)}")
    
print("\nConnection test complete.")
