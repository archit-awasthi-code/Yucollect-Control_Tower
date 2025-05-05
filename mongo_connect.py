import os
from pymongo import MongoClient
from pprint import pprint

# MongoDB connection details
MONGO_DB_NAME = "qa_ihs"
MONGO_DB_USER = "qa_ihs_mongo_svc"
MONGO_DB_PASSWORD = "cwUo8Zzy52CBhUhf"
MONGO_DB_CLUSTER = "http://nonprd-yucollect-cluste.ax1ah.mongodb.net/"

# Construct the MongoDB URI
mongo_uri = f"mongodb+srv://{MONGO_DB_USER}:{MONGO_DB_PASSWORD}@{MONGO_DB_CLUSTER.replace('http://', '')}{MONGO_DB_NAME}?retryWrites=true&w=majority"

try:
    # Connect to MongoDB
    print(f"Connecting to MongoDB database: {MONGO_DB_NAME}...")
    print(f"Using URI: {mongo_uri}")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    
    # Verify connection
    client.admin.command('ping')
    print("Successfully connected to MongoDB!")
    
    # Get database
    db = client[MONGO_DB_NAME]
    
    # List all collections
    print("\nCollections in the database:")
    collections = db.list_collection_names()
    for i, collection in enumerate(collections, 1):
        print(f"{i}. {collection}")
    
    # Ask user which collection to explore
    if collections:
        print("\nWould you like to explore a specific collection? Enter the number or name (or 'exit' to quit):")
        choice = input("> ")
        
        if choice.lower() != 'exit':
            try:
                # Try to interpret as a number
                if choice.isdigit() and 1 <= int(choice) <= len(collections):
                    collection_name = collections[int(choice) - 1]
                else:
                    # Use as a collection name
                    collection_name = choice
                
                # Get the collection
                collection = db[collection_name]
                
                # Count documents
                doc_count = collection.count_documents({})
                print(f"\nCollection '{collection_name}' has {doc_count} documents")
                
                # Show sample document if available
                if doc_count > 0:
                    print("\nSample document structure:")
                    sample = collection.find_one()
                    pprint(sample)
                    
                    # Show schema (fields and their types)
                    print("\nSchema (fields and their types):")
                    field_types = {}
                    for field, value in sample.items():
                        field_types[field] = type(value).__name__
                    pprint(field_types)
            except Exception as e:
                print(f"Error exploring collection: {str(e)}")
    
except Exception as e:
    print(f"Error connecting to MongoDB: {str(e)}")
finally:
    # Close the connection
    if 'client' in locals():
        client.close()
        print("\nMongoDB connection closed")
