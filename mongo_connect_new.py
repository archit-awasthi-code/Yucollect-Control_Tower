import os
from pymongo import MongoClient
from pprint import pprint

# MongoDB connection details
MONGO_DB_URI = "mongodb+srv://qa_ihs_mongo_svc:cwUo8Zzy52CBhUhf@pl-0-ap-south-1.ax1ah.mongodb.net/qa_ihs"
MONGO_DB_USERNAME = "qa_ihs_mongo_svc"
MONGO_DB_NAME = "qa_ihs"

print(f"Connecting to MongoDB using URI: {MONGO_DB_URI}")

try:
    # Connect to MongoDB
    client = MongoClient(MONGO_DB_URI, serverSelectionTimeoutMS=10000)
    
    # Verify connection
    client.admin.command('ping')
    print("✅ Successfully connected to MongoDB!")
    
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
            
            # Show schema (fields and their types)
            print("\nSchema (fields and their types):")
            field_types = {}
            for field, value in sample.items():
                field_types[field] = type(value).__name__
            pprint(field_types)
    
except Exception as e:
    print(f"❌ Error connecting to MongoDB: {str(e)}")
finally:
    # Close the connection
    if 'client' in locals():
        client.close()
        print("\nMongoDB connection closed")
