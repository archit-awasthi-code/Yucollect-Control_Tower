from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_mongo_client():
    try:
        # Get MongoDB connection details from environment variables
        db_name = os.getenv('MONGO_DB_NAME')
        user = os.getenv('MONGO_DB_USER')
        password = os.getenv('MONGO_DB_PASSWORD')
        cluster = os.getenv('MONGO_DB_CLUSTER')
        
        # Create MongoDB connection URI for Atlas
        uri = f"mongodb+srv://{user}:{password}@{cluster}/?retryWrites=true&w=majority"
        
        print(f"Connecting to MongoDB database: {db_name}")
        print(f"Cluster: {cluster}")
        
        # Connect with server selection timeout
        client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        
        # Test the connection
        db = client[db_name]
        # Force a connection to verify it works
        client.server_info()
        
        print("\nConnection successful! Listing collections...")
        collections = db.list_collection_names()
        print("\nAvailable collections:")
        for collection in collections:
            print(f"- {collection}")
            # Get sample document count
            count = db[collection].count_documents({})
            print(f"  Documents: {count}")
            # Get sample document
            if count > 0:
                sample = db[collection].find_one()
                print(f"  Sample fields: {list(sample.keys())}")
        
        return client
        
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        return None

if __name__ == "__main__":
    client = get_mongo_client()
    if client:
        print("\nMongoDB connection successful!")
        client.close()
