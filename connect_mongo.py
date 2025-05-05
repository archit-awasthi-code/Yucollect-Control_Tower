from pymongo import MongoClient
from dotenv import load_dotenv
import os
import time
import dns.resolver

# Load environment variables
load_dotenv()

def connect_to_mongodb():
    try:
        # Set Google DNS servers
        dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
        dns.resolver.default_resolver.nameservers = ['8.8.8.8', '8.8.4.4']
        
        # MongoDB Atlas connection string format
        mongo_uri = f"mongodb+srv://{os.getenv('MONGO_DB_USER')}:{os.getenv('MONGO_DB_PASSWORD')}@{os.getenv('MONGO_DB_CLUSTER')}"
        
        print("Attempting to connect to MongoDB...")
        print(f"Database: {os.getenv('MONGO_DB_NAME')}")
        print(f"Cluster: {os.getenv('MONGO_DB_CLUSTER')}")
        print(f"Using Google DNS servers: 8.8.8.8, 8.8.4.4")
        
        # Create a new client and connect to the server
        client = MongoClient(
            mongo_uri,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000,
            socketTimeoutMS=10000
        )
        
        # Send a ping to confirm a successful connection
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
        
        # Get the database
        db = client[os.getenv('MONGO_DB_NAME')]
        
        # List all collections
        print("\nAvailable collections:")
        collections = db.list_collection_names()
        for collection in collections:
            count = db[collection].count_documents({})
            print(f"- {collection}: {count:,} documents")
        
        return client, db
        
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        print("\nTroubleshooting tips:")
        print("1. Try resolving the MongoDB cluster DNS:")
        try:
            answers = dns.resolver.resolve(os.getenv('MONGO_DB_CLUSTER'), 'A')
            print("\nDNS Resolution results:")
            for rdata in answers:
                print(f"IP Address: {rdata.address}")
        except Exception as dns_error:
            print(f"DNS Resolution error: {str(dns_error)}")
        return None, None

if __name__ == "__main__":
    client, db = connect_to_mongodb()
    if client:
        try:
            # Keep the connection open for a moment to see the results
            time.sleep(2)
        finally:
            client.close()
            print("\nMongoDB connection closed.")
