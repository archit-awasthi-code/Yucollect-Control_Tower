from pymongo import MongoClient

# Connection details
username = "qa_ihs_mongo_svc"
password = "cwUo8Zzy52CBhUhf"
host = "nonprd-yucollect-cluster-pl-0.ax1ah.mongodb.net"
db_name = "qa_ihs"

# Try different connection string formats
connection_strings = [
    f"mongodb://{username}:{password}@{host}/{db_name}",
    f"mongodb+srv://{username}:{password}@{host}/{db_name}?retryWrites=true&w=majority",
    f"mongodb://{username}:{password}@{host}:27017/{db_name}",
    f"mongodb://{username}:{password}@{host}",
]

for uri in connection_strings:
    print(f"\nTrying connection string: {uri}")
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        db = client[db_name]
        # Test connection
        db.command('ping')
        print("✅ Connection successful!")
        # List collections
        print("Collections:", db.list_collection_names())
        break
    except Exception as e:
        print(f"❌ Failed: {str(e)}")
        continue
