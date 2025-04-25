from db_manager import DatabaseManager
from tabulate import tabulate
from bson import json_util
import json

def print_collection_info(collection, sample_size=1):
    """Print information about a MongoDB collection"""
    print(f"\n📋 Collection: {collection.name}")
    print("=" * (len(collection.name) + 12))
    
    try:
        # Get count of documents
        doc_count = collection.count_documents({})
        print(f"Document Count: {doc_count}")
        
        # Get sample document
        if doc_count > 0:
            print("\nDocument Structure (from sample):")
            sample = collection.find_one()
            
            def format_document(doc, indent=0):
                output = []
                for key, value in doc.items():
                    if isinstance(value, dict):
                        output.append("  " * indent + f"{key}:")
                        output.extend(format_document(value, indent + 1))
                    elif isinstance(value, list):
                        output.append("  " * indent + f"{key}: Array[{len(value)}]")
                        if len(value) > 0 and isinstance(value[0], dict):
                            output.extend(format_document(value[0], indent + 1))
                    else:
                        output.append("  " * indent + f"{key}: {type(value).__name__}")
                return output
            
            structure = format_document(sample)
            print("\n".join(structure))
            
            # Get indexes
            indexes = collection.list_indexes()
            print("\n📇 Indexes:")
            for idx in indexes:
                print(f"  - {idx['name']}: {idx['key']}")
    except Exception as e:
        print(f"Error analyzing collection: {str(e)}")

def main():
    print("Connecting to MongoDB...")
    try:
        with DatabaseManager() as db:
            mongo_db = db.get_mongo_db()
            if mongo_db is None:
                print("Failed to connect to MongoDB")
                return

            print("\n=== MongoDB Database Overview ===")
            print(f"Database: {mongo_db.name}")
            
            try:
                # List all collections
                collections = mongo_db.list_collection_names()
                print(f"\nFound {len(collections)} collections:")
                
                for collection_name in collections:
                    collection = mongo_db.get_collection(collection_name)
                    print_collection_info(collection)
            except Exception as e:
                print(f"Error listing collections: {str(e)}")
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")

if __name__ == "__main__":
    main()
