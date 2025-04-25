import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_connection():
    # Connection parameters
    params = {
        "dbname": os.getenv('POSTGRES_INGESTION_DB_NAME'),
        "user": os.getenv('POSTGRES_INGESTION_DB_USER'),
        "password": os.getenv('POSTGRES_INGESTION_DB_PASSWORD'),
        "host": os.getenv('POSTGRES_INGESTION_DB_HOST'),
        "port": os.getenv('POSTGRES_INGESTION_DB_PORT', 5432)
    }

    print("Attempting to connect with parameters:")
    for key, value in params.items():
        if key != 'password':
            print(f"{key}: {value}")
    print("password: ****")

    try:
        # Try connection
        conn = psycopg2.connect(**params)
        print("\nConnection successful!")
        
        # Test a simple query
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT COUNT(*) as count FROM allocation_files")
        result = cur.fetchone()
        print(f"Number of allocation files: {result['count']}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"\nConnection failed: {str(e)}")
        print("\nTroubleshooting tips:")
        print("1. Check if the host is correct")
        print("2. Verify the port is open")
        print("3. Confirm VPN/network access")
        print("4. Validate credentials")

if __name__ == "__main__":
    test_connection()
