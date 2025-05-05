import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

# Database configurations
db_configs = [
    {
        "name": "Ingestion Service",
        "dbname": os.getenv('POSTGRES_INGESTION_DB_NAME'),
        "user": os.getenv('POSTGRES_INGESTION_DB_USER'),
        "password": os.getenv('POSTGRES_INGESTION_DB_PASSWORD'),
        "host": os.getenv('POSTGRES_INGESTION_DB_HOST'),
        "port": os.getenv('POSTGRES_INGESTION_DB_PORT')
    },
    {
        "name": "Entity Management",
        "dbname": os.getenv('POSTGRES_ENTITY_DB_NAME'),
        "user": os.getenv('POSTGRES_ENTITY_DB_USER'),
        "password": os.getenv('POSTGRES_ENTITY_DB_PASSWORD'),
        "host": os.getenv('POSTGRES_ENTITY_DB_HOST'),
        "port": os.getenv('POSTGRES_ENTITY_DB_PORT')
    },
    {
        "name": "UCF Service",
        "dbname": os.getenv('POSTGRES_UCF_DB_NAME'),
        "user": os.getenv('POSTGRES_UCF_DB_USER'),
        "password": os.getenv('POSTGRES_UCF_DB_PASSWORD'),
        "host": os.getenv('POSTGRES_UCF_DB_HOST'),
        "port": os.getenv('POSTGRES_UCF_DB_PORT')
    }
]

# Keywords that might indicate MongoDB-related data
mongodb_related_keywords = [
    'mongo', 'nosql', 'document', 'ihs', 'qa_ihs', 'sync', 'replica', 
    'integration', 'external', 'import', 'export'
]

# Function to explore a database
def explore_database(config):
    print(f"\n\n{'='*80}")
    print(f"Exploring {config['name']} Database")
    print(f"{'='*80}")
    
    try:
        # Connect to the database
        conn = psycopg2.connect(
            dbname=config['dbname'],
            user=config['user'],
            password=config['password'],
            host=config['host'],
            port=config['port'],
            cursor_factory=RealDictCursor
        )
        
        cursor = conn.cursor()
        
        # Get all tables in the database
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        
        print(f"Found {len(tables)} tables in {config['name']} database:")
        for i, table in enumerate(tables):
            table_name = table['table_name']
            print(f"{i+1}. {table_name}")
            
            # Check if table name contains any MongoDB-related keywords
            if any(keyword in table_name.lower() for keyword in mongodb_related_keywords):
                print(f"   ⭐ This table might contain MongoDB-related data")
                
                # Get table structure
                cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table_name}'
                    ORDER BY ordinal_position;
                """)
                
                columns = cursor.fetchall()
                print(f"   Table structure ({len(columns)} columns):")
                for col in columns:
                    print(f"     - {col['column_name']} ({col['data_type']})")
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()['count']
                print(f"   Row count: {row_count}")
                
                # If table has data, show a sample
                if row_count > 0:
                    cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
                    sample = cursor.fetchone()
                    print("   Sample row (first few fields):")
                    sample_dict = dict(sample)
                    sample_items = list(sample_dict.items())[:5]  # Show first 5 fields
                    for key, value in sample_items:
                        print(f"     - {key}: {value}")
                    if len(sample_dict) > 5:
                        print(f"     - ... and {len(sample_dict) - 5} more fields")
        
        # Look for tables that might contain metrics data
        print("\nTables that might contain metrics data:")
        metric_keywords = ['metric', 'stat', 'count', 'total', 'summary', 'report', 'analytics']
        metric_tables = [table['table_name'] for table in tables 
                         if any(keyword in table['table_name'].lower() for keyword in metric_keywords)]
        
        if metric_tables:
            for table_name in metric_tables:
                print(f"- {table_name}")
                
                # Get table structure
                cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table_name}'
                    ORDER BY ordinal_position;
                """)
                
                columns = cursor.fetchall()
                print(f"  Table structure ({len(columns)} columns):")
                for col in columns:
                    print(f"    - {col['column_name']} ({col['data_type']})")
        else:
            print("No tables with obvious metrics-related names found.")
        
        # Close the connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error exploring {config['name']} database: {str(e)}")

# Explore each database
for config in db_configs:
    explore_database(config)

print("\n\nDatabase exploration complete.")
