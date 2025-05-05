from db_manager import DatabaseManager
import pandas as pd

def explore_channel_data():
    db = DatabaseManager()
    
    # Get cursors
    ingestion_cur = db.get_ingestion_cursor()
    entity_cur = db.get_entity_cursor()
    
    if not ingestion_cur or not entity_cur:
        print("Could not connect to databases")
        return
    
    # Check allocation_files table
    print("\nChecking allocation_files table structure:")
    ingestion_cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'allocation_files'
    """)
    print(pd.DataFrame(ingestion_cur.fetchall()))
    
    # Check if we have channel information
    print("\nChecking for channel information in allocation_files:")
    ingestion_cur.execute("""
        SELECT DISTINCT 
            jsonb_object_keys(channel) as channel_keys
        FROM allocation_files 
        WHERE channel IS NOT NULL
        LIMIT 5
    """)
    print(pd.DataFrame(ingestion_cur.fetchall()))
    
    # Check call_logs table
    print("\nChecking for call logs:")
    ingestion_cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'call_logs'
        )
    """)
    print("call_logs table exists:", ingestion_cur.fetchone()['exists'])
    
    # Check field_visits table
    print("\nChecking for field visits:")
    ingestion_cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'field_visits'
        )
    """)
    print("field_visits table exists:", ingestion_cur.fetchone()['exists'])
    
    # Sample some data
    print("\nSampling allocation channel data:")
    ingestion_cur.execute("""
        SELECT 
            id,
            channel,
            created_at
        FROM allocation_files
        WHERE channel IS NOT NULL
        LIMIT 5
    """)
    print(pd.DataFrame(ingestion_cur.fetchall()))

if __name__ == "__main__":
    explore_channel_data()
