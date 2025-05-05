from db_manager import DatabaseManager
import pandas as pd

def explore_ucf_schema():
    db = DatabaseManager()
    ucf_cur = db.get_ucf_cursor()
    
    if not ucf_cur:
        print("Could not connect to UCF database")
        return
    
    # List all tables
    print("\nUCF Database Tables:")
    ucf_cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    tables = ucf_cur.fetchall()
    
    for table in tables:
        table_name = table['table_name']
        print(f"\n=== Table: {table_name} ===")
        
        # Get column information
        ucf_cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """, (table_name,))
        
        columns = ucf_cur.fetchall()
        print("\nColumns:")
        for col in columns:
            print(f"- {col['column_name']} ({col['data_type']}) {'NULL' if col['is_nullable'] == 'YES' else 'NOT NULL'}")
        
        # Get sample data
        ucf_cur.execute(f"""
            SELECT *
            FROM {table_name}
            LIMIT 1;
        """)
        sample = ucf_cur.fetchone()
        if sample:
            print("\nSample Data:")
            for key, value in sample.items():
                print(f"- {key}: {value}")

if __name__ == "__main__":
    explore_ucf_schema()
