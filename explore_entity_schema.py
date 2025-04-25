import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters for entity-management
db_params = {
    "dbname": "entity-management",
    "user": "entity_management_svc",
    "password": os.getenv('POSTGRES_ENTITY_DB_PASSWORD'),
    "host": "qa-postgres-rds.yucollect.in",
    "port": "5432"
}

try:
    # Connect to the database
    print(f"Connecting to entity-management database...")
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    # Get all tables
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    tables = cur.fetchall()
    
    print("\nFound tables:")
    for table in tables:
        table_name = table[0]
        print(f"\n{'-'*50}")
        print(f"Table: {table_name}")
        print(f"{'-'*50}")
        
        # Get columns for each table
        cur.execute("""
            SELECT 
                column_name, 
                data_type,
                character_maximum_length,
                column_default,
                is_nullable
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position;
        """, (table_name,))
        
        columns = cur.fetchall()
        print("Columns:")
        for col in columns:
            col_name, data_type, max_length, default, nullable = col
            print(f"  - {col_name}")
            print(f"    Type: {data_type}")
            if max_length:
                print(f"    Max Length: {max_length}")
            if default:
                print(f"    Default: {default}")
            print(f"    Nullable: {nullable}")
        
        # Get foreign keys
        cur.execute("""
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_name = %s;
        """, (table_name,))
        
        fkeys = cur.fetchall()
        if fkeys:
            print("\nForeign Keys:")
            for fk in fkeys:
                col, ftable, fcol = fk
                print(f"  - {col} -> {ftable}({fcol})")
        
        # Get indexes
        cur.execute("""
            SELECT
                indexname,
                indexdef
            FROM
                pg_indexes
            WHERE
                tablename = %s;
        """, (table_name,))
        
        indexes = cur.fetchall()
        if indexes:
            print("\nIndexes:")
            for idx in indexes:
                idx_name, idx_def = idx
                print(f"  - {idx_name}")
                print(f"    {idx_def}")
    
except Exception as e:
    print(f"Error: {str(e)}")
    if hasattr(e, 'pgerror'):
        print(f"PostgreSQL Error: {e.pgerror}")
    if hasattr(e, 'diag'):
        print(f"Diagnostics: {e.diag}")
finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()
