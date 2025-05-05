import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configurations
db_configs = [
    {
        "name": "Ingestion Service",
        "dbname": os.getenv('POSTGRES_INGESTION_DB_NAME', 'ingestion-service'),
        "user": os.getenv('POSTGRES_INGESTION_DB_USER', 'ingestion_service_svc'),
        "password": os.getenv('POSTGRES_INGESTION_DB_PASSWORD', 'VfdKEJvqb4ki2qeQGL'),
        "host": os.getenv('POSTGRES_INGESTION_DB_HOST', 'qa-postgres-rds.yucollect.in'),
        "port": os.getenv('POSTGRES_INGESTION_DB_PORT', 5432)
    },
    {
        "name": "Entity Management",
        "dbname": os.getenv('POSTGRES_ENTITY_DB_NAME', 'entity-management'),
        "user": os.getenv('POSTGRES_ENTITY_DB_USER', 'entity_management_svc'),
        "password": os.getenv('POSTGRES_ENTITY_DB_PASSWORD', 'VfdKEJvqb4ki2qeQGL'),
        "host": os.getenv('POSTGRES_ENTITY_DB_HOST', 'qa-postgres-rds.yucollect.in'),
        "port": os.getenv('POSTGRES_ENTITY_DB_PORT', 5432)
    },
    {
        "name": "UCF Service",
        "dbname": os.getenv('POSTGRES_UCF_DB_NAME', 'qa-ucf'),
        "user": os.getenv('POSTGRES_UCF_DB_USER', 'ucf_svc'),
        "password": os.getenv('POSTGRES_UCF_DB_PASSWORD', 'VfdKEJvqb4ki2qeQGL'),
        "host": os.getenv('POSTGRES_UCF_DB_HOST', 'qa-postgres-rds.yucollect.in'),
        "port": os.getenv('POSTGRES_UCF_DB_PORT', 5432)
    }
]

def check_table_exists(conn, table_name):
    """Check if a table exists in the database"""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name = %s
            );
        """, (table_name,))
        return cursor.fetchone()[0]

def get_table_schema(conn, table_name):
    """Get the schema of a table"""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT 
                column_name, 
                data_type, 
                character_maximum_length, 
                column_default, 
                is_nullable
            FROM 
                information_schema.columns 
            WHERE 
                table_name = %s
            ORDER BY 
                ordinal_position;
        """, (table_name,))
        return cursor.fetchall()

def list_all_tables(conn):
    """List all tables in the database"""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        return [row[0] for row in cursor.fetchall()]

def main():
    table_name = "borrower_details"
    
    for config in db_configs:
        print(f"\n{'='*80}")
        print(f"Checking {config['name']} database ({config['dbname']})...")
        print(f"{'='*80}")
        
        try:
            # Connect to the database
            conn = psycopg2.connect(
                dbname=config['dbname'],
                user=config['user'],
                password=config['password'],
                host=config['host'],
                port=config['port'],
                connect_timeout=10
            )
            
            # Check if the table exists
            if check_table_exists(conn, table_name):
                print(f"✅ Table '{table_name}' exists in {config['name']} database!")
                
                # Get and print the table schema
                print(f"\nSchema for '{table_name}':")
                schema = get_table_schema(conn, table_name)
                for column in schema:
                    data_type = column['data_type']
                    if column['character_maximum_length']:
                        data_type += f"({column['character_maximum_length']})"
                    
                    nullable = "NULL" if column['is_nullable'] == 'YES' else "NOT NULL"
                    default = f"DEFAULT {column['column_default']}" if column['column_default'] else ""
                    
                    print(f"  {column['column_name']}: {data_type} {nullable} {default}".strip())
            else:
                print(f"❌ Table '{table_name}' does not exist in {config['name']} database.")
                
                # List all tables in the database
                print(f"\nAvailable tables in {config['name']} database:")
                tables = list_all_tables(conn)
                for i, table in enumerate(tables, 1):
                    print(f"  {i}. {table}")
            
            # Close the connection
            conn.close()
            
        except Exception as e:
            print(f"Error connecting to {config['name']} database: {str(e)}")

if __name__ == "__main__":
    main()
