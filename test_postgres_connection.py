import psycopg2
import sys
import socket

# Testing various PostgreSQL databases
databases = [
    {
        "name": "Ingestion Service",
        "db_name": "ingestion-service",
        "user": "ingestion_svc",
        "password": "VfdKEJvqb4ki2qeQGL",
        "host": "qa-postgres-rds.yucollect.in",
        "port": "5432"
    },
    {
        "name": "Entity Management",
        "db_name": "entity-management",
        "user": "entity_management_svc",
        "password": "VfdKEJvqb4ki2qeQGL",
        "host": "qa-postgres-rds.yucollect.in",
        "port": "5432"
    },
    {
        "name": "UCF Service",
        "db_name": "qa-ucf",
        "user": "ucf_svc",
        "password": "VfdKEJvqb4ki2qeQGL",
        "host": "qa-postgres-rds.yucollect.in",
        "port": "5432"
    }
]

# First check if the hostname is resolvable
print("Checking if the PostgreSQL host is resolvable...")
try:
    host_ip = socket.gethostbyname("qa-postgres-rds.yucollect.in")
    print(f"✓ Host qa-postgres-rds.yucollect.in resolves to IP: {host_ip}")
    hostname_resolvable = True
except Exception as e:
    print(f"✗ Unable to resolve hostname qa-postgres-rds.yucollect.in")
    print(f"  Error: {str(e)}")
    hostname_resolvable = False

if not hostname_resolvable:
    print("\nIt appears you might not have network connectivity to the database servers.")
    print("You might need to connect to a VPN to access these databases.")
    sys.exit(1)

for db in databases:
    print(f"\nTesting connection to {db['name']} database...")
    try:
        conn = psycopg2.connect(
            dbname=db['db_name'],
            user=db['user'],
            password=db['password'],
            host=db['host'],
            port=db['port'],
            connect_timeout=10  # Add connection timeout
        )
        
        print(f"✓ Successfully connected to {db['name']} database")
        
        # Get list of tables
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        print(f"Tables in {db['name']} database:")
        for table in tables[:10]:  # Show up to 10 tables
            print(f"  - {table[0]}")
            
        if len(tables) > 10:
            print(f"  ... and {len(tables) - 10} more tables")
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"✗ Failed to connect to {db['name']} database:")
        print(f"  Error: {str(e)}")
