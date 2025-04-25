from db_manager import DatabaseManager
from tabulate import tabulate

def check_payment_tables():
    with DatabaseManager() as db:
        cur = db.get_ingestion_cursor()
        if cur:
            # First, let's find all tables that might contain payment information
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
                AND (
                    table_name ILIKE '%payment%'
                    OR table_name ILIKE '%collection%'
                    OR table_name ILIKE '%paid%'
                    OR table_name ILIKE '%recovery%'
                );
            """)
            payment_tables = cur.fetchall()
            
            print("\n=== Payment Related Tables ===")
            if payment_tables:
                for table in payment_tables:
                    table_name = table['table_name']
                    print(f"\nTable: {table_name}")
                    
                    # Get columns for this table
                    cur.execute(f"""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                        AND table_name = '{table_name}'
                        ORDER BY ordinal_position;
                    """)
                    columns = cur.fetchall()
                    print(tabulate(columns, headers='keys', tablefmt='grid'))
                    
                    # Get a sample record
                    cur.execute(f"""
                        SELECT *
                        FROM {table_name}
                        LIMIT 1;
                    """)
                    sample = cur.fetchone()
                    if sample:
                        print("\nSample Record:")
                        for key, value in dict(sample).items():
                            print(f"{key}: {value}")
            else:
                print("No payment-related tables found in the database.")

            # Check if allocation_files table has any payment-related columns
            print("\n=== Payment Related Columns in allocation_files ===")
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = 'allocation_files'
                AND (
                    column_name ILIKE '%payment%'
                    OR column_name ILIKE '%collection%'
                    OR column_name ILIKE '%paid%'
                    OR column_name ILIKE '%recovery%'
                    OR column_name ILIKE '%amount%'
                );
            """)
            payment_columns = cur.fetchall()
            if payment_columns:
                print(tabulate(payment_columns, headers='keys', tablefmt='grid'))
            else:
                print("No payment-related columns found in allocation_files table.")

        # Check entity management database for payment information
        cur = db.get_entity_cursor()
        if cur:
            print("\n=== Payment Related Tables in Entity Management ===")
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
                AND (
                    table_name ILIKE '%payment%'
                    OR table_name ILIKE '%collection%'
                    OR table_name ILIKE '%paid%'
                    OR table_name ILIKE '%recovery%'
                );
            """)
            payment_tables = cur.fetchall()
            if payment_tables:
                for table in payment_tables:
                    table_name = table['table_name']
                    print(f"\nTable: {table_name}")
                    
                    # Get columns for this table
                    cur.execute(f"""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                        AND table_name = '{table_name}'
                        ORDER BY ordinal_position;
                    """)
                    columns = cur.fetchall()
                    print(tabulate(columns, headers='keys', tablefmt='grid'))
            else:
                print("No payment-related tables found in entity management database.")

if __name__ == "__main__":
    check_payment_tables()
