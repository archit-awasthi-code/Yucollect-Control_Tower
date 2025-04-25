from db_manager import DatabaseManager
from tabulate import tabulate

def check_schema():
    with DatabaseManager() as db:
        cur = db.get_ingestion_cursor()
        if cur:
            # Get column information
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'allocation_files'
                ORDER BY ordinal_position;
            """)
            results = cur.fetchall()
            print("\n=== Allocation Files Table Schema ===")
            print(tabulate(results, headers='keys', tablefmt='grid'))

            # Get a sample record
            cur.execute("""
                SELECT *
                FROM allocation_files
                LIMIT 1;
            """)
            results = cur.fetchall()
            if results:
                print("\n=== Sample Record ===")
                for key, value in dict(results[0]).items():
                    print(f"{key}: {value}")

if __name__ == "__main__":
    check_schema()
