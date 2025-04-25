from db_manager import DatabaseManager
from tabulate import tabulate

def get_processed_records():
    with DatabaseManager() as db:
        cur = db.get_ingestion_cursor()
        if cur:
            # Get total records processed
            cur.execute("""
                WITH stats AS (
                    SELECT 
                        COUNT(*) as total_allocations,
                        SUM(total_records) as total_records,
                        SUM(CASE 
                            WHEN status = 'fully-allocated' 
                            THEN total_records 
                            ELSE 0 
                        END) as fully_allocated_records,
                        SUM(CASE 
                            WHEN status = 'partially-allocated' 
                            THEN total_records 
                            ELSE 0 
                        END) as partially_allocated_records,
                        SUM(CASE 
                            WHEN status = 'unallocated' 
                            THEN total_records 
                            ELSE 0 
                        END) as unallocated_records,
                        SUM(valid_records) as total_valid_records,
                        SUM(COALESCE(dnd_accounts, 0)) as total_dnd_accounts,
                        SUM(total_outstanding) as total_outstanding_amount
                    FROM allocation_files
                    WHERE total_records IS NOT NULL
                )
                SELECT 
                    total_allocations,
                    total_records,
                    total_valid_records,
                    total_dnd_accounts,
                    CAST(total_outstanding_amount as numeric(20,2)) as total_outstanding_amount,
                    fully_allocated_records,
                    partially_allocated_records,
                    unallocated_records,
                    CAST((fully_allocated_records::float / NULLIF(total_records, 0) * 100) as numeric(10,2)) as fully_allocated_percentage,
                    CAST((partially_allocated_records::float / NULLIF(total_records, 0) * 100) as numeric(10,2)) as partially_allocated_percentage,
                    CAST((unallocated_records::float / NULLIF(total_records, 0) * 100) as numeric(10,2)) as unallocated_percentage
                FROM stats;
            """)
            results = cur.fetchall()
            print("\n=== Total Records Processed ===")
            print(tabulate(results, headers='keys', tablefmt='grid'))

            # Get agency-wise record counts
            cur.execute("""
                SELECT 
                    agency_id,
                    agency_name,
                    COUNT(*) as total_allocations,
                    SUM(total_records) as total_records,
                    SUM(valid_records) as valid_records,
                    SUM(CASE 
                        WHEN status = 'fully-allocated' 
                        THEN total_records 
                        ELSE 0 
                    END) as fully_allocated_records,
                    CAST(SUM(total_outstanding) as numeric(20,2)) as total_outstanding,
                    CAST((SUM(CASE 
                        WHEN status = 'fully-allocated' 
                        THEN total_records 
                        ELSE 0 
                    END)::float / NULLIF(SUM(total_records), 0) * 100) as numeric(10,2)) as success_rate
                FROM allocation_files
                WHERE total_records IS NOT NULL
                    AND agency_id IS NOT NULL
                    AND agency_id != '{agencyId}'
                GROUP BY agency_id, agency_name
                ORDER BY total_records DESC;
            """)
            results = cur.fetchall()
            print("\n=== Agency-wise Record Processing ===")
            print(tabulate(results, headers='keys', tablefmt='grid'))

            # Get daily record processing trends
            cur.execute("""
                SELECT 
                    DATE(created_at) as date,
                    COUNT(*) as allocations,
                    SUM(total_records) as total_records,
                    SUM(valid_records) as valid_records,
                    SUM(CASE 
                        WHEN status = 'fully-allocated' 
                        THEN total_records 
                        ELSE 0 
                    END) as fully_allocated_records,
                    CAST(SUM(total_outstanding) as numeric(20,2)) as total_outstanding,
                    CAST((SUM(CASE 
                        WHEN status = 'fully-allocated' 
                        THEN total_records 
                        ELSE 0 
                    END)::float / NULLIF(SUM(total_records), 0) * 100) as numeric(10,2)) as success_rate
                FROM allocation_files
                WHERE total_records IS NOT NULL
                    AND created_at >= NOW() - INTERVAL '7 days'
                GROUP BY DATE(created_at)
                ORDER BY date DESC;
            """)
            results = cur.fetchall()
            print("\n=== Daily Record Processing (Last 7 Days) ===")
            print(tabulate(results, headers='keys', tablefmt='grid'))

if __name__ == "__main__":
    get_processed_records()
