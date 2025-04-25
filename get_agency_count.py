from db_manager import DatabaseManager
from tabulate import tabulate

def get_agency_details():
    with DatabaseManager() as db:
        cur = db.get_ingestion_cursor()
        if cur:
            # Get unique agency counts and their allocation stats
            cur.execute("""
                SELECT 
                    COUNT(DISTINCT agency_id) as total_unique_agencies,
                    COUNT(DISTINCT CASE WHEN status = 'fully-allocated' THEN agency_id END) as agencies_with_success,
                    COUNT(DISTINCT CASE WHEN created_at >= NOW() - INTERVAL '7 days' THEN agency_id END) as active_agencies_7d,
                    COUNT(DISTINCT CASE WHEN created_at >= NOW() - INTERVAL '30 days' THEN agency_id END) as active_agencies_30d
                FROM allocation_files
                WHERE agency_id IS NOT NULL AND agency_id != '{agencyId}';
            """)
            results = cur.fetchall()
            print("\n=== Agency Count Summary ===")
            print(tabulate(results, headers='keys', tablefmt='grid'))

            # Get detailed agency stats
            cur.execute("""
                WITH agency_stats AS (
                    SELECT 
                        agency_id,
                        COUNT(*) as total_allocations,
                        COUNT(CASE WHEN status = 'fully-allocated' THEN 1 END) as fully_allocated,
                        COUNT(CASE WHEN status = 'partially-allocated' THEN 1 END) as partially_allocated,
                        COUNT(CASE WHEN status = 'unallocated' THEN 1 END) as unallocated,
                        MAX(created_at) as last_allocation,
                        MIN(created_at) as first_allocation
                    FROM allocation_files
                    WHERE agency_id IS NOT NULL AND agency_id != '{agencyId}'
                    GROUP BY agency_id
                )
                SELECT 
                    agency_id,
                    total_allocations,
                    fully_allocated,
                    partially_allocated,
                    unallocated,
                    CAST((fully_allocated::float / NULLIF(total_allocations, 0) * 100) as numeric(10,2)) as success_rate,
                    last_allocation::date as last_active,
                    first_allocation::date as first_active,
                    (last_allocation::date - first_allocation::date) as active_days
                FROM agency_stats
                ORDER BY total_allocations DESC;
            """)
            results = cur.fetchall()
            print("\n=== Detailed Agency Statistics ===")
            print(tabulate(results, headers='keys', tablefmt='grid'))

if __name__ == "__main__":
    get_agency_details()
