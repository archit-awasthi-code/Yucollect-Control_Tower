from db_manager import DatabaseManager
from tabulate import tabulate

def get_totals():
    with DatabaseManager() as db:
        # Get total allocations from ingestion service
        cur = db.get_ingestion_cursor()
        if cur:
            print("\n=== Allocation Statistics ===")
            
            # Get total allocations with status breakdown
            cur.execute("""
                SELECT 
                    COUNT(*) as total_allocations,
                    COUNT(DISTINCT agency_id) as unique_agencies,
                    COUNT(CASE WHEN status = 'fully-allocated' THEN 1 END) as fully_allocated,
                    COUNT(CASE WHEN status = 'partially-allocated' THEN 1 END) as partially_allocated,
                    COUNT(CASE WHEN status = 'unallocated' THEN 1 END) as unallocated,
                    CAST((COUNT(CASE WHEN status = 'fully-allocated' THEN 1 END)::float / 
                          NULLIF(COUNT(*), 0) * 100) as numeric(10,2)) as success_rate
                FROM allocation_files;
            """)
            results = cur.fetchall()
            if results:
                print("\nOverall Allocation Stats:")
                print(tabulate(results, headers='keys', tablefmt='grid'))

            # Get agency-wise allocation counts
            cur.execute("""
                SELECT 
                    agency_id,
                    COUNT(*) as total_allocations,
                    COUNT(CASE WHEN status = 'fully-allocated' THEN 1 END) as fully_allocated,
                    CAST((COUNT(CASE WHEN status = 'fully-allocated' THEN 1 END)::float / 
                          NULLIF(COUNT(*), 0) * 100) as numeric(10,2)) as success_rate
                FROM allocation_files
                WHERE agency_id IS NOT NULL
                GROUP BY agency_id
                ORDER BY total_allocations DESC
                LIMIT 5;
            """)
            results = cur.fetchall()
            if results:
                print("\nTop 5 Agencies by Allocation Volume:")
                print(tabulate(results, headers='keys', tablefmt='grid'))

            # Get daily allocation trends
            cur.execute("""
                SELECT 
                    DATE(created_at) as date,
                    COUNT(*) as total_allocations,
                    COUNT(DISTINCT agency_id) as unique_agencies,
                    COUNT(CASE WHEN status = 'fully-allocated' THEN 1 END) as fully_allocated,
                    CAST((COUNT(CASE WHEN status = 'fully-allocated' THEN 1 END)::float / 
                          NULLIF(COUNT(*), 0) * 100) as numeric(10,2)) as success_rate
                FROM allocation_files
                WHERE created_at >= NOW() - INTERVAL '7 days'
                GROUP BY DATE(created_at)
                ORDER BY date DESC;
            """)
            results = cur.fetchall()
            if results:
                print("\nDaily Allocation Trends (Last 7 Days):")
                print(tabulate(results, headers='keys', tablefmt='grid'))

        # Get total agencies from entity management
        cur = db.get_entity_cursor()
        if cur:
            print("\n=== Agency Statistics ===")
            
            # Get total agencies
            cur.execute("""
                SELECT 
                    COUNT(*) as total_agencies,
                    COUNT(DISTINCT state) as total_states,
                    COUNT(DISTINCT city) as total_cities,
                    CAST(AVG(yucollect_score) as numeric(10,2)) as avg_score,
                    CAST(AVG(experience) as numeric(10,2)) as avg_experience
                FROM agency;
            """)
            results = cur.fetchall()
            if results:
                print("\nOverall Agency Stats:")
                print(tabulate(results, headers='keys', tablefmt='grid'))

            # Get state-wise agency distribution
            cur.execute("""
                SELECT 
                    state,
                    COUNT(*) as agency_count,
                    CAST(AVG(yucollect_score) as numeric(10,2)) as avg_score
                FROM agency
                WHERE state IS NOT NULL
                GROUP BY state
                ORDER BY agency_count DESC
                LIMIT 5;
            """)
            results = cur.fetchall()
            if results:
                print("\nTop 5 States by Agency Count:")
                print(tabulate(results, headers='keys', tablefmt='grid'))

if __name__ == "__main__":
    get_totals()
