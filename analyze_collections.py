from db_manager import DatabaseManager
from tabulate import tabulate

def analyze_collections():
    with DatabaseManager() as db:
        cur = db.get_ingestion_cursor()
        if cur:
            # Get overall collection statistics
            cur.execute("""
                WITH allocation_stats AS (
                    SELECT 
                        af.allocation_id,
                        af.agency_id,
                        af.agency_name,
                        af.total_outstanding,
                        COALESCE(SUM(ap.collection_amount), 0) as total_collected
                    FROM allocation_files af
                    LEFT JOIN allocation_payments ap ON af.allocation_id = ap.allocation_id
                    GROUP BY af.allocation_id, af.agency_id, af.agency_name, af.total_outstanding
                )
                SELECT 
                    COUNT(DISTINCT allocation_id) as total_allocations,
                    COUNT(DISTINCT CASE WHEN total_collected > 0 THEN allocation_id END) as allocations_with_collection,
                    CAST(SUM(total_outstanding) as numeric(20,2)) as total_outstanding,
                    CAST(SUM(total_collected) as numeric(20,2)) as total_collected,
                    CAST((SUM(total_collected) * 100.0 / NULLIF(SUM(total_outstanding), 0)) as numeric(10,2)) as collection_percentage
                FROM allocation_stats;
            """)
            results = cur.fetchall()
            print("\n=== Overall Collection Statistics ===")
            print(tabulate(results, headers='keys', tablefmt='grid'))

            # Get agency-wise collection statistics
            cur.execute("""
                WITH agency_collections AS (
                    SELECT 
                        af.agency_id,
                        af.agency_name,
                        COUNT(DISTINCT af.allocation_id) as total_allocations,
                        COUNT(DISTINCT CASE WHEN ap.payment_id IS NOT NULL THEN af.allocation_id END) as allocations_with_collection,
                        CAST(SUM(af.total_outstanding) as numeric(20,2)) as total_outstanding,
                        CAST(COALESCE(SUM(ap.collection_amount), 0) as numeric(20,2)) as total_collected
                    FROM allocation_files af
                    LEFT JOIN allocation_payments ap ON af.allocation_id = ap.allocation_id
                    WHERE af.agency_id IS NOT NULL AND af.agency_id != '{agencyId}'
                    GROUP BY af.agency_id, af.agency_name
                )
                SELECT 
                    agency_id,
                    agency_name,
                    total_allocations,
                    allocations_with_collection,
                    total_outstanding,
                    total_collected,
                    CAST((total_collected * 100.0 / NULLIF(total_outstanding, 0)) as numeric(10,2)) as collection_percentage
                FROM agency_collections
                ORDER BY total_collected DESC;
            """)
            results = cur.fetchall()
            print("\n=== Agency-wise Collection Statistics ===")
            print(tabulate(results, headers='keys', tablefmt='grid'))

            # Get daily collection trends
            cur.execute("""
                SELECT 
                    DATE(ap.created_at) as date,
                    COUNT(DISTINCT ap.allocation_id) as allocations_with_payment,
                    COUNT(*) as payment_records,
                    CAST(SUM(ap.collection_amount) as numeric(20,2)) as amount_collected
                FROM allocation_payments ap
                WHERE ap.created_at >= NOW() - INTERVAL '30 days'
                GROUP BY DATE(ap.created_at)
                ORDER BY date DESC;
            """)
            results = cur.fetchall()
            print("\n=== Daily Collection Trends (Last 30 Days) ===")
            print(tabulate(results, headers='keys', tablefmt='grid'))

if __name__ == "__main__":
    analyze_collections()
