from db_manager import DatabaseManager
from tabulate import tabulate
import json

def explore_metrics():
    with DatabaseManager() as db:
        # Check Ingestion Service Database
        print("\n=== Ingestion Service Database Metrics ===")
        cur = db.get_ingestion_cursor()
        if cur:
            # Check allocation metrics with JSON casting
            cur.execute("""
                WITH allocation_stats AS (
                    SELECT 
                        DISTINCT product->>'name' as product_name,
                        bucket->>'name' as bucket_name,
                        COUNT(*) as allocation_count,
                        COUNT(CASE WHEN status = 'fully-allocated' THEN 1 END) as fully_allocated,
                        COUNT(CASE WHEN status = 'partially-allocated' THEN 1 END) as partially_allocated,
                        COUNT(CASE WHEN status = 'unallocated' THEN 1 END) as unallocated
                    FROM allocation_files
                    WHERE product IS NOT NULL
                    GROUP BY product->>'name', bucket->>'name'
                )
                SELECT 
                    product_name,
                    bucket_name,
                    allocation_count,
                    fully_allocated,
                    partially_allocated,
                    unallocated,
                    CAST((fully_allocated::float / NULLIF(allocation_count, 0) * 100) as numeric(10,2)) as success_rate
                FROM allocation_stats
                ORDER BY allocation_count DESC;
            """)
            results = cur.fetchall()
            if results:
                print("\nProduct-wise Allocation Metrics:")
                print(tabulate(results, headers='keys', tablefmt='grid'))

            # Check daily allocation trends by product
            cur.execute("""
                SELECT 
                    DATE(created_at) as date,
                    product->>'name' as product_name,
                    COUNT(*) as total_allocations,
                    COUNT(CASE WHEN status = 'fully-allocated' THEN 1 END) as fully_allocated,
                    CAST((COUNT(CASE WHEN status = 'fully-allocated' THEN 1 END)::float / 
                          NULLIF(COUNT(*), 0) * 100) as numeric(10,2)) as daily_success_rate
                FROM allocation_files
                WHERE created_at >= NOW() - INTERVAL '7 days'
                    AND product IS NOT NULL
                GROUP BY DATE(created_at), product->>'name'
                ORDER BY date DESC, total_allocations DESC;
            """)
            results = cur.fetchall()
            if results:
                print("\nDaily Product Allocation Trends (Last 7 Days):")
                print(tabulate(results, headers='keys', tablefmt='grid'))

            # Check agency performance by product
            cur.execute("""
                WITH agency_stats AS (
                    SELECT 
                        DISTINCT product->>'name' as product_name,
                        agency_id,
                        COUNT(*) as total_allocations,
                        COUNT(CASE WHEN status = 'fully-allocated' THEN 1 END) as fully_allocated
                    FROM allocation_files
                    WHERE product IS NOT NULL AND agency_id IS NOT NULL
                    GROUP BY product->>'name', agency_id
                )
                SELECT 
                    product_name,
                    agency_id,
                    total_allocations,
                    fully_allocated,
                    CAST((fully_allocated::float / NULLIF(total_allocations, 0) * 100) as numeric(10,2)) as success_rate
                FROM agency_stats
                ORDER BY total_allocations DESC
                LIMIT 10;
            """)
            results = cur.fetchall()
            if results:
                print("\nTop Agency Performance by Product:")
                print(tabulate(results, headers='keys', tablefmt='grid'))

            # Check derived fields logic by product
            cur.execute("""
                SELECT 
                    unnest(product) as product_name,
                    COUNT(*) as logic_count,
                    COUNT(DISTINCT agency_id) as agency_count,
                    COUNT(DISTINCT allocator_id) as allocator_count
                FROM allocation_derived_fields_logics
                GROUP BY unnest(product)
                ORDER BY logic_count DESC;
            """)
            results = cur.fetchall()
            if results:
                print("\nProduct-wise Configuration Metrics:")
                print(tabulate(results, headers='keys', tablefmt='grid'))

        # Check Entity Management Database
        print("\n=== Entity Management Database Metrics ===")
        cur = db.get_entity_cursor()
        if cur:
            # Agency performance metrics
            cur.execute("""
                SELECT 
                    COUNT(*) as total_agencies,
                    CAST(AVG(yucollect_score) as numeric(10,2)) as avg_yucollect_score,
                    CAST(AVG(price) as numeric(10,2)) as avg_price,
                    CAST(AVG(experience) as numeric(10,2)) as avg_experience,
                    COUNT(DISTINCT state) as state_count,
                    COUNT(DISTINCT city) as city_count
                FROM agency
                WHERE is_active = true;
            """)
            results = cur.fetchall()
            if results:
                print("\nAgency Overview Metrics:")
                print(tabulate(results, headers='keys', tablefmt='grid'))

            # Agency performance distribution
            cur.execute("""
                SELECT 
                    CASE 
                        WHEN yucollect_score >= 8 THEN 'High (8-10)'
                        WHEN yucollect_score >= 6 THEN 'Medium (6-8)'
                        ELSE 'Low (<6)'
                    END as score_range,
                    COUNT(*) as agency_count,
                    CAST(AVG(price) as numeric(10,2)) as avg_price,
                    CAST(AVG(experience) as numeric(10,2)) as avg_experience
                FROM agency
                WHERE is_active = true
                GROUP BY 
                    CASE 
                        WHEN yucollect_score >= 8 THEN 'High (8-10)'
                        WHEN yucollect_score >= 6 THEN 'Medium (6-8)'
                        ELSE 'Low (<6)'
                    END
                ORDER BY score_range;
            """)
            results = cur.fetchall()
            if results:
                print("\nAgency Performance Distribution:")
                print(tabulate(results, headers='keys', tablefmt='grid'))

if __name__ == "__main__":
    explore_metrics()
