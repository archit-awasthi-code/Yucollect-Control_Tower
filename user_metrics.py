import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

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
    
    # Set date range (last 30 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    # Get user metrics
    print("\nFetching user metrics...")
    cur.execute("""
        WITH user_metrics AS (
            SELECT
                COUNT(DISTINCT id) as total_users,
                COUNT(DISTINCT CASE WHEN role = 'supervisor' THEN id END) as total_supervisors,
                COUNT(DISTINCT CASE WHEN role = 'call_agent' THEN id END) as total_call_agents,
                COUNT(DISTINCT CASE WHEN role = 'field_agent' THEN id END) as total_field_agents,
                COUNT(DISTINCT CASE WHEN online_status = 'ONLINE' THEN id END) as active_users
            FROM yucollect_agent ya
            WHERE status = 'ACTIVE'
            AND created_at <= %s
            AND (end_date IS NULL OR end_date::timestamp > %s)
        )
        SELECT 
            total_users,
            total_supervisors,
            total_call_agents,
            total_field_agents,
            active_users
        FROM user_metrics;
    """, (end_date, start_date))
    
    user_counts = cur.fetchone()
    
    print("\nUser Metrics:")
    print(f"Total Users: {user_counts[0] or 0}")
    print(f"Total Supervisors: {user_counts[1] or 0}")
    print(f"Total Call Agents: {user_counts[2] or 0}")
    print(f"Total Field Agents: {user_counts[3] or 0}")
    print(f"Currently Active Users: {user_counts[4] or 0}")
    
    # Get agent activity details
    print("\nFetching agent activity details...")
    cur.execute("""
        SELECT 
            role,
            COUNT(*) as agent_count,
            COUNT(CASE WHEN online_status = 'ONLINE' THEN 1 END) as online_count,
            MIN(created_at) as earliest_active,
            MAX(created_at) as latest_active
        FROM yucollect_agent
        WHERE status = 'ACTIVE'
        GROUP BY role
        ORDER BY role;
    """)
    
    activity_details = cur.fetchall()
    if activity_details:
        print("\nAgent Activity by Role:")
        for detail in activity_details:
            role, count, online, earliest, latest = detail
            print(f"\nRole: {role}")
            print(f"Total Agents: {count}")
            print(f"Currently Online: {online or 0}")
            if earliest:
                print(f"Active Since: {earliest.strftime('%Y-%m-%d')}")
            if latest:
                print(f"Last Activity: {latest.strftime('%Y-%m-%d')}")
    
    # Get time spent metrics from ingestion service
    ingestion_db_params = {
        "dbname": "ingestion-service",
        "user": "ingestion_svc",
        "password": os.getenv('POSTGRES_INGESTION_DB_PASSWORD'),
        "host": "qa-postgres-rds.yucollect.in",
        "port": "5432"
    }
    
    conn_ingestion = psycopg2.connect(**ingestion_db_params)
    cur_ingestion = conn_ingestion.cursor()
    
    print("\nFetching time spent metrics...")
    cur_ingestion.execute("""
        SELECT 
            SUM(EXTRACT(EPOCH FROM (session_end - session_start))/3600) as total_hours
        FROM user_sessions
        WHERE session_start >= %s
        AND session_end <= %s;
    """, (start_date, end_date))
    
    time_spent = cur_ingestion.fetchone()
    print(f"\nTotal Time Spent: {time_spent[0]:.2f} hours")

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
    if 'cur_ingestion' in locals():
        cur_ingestion.close()
    if 'conn_ingestion' in locals():
        conn_ingestion.close()
