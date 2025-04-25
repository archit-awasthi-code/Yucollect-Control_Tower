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
    
    # First check if we have any data
    print("\nChecking table data...")
    cur.execute("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT role) as unique_roles,
            array_agg(DISTINCT role) as role_types
        FROM yucollect_agent;
    """)
    table_info = cur.fetchone()
    print(f"Total rows in yucollect_agent: {table_info[0]}")
    print(f"Unique roles: {table_info[1]}")
    print(f"Role types: {table_info[2]}")
    
    # Check status distribution
    print("\nChecking status distribution...")
    cur.execute("""
        SELECT 
            status,
            COUNT(*) as count
        FROM yucollect_agent
        GROUP BY status;
    """)
    status_info = cur.fetchall()
    print("Status distribution:")
    for status, count in status_info:
        print(f"  {status}: {count}")
    
    # Check role distribution
    print("\nChecking role distribution...")
    cur.execute("""
        SELECT 
            role,
            COUNT(*) as count,
            COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active_count,
            COUNT(CASE WHEN online_status = 'ONLINE' THEN 1 END) as online_count
        FROM yucollect_agent
        GROUP BY role;
    """)
    role_info = cur.fetchall()
    print("Role distribution:")
    for role, count, active, online in role_info:
        print(f"\nRole: {role or 'NULL'}")
        print(f"  Total: {count}")
        print(f"  Active: {active}")
        print(f"  Online: {online}")
    
    # Check recent activity
    print("\nChecking recent activity...")
    cur.execute("""
        SELECT 
            created_at::date as date,
            COUNT(*) as new_users,
            COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active_users
        FROM yucollect_agent
        WHERE created_at >= %s
        GROUP BY created_at::date
        ORDER BY date DESC
        LIMIT 5;
    """, (start_date,))
    recent_activity = cur.fetchall()
    print("Recent activity (last 5 days with data):")
    for date, new, active in recent_activity:
        print(f"  {date}: {new} new users, {active} active users")
    
    # Get user metrics
    print("\nFetching user metrics...")
    cur.execute("""
        WITH user_metrics AS (
            SELECT
                COUNT(DISTINCT id) as total_users,
                COUNT(DISTINCT CASE WHEN role = 'SUPERVISOR' THEN id END) as total_supervisors,
                COUNT(DISTINCT CASE WHEN role = 'AGENT' THEN id END) as total_call_agents,
                COUNT(DISTINCT CASE WHEN role = 'FIELD EXECUTIVE' THEN id END) as total_field_agents,
                COUNT(DISTINCT CASE WHEN online_status = 'ONLINE' THEN id END) as active_users,
                SUM(CASE 
                    WHEN online_status = 'ONLINE' 
                    AND shift_start_time IS NOT NULL 
                    AND shift_end_time IS NOT NULL 
                    THEN 
                        EXTRACT(EPOCH FROM (
                            shift_end_time::time - shift_start_time::time
                        ))/3600 
                    ELSE 0 
                END) as total_hours
            FROM yucollect_agent
        )
        SELECT 
            total_users,
            total_supervisors,
            total_call_agents,
            total_field_agents,
            active_users,
            total_hours
        FROM user_metrics;
    """)
    
    user_counts = cur.fetchone()
    
    print("\nUser Metrics:")
    print(f"Total Users: {user_counts[0] or 0}")
    print(f"Total Supervisors: {user_counts[1] or 0}")
    print(f"Total Call Agents (AGENT): {user_counts[2] or 0}")
    print(f"Total Field Agents (FIELD EXECUTIVE): {user_counts[3] or 0}")
    print(f"Currently Active Users: {user_counts[4] or 0}")
    print(f"Total Hours (based on shifts): {user_counts[5] or 0:.2f} hours")
    
    # Get role-wise breakdown
    print("\nRole-wise Breakdown:")
    cur.execute("""
        SELECT 
            role,
            COUNT(*) as total_count,
            COUNT(CASE WHEN online_status = 'ONLINE' THEN 1 END) as online_count,
            SUM(CASE 
                WHEN online_status = 'ONLINE' 
                AND shift_start_time IS NOT NULL 
                AND shift_end_time IS NOT NULL 
                THEN 
                    EXTRACT(EPOCH FROM (
                        shift_end_time::time - shift_start_time::time
                    ))/3600 
                ELSE 0 
            END) as role_hours
        FROM yucollect_agent
        GROUP BY role
        ORDER BY role;
    """)
    
    role_details = cur.fetchall()
    for role in role_details:
        print(f"\nRole: {role[0]}")
        print(f"  Total Users: {role[1]}")
        print(f"  Online Users: {role[2]}")
        print(f"  Total Hours: {role[3]:.2f}")
    
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
