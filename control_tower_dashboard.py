import streamlit as st
import pandas as pd
import altair as alt
import psycopg2
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from db_manager import DatabaseManager

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="Yucollect Control Tower",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Add Bootstrap dependencies
st.markdown("""
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css">
    <script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            flatpickr(".datepicker", {
                dateFormat: "d/M/Y",
                allowInput: true,
                wrap: true
            });
        });
    </script>
""", unsafe_allow_html=True)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding-top: 0;
    }
    .header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 0.5rem 0;
        border-bottom: 1px solid #eee;
        margin-bottom: 1rem;
    }
    .title {
        font-size: 2rem !important;
        font-weight: 600 !important;
        color: #2c3e50;
        margin: 0;
        letter-spacing: -0.5px;
    }
    .section-heading {
        font-size: 1.5rem !important;
        font-weight: 600 !important;
        color: #2c3e50;
        margin: 2rem 0 1.5rem 0 !important;
        padding-bottom: 0.75rem;
        border-bottom: 2px solid #3498db;
    }
    /* Style the date input */
    div[data-testid="stDateInput"] {
        width: 90px !important;
    }
    div[data-testid="stDateInput"] input {
        font-size: 0.9rem !important;
        padding: 0.4rem !important;
        min-height: unset !important;
    }
    div[data-testid="stDateInput"] div {
        min-width: unset !important;
    }
    .date-separator {
        color: #666;
        font-size: 0.9rem;
        margin: 0 0.1rem;
        padding-top: 5px;
        text-align: center;
    }
    .refresh-info {
        color: #666;
        font-size: 0.85rem;
        margin-right: 0.5rem;
        white-space: nowrap;
        padding-top: 2px;
        text-align: right;
    }
    .metric-card {
        background: white;
        padding: 1.25rem;
        border-radius: 8px;
        border: 1px solid rgba(0,0,0,0.1);
        box-shadow: 0 4px 12px rgba(0,0,0,0.08);
        transition: transform 0.2s, box-shadow 0.2s;
        margin-bottom: 1rem;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 16px rgba(0,0,0,0.12);
    }
    .metric-title {
        font-size: 1.1rem !important;
        font-weight: 600 !important;
        color: #2c3e50;
        margin-bottom: 0.75rem;
        text-transform: capitalize;
    }
    .metric-value {
        font-size: 2rem !important;
        font-weight: 600 !important;
        color: #3498db;
        margin: 0.75rem 0;
    }
    .metric-description {
        font-size: 0.9rem;
        color: #666;
        margin-top: 0.5rem;
    }
    </style>
""", unsafe_allow_html=True)

# Function to format date
def format_date(date):
    return date.strftime("%d/%b/%Y")

# Header with title and date controls
header_container = st.container()
with header_container:
    # Create all columns at once with appropriate widths
    col1, col2, col3, col4, col5, col6, col7 = st.columns([2, 2.5, 0.8, 0.15, 0.8, 1.5, 0.5])
    
    with col1:
        st.markdown('<h1 class="title">Yucollect Control Tower</h1>', unsafe_allow_html=True)
    
    with col2:
        st.write("")  # Empty space to push date pickers right
    
    with col3:
        start_date = st.date_input("", 
                                value=datetime.now() - timedelta(days=30),
                                label_visibility="collapsed",
                                key="start_date")
    
    with col4:
        st.markdown('<div class="date-separator">To</div>', unsafe_allow_html=True)
    
    with col5:
        end_date = st.date_input("", 
                               value=datetime.now(),
                               label_visibility="collapsed",
                               key="end_date")
    
    with col6:
        st.markdown(f'<div class="refresh-info">Last Refreshed On: {format_date(datetime.now())} {datetime.now().strftime("%I:%M %p")}</div>', unsafe_allow_html=True)
    
    with col7:
        if st.button("🔄", type="primary"):
            st.experimental_rerun()

# Summary Section
st.markdown('<h2 class="section-heading">Summary</h2>', unsafe_allow_html=True)

try:
    # Initialize database connection
    db = DatabaseManager()
    cur = db.get_ingestion_cursor()
    
    if not cur:
        st.error("Could not connect to database")
        st.stop()

    # Get summary metrics
    cur.execute("""
        WITH summary AS (
            SELECT 
                COUNT(DISTINCT agency_id) as total_agencies,
                COUNT(DISTINCT allocator_id) as total_allocators,
                COUNT(DISTINCT allocation_id) as total_allocations,
                COUNT(DISTINCT CONCAT(allocator_id, '-', product->>'name', '-', bucket->>'name')) as unique_lobs,
                SUM(total_records) as total_records,
                SUM(total_outstanding) as total_outstanding,
                (
                    SELECT COALESCE(SUM(collection_amount), 0)
                    FROM allocation_payments
                    WHERE created_at BETWEEN %s AND %s
                ) as total_collections
            FROM allocation_files
            WHERE created_at BETWEEN %s AND %s
        )
        SELECT 
            total_agencies,
            total_allocators,
            total_allocations,
            unique_lobs,
            total_records,
            total_outstanding,
            total_collections,
            CASE 
                WHEN total_outstanding > 0 
                THEN ROUND((total_collections::float / NULLIF(total_outstanding, 0) * 100)::numeric, 2)
                ELSE 0 
            END as collection_percentage
        FROM summary;
    """, (start_date, end_date, start_date, end_date))
    summary = cur.fetchone()

    # Display summary metrics in two rows
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Total Agencies</div>
                <div class="metric-value">{summary['total_agencies']}</div>
                <div class="metric-description">Active agencies in the system</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Total Records</div>
                <div class="metric-value">{summary['total_records']:,}</div>
                <div class="metric-description">Total records across all allocations</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Total Allocators</div>
                <div class="metric-value">{summary['total_allocators']}</div>
                <div class="metric-description">Unique allocators with allocations</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Total Outstanding</div>
                <div class="metric-value">₹{summary['total_outstanding']/10000000:.2f}Cr</div>
                <div class="metric-description">Total outstanding amount</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Total Allocations</div>
                <div class="metric-value">{summary['total_allocations']}</div>
                <div class="metric-description">All allocations across agencies</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Total Collections</div>
                <div class="metric-value">₹{summary['total_collections']/10000000:.2f}Cr</div>
                <div class="metric-description">Total collected amount</div>
            </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Unique LOBs</div>
                <div class="metric-value">{summary['unique_lobs']}</div>
                <div class="metric-description">Unique allocator-product-bucket combinations</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Collection Rate</div>
                <div class="metric-value">{summary['collection_percentage']}%</div>
                <div class="metric-description">Total collection percentage</div>
            </div>
        """, unsafe_allow_html=True)

    # User Distribution Section
    st.markdown('<h2 class="section-heading">User Distribution</h2>', unsafe_allow_html=True)
    
    # Get user metrics from entity-management database
    entity_db_params = {
        "dbname": "entity-management",
        "user": "entity_management_svc",
        "password": os.getenv('POSTGRES_ENTITY_DB_PASSWORD'),
        "host": "qa-postgres-rds.yucollect.in",
        "port": "5432"
    }
    
    try:
        conn_entity = psycopg2.connect(**entity_db_params)
        cur_entity = conn_entity.cursor()
        
        # Get user distribution metrics
        cur_entity.execute("""
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
                        ELSE 8 -- Assuming 8 hours per day for users without shift times
                    END) as total_hours
                FROM yucollect_agent
            )
            SELECT * FROM user_metrics;
        """)
        
        user_metrics = cur_entity.fetchone()
        
        # Display metrics in two rows of cards
        user_col1, user_col2, user_col3 = st.columns(3)
        
        with user_col1:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Total Users</div>
                    <div class="metric-value">{user_metrics[0]:,}</div>
                    <div class="metric-description">Total users on the platform</div>
                </div>
                
                <div class="metric-card">
                    <div class="metric-title">Total Call Agents</div>
                    <div class="metric-value">{user_metrics[2]:,}</div>
                    <div class="metric-description">Call center agents on platform</div>
                </div>
            """, unsafe_allow_html=True)
        
        with user_col2:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Total Supervisors</div>
                    <div class="metric-value">{user_metrics[1]:,}</div>
                    <div class="metric-description">Supervisors managing teams</div>
                </div>
                
                <div class="metric-card">
                    <div class="metric-title">Total Field Agents</div>
                    <div class="metric-value">{user_metrics[3]:,}</div>
                    <div class="metric-description">Field executives on platform</div>
                </div>
            """, unsafe_allow_html=True)
        
        with user_col3:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Active Users</div>
                    <div class="metric-value">{user_metrics[4]:,}</div>
                    <div class="metric-description">Currently online users</div>
                </div>
                
                <div class="metric-card">
                    <div class="metric-title">Total Time Spent</div>
                    <div class="metric-value">{user_metrics[5]:,.1f} hrs</div>
                    <div class="metric-description">Cumulative platform usage time</div>
                </div>
            """, unsafe_allow_html=True)
        
        # Add user distribution visualization
        st.subheader("User Role Distribution")
        
        # Create pie chart for user distribution
        user_dist_data = pd.DataFrame({
            'Role': ['Supervisors', 'Call Agents', 'Field Agents'],
            'Count': [
                user_metrics[1],
                user_metrics[2],
                user_metrics[3]
            ]
        })
        
        fig = alt.Chart(user_dist_data).mark_arc().encode(
            theta=alt.Theta(field="Count", type="quantitative"),
            color=alt.Color(field="Role", type="nominal")
        )
        
        st.altair_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.error(f"Error fetching user metrics: {str(e)}")
    finally:
        if 'cur_entity' in locals():
            cur_entity.close()
        if 'conn_entity' in locals():
            conn_entity.close()

except Exception as e:
    st.error(f"Error: {str(e)}")
    print(f"Error details: {str(e)}")  # For debugging
