import streamlit as st

# Page config - must be the first Streamlit command
st.set_page_config(
    page_title="Control Tower",
    layout="wide",
    initial_sidebar_state="collapsed"
)

import pandas as pd
import altair as alt
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import DictCursor

# Load environment variables
load_dotenv()

# Add the parent directory to the path so we can import modules
try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except Exception as e:
    st.write(f"Error setting Python path: {e}")
    st.write(f"Python path: {sys.path}")
    raise

from db_manager import DatabaseManager
from cache_manager import CacheManager
from aggregation_manager import AggregationManager

# Function to format date
def format_date(date):
    return date.strftime("%d/%b/%Y")

# Function to format amount
def format_amount(amount):
    """Format amount based on its size:
    - If < 1 lakh: show exact amount with commas
    - If < 1 crore: show in lakhs
    - If >= 1 crore: show in crores
    """
    # Use AggregationManager but maintain the same behavior
    return AggregationManager.format_currency(amount)

# Function to create a "View All" button
def view_all_button(page_name):
    return f'<a href="/{page_name}" target="_self" class="view-all-btn">View All</a>'

# Initialize database connections
db = DatabaseManager()

# Add Bootstrap dependencies
st.markdown("""
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
    
    <style>
        /* Remove scrollbar from sidebar on main dashboard */
        section[data-testid="stSidebar"] {
            overflow-y: visible !important;
        }
        
        div[data-testid="stVerticalBlock"] {
            overflow-y: visible !important;
        }
        
        /* Remove section dividers from sidebar - more specific selectors */
        section[data-testid="stSidebar"] hr,
        section[data-testid="stSidebar"] div[data-testid="stBlock"] hr,
        section[data-testid="stSidebar"] div.element-container hr,
        section[data-testid="stSidebar"] div.stHorizontalBlock hr,
        .sidebar .sidebar-content .block-container hr {
            display: none !important;
            height: 0 !important;
            margin: 0 !important;
            padding: 0 !important;
            border: none !important;
            visibility: hidden !important;
            opacity: 0 !important;
        }
        
        /* Make sidebar appear as a single pane */
        section[data-testid="stSidebar"] div[data-testid="stSidebarUserContent"] {
            padding-top: 0 !important;
        }
        
        section[data-testid="stSidebar"] div.block-container {
            padding-top: 0 !important;
            padding-bottom: 0 !important;
        }
        
        /* Remove any extra spacing in sidebar */
        section[data-testid="stSidebar"] div.element-container {
            margin-bottom: 0 !important;
        }
        
        /* Custom styling for metrics */
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
            position: relative;
            min-height: 160px;
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
        .view-all-btn {
            display: inline-block;
            position: absolute;
            bottom: 1rem;
            right: 1rem;
            padding: 0.3rem 0.6rem;
            background-color: #f8f9fa;
            color: #3498db;
            border: 1px solid #3498db;
            border-radius: 4px;
            font-size: 0.8rem;
            text-decoration: none;
            transition: all 0.2s ease;
            cursor: pointer;
        }
        .view-all-btn:hover {
            background-color: #3498db;
            color: white;
        }
    </style>
""", unsafe_allow_html=True)

# Custom CSS for sidebar title and navigation
st.markdown("""
<style>
    span.sidebar-title {
        font-size: 1.4rem !important;
        font-weight: 600 !important;
        margin-bottom: 1rem !important;
        color: #2c3e50 !important;
        display: none !important; /* Hide the sidebar title */
    }
    
    /* Capitalize page names in sidebar navigation */
    section[data-testid="stSidebar"] li div p {
        text-transform: capitalize !important;
        font-size: 1rem !important;
    }
    
    /* Style for the main page name in sidebar */
    section[data-testid="stSidebar"] ul li:first-child div p {
        font-weight: bold !important;
        font-size: 1.1rem !important;
    }
    
    /* Custom capitalization for Control Tower Dashboard */
    section[data-testid="stSidebar"] ul li:first-child div p:first-letter,
    section[data-testid="stSidebar"] ul li:first-child div p span:nth-child(2):first-letter,
    section[data-testid="stSidebar"] ul li:first-child div p span:nth-child(3):first-letter {
        text-transform: uppercase !important;
    }
</style>
""", unsafe_allow_html=True)

# Add a custom title to the sidebar
st.sidebar.markdown('<span class="sidebar-title">Control Tower</span>', unsafe_allow_html=True)

# Create sidebar navigation links for View All buttons
if 'view_all_target' not in st.session_state:
    st.session_state.view_all_target = None

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
                                datetime.now() - timedelta(days=30),
                                label_visibility="collapsed",
                                key="start_date")
    
    with col4:
        st.markdown('<div class="date-separator">To</div>', unsafe_allow_html=True)
    
    with col5:
        end_date = st.date_input("", 
                                datetime.now(),
                                label_visibility="collapsed",
                                key="end_date")
    
    with col6:
        st.markdown(f'<div class="refresh-info">Last Refreshed On: {format_date(datetime.now())} {datetime.now().strftime("%I:%M %p")}</div>', unsafe_allow_html=True)
    
    with col7:
        if st.button("🔄", type="primary"):
            # Clear cache when refreshing
            CacheManager.clear_all_caches()
            st.experimental_rerun()

# Convert dates to strings for SQL queries
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# Get summary metrics
def get_summary_metrics(start_date, end_date):
    try:
        # Get ingestion database connection
        cur = db.get_ingestion_cursor()
        if not cur:
            return None
            
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
        return summary
    except Exception as e:
        st.error(f"Error getting summary metrics: {str(e)}")
        return None

# Get summary metrics with caching
summary = CacheManager.get_date_filtered_data(get_summary_metrics, start_date_str, end_date_str)

if not summary:
    st.warning("⚠️ Some tables are missing in the QA environment. This is expected as you're not connected to production.")
    st.info("Required tables: allocation_files, allocation_payments")
    st.info("To fix this, either:")
    st.info("1. Connect to the production database (recommended)")
    st.info("2. Create the required tables in QA with sample data")
    summary = {
        'total_agencies': 0,
        'total_allocators': 0,
        'total_allocations': 0,
        'unique_lobs': 0,
        'total_records': 0,
        'total_outstanding': 0,
        'total_collections': 0,
        'collection_percentage': 0
    }

# Display summary metrics in two rows
col1, col2, col3, col4 = st.columns(4)
    
with col1:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Total Agencies</div>
            <div class="metric-value">{summary['total_agencies']}</div>
            <div class="metric-description">Active agencies in the system</div>
            {view_all_button("Agency_Details")}
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
            {view_all_button("Allocator_Details")}
        </div>
        
        <div class="metric-card">
            <div class="metric-title">Total Outstanding</div>
            <div class="metric-value">{format_amount(summary['total_outstanding'])}</div>
            <div class="metric-description">Total outstanding amount</div>
        </div>
    """, unsafe_allow_html=True)
    
with col3:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Total Allocations</div>
            <div class="metric-value">{summary['total_allocations']}</div>
            <div class="metric-description">Total allocations across all agencies</div>
            {view_all_button("Allocation_Details")}
        </div>
        
        <div class="metric-card">
            <div class="metric-title">Total Collections</div>
            <div class="metric-value">{format_amount(summary['total_collections'])}</div>
            <div class="metric-description">Total collected amount</div>
        </div>
    """, unsafe_allow_html=True)
    
with col4:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Unique LOBs</div>
            <div class="metric-value">{summary['unique_lobs']}</div>
            <div class="metric-description">Unique lines of business</div>
            {view_all_button("LOB_Details")}
        </div>
        
        <div class="metric-card">
            <div class="metric-title">Collection Rate</div>
            <div class="metric-value">{summary['collection_percentage']}%</div>
            <div class="metric-description">Total collection percentage</div>
        </div>
    """, unsafe_allow_html=True)

# Channel Distribution Section
st.markdown('<h2 class="section-heading">Channel Distribution</h2>', unsafe_allow_html=True)

# Get channel metrics
def get_channel_metrics():
    try:
        # Get UCF database connection
        cur = db.get_ucf_cursor()
        if not cur:
            return None
            
        # Get records processed via channel from borrower_details
        cur.execute("""
            SELECT
                COUNT(DISTINCT CASE WHEN UPPER(channel) = 'FIELD' THEN id END) as records_field,
                COUNT(DISTINCT CASE WHEN UPPER(channel) = 'CALL' THEN id END) as records_call,
                COUNT(DISTINCT CASE WHEN UPPER(channel) = 'DIGITAL' THEN id END) as records_digital
            FROM borrower_details
        """)
        
        channel_counts = cur.fetchone()
        
        # Get total calls from order_dispositions
        cur.execute("""
            SELECT COUNT(*) as total_calls
            FROM order_dispositions
        """)
        
        call_counts = cur.fetchone()
        
        # Get field visits from order_dispositions
        cur.execute("""
            SELECT COUNT(*) as total_visits
            FROM order_dispositions
            WHERE UPPER(source) = 'FIELD'
        """)
        
        visit_counts = cur.fetchone()
        
        # Combine all metrics
        channel_metrics = {
            'records_digital': channel_counts['records_digital'] or 0,
            'records_call': channel_counts['records_call'] or 0,
            'records_field': channel_counts['records_field'] or 0,
            'total_calls': call_counts['total_calls'] or 0,
            'total_visits': visit_counts['total_visits'] or 0
        }
        
        return channel_metrics
    except Exception as e:
        st.error(f"Error getting channel metrics: {str(e)}")
        return None

# Get channel metrics with caching
channel_metrics = CacheManager.get_cached_data(get_channel_metrics)

if not channel_metrics:
    channel_metrics = {
        'records_digital': 0,
        'records_call': 0,
        'records_field': 0,
        'total_calls': 0,
        'total_visits': 0
    }

# Display metrics in three columns
col1, col2, col3 = st.columns(3)
    
with col1:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Total Records</div>
            <div class="metric-value">{summary['total_records']:,}</div>
            <div class="metric-description">Total records across all allocations</div>
        </div>
        
        <div class="metric-card">
            <div class="metric-title">Records Processed via Digital</div>
            <div class="metric-value">{channel_metrics['records_digital']:,}</div>
            <div class="metric-description">Total accounts that are run via digital channel</div>
        </div>
    """, unsafe_allow_html=True)
    
with col2:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Records Processed via Call</div>
            <div class="metric-value">{channel_metrics['records_call']:,}</div>
            <div class="metric-description">Total accounts that are run via call channel</div>
        </div>
        
        <div class="metric-card">
            <div class="metric-title">Records Processed via Field</div>
            <div class="metric-value">{channel_metrics['records_field']:,}</div>
            <div class="metric-description">Total accounts that are run via field channel</div>
        </div>
    """, unsafe_allow_html=True)
    
with col3:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Total Calls</div>
            <div class="metric-value">{channel_metrics['total_calls']:,}</div>
            <div class="metric-description">Total calls made on the platform</div>
        </div>
        
        <div class="metric-card">
            <div class="metric-title">Total Field Visits</div>
            <div class="metric-value">{channel_metrics['total_visits']:,}</div>
            <div class="metric-description">Total field visits made by field agents</div>
        </div>
    """, unsafe_allow_html=True)

# User Distribution Section
st.markdown('<h2 class="section-heading">User Distribution</h2>', unsafe_allow_html=True)

# Get user metrics
def get_user_metrics():
    try:
        # Get entity management database connection
        cur = db.get_entity_cursor()
        if not cur:
            return None
            
        # Get user distribution metrics
        cur.execute("""
            WITH user_metrics AS (
                SELECT
                    COUNT(DISTINCT id) as total_users,
                    COUNT(DISTINCT CASE WHEN role = 'SUPERVISOR' THEN id END) as total_supervisors,
                    COUNT(DISTINCT CASE WHEN role = 'AGENT' THEN id END) as total_call_agents,
                    COUNT(DISTINCT CASE WHEN role = 'FIELD EXECUTIVE' THEN id END) as total_field_agents,
                    COUNT(DISTINCT CASE WHEN status = 'ACTIVE' OR online_status = 'ONLINE' THEN id END) as active_users,
                    SUM(CASE 
                        WHEN online_status = 'ONLINE' 
                        AND shift_start_time IS NOT NULL 
                        AND shift_end_time IS NOT NULL 
                        THEN 
                            EXTRACT(EPOCH FROM (
                                CAST(shift_end_time AS TIME) - CAST(shift_start_time AS TIME)
                            ))/3600 
                        ELSE 8 -- Assuming 8 hours per day for users without shift times
                    END) as total_hours
                FROM yucollect_agent
            )
            SELECT * FROM user_metrics;
        """)
        
        user_metrics = cur.fetchone()
        return user_metrics
    except Exception as e:
        st.error(f"Error getting user metrics: {str(e)}")
        return None

# Get user metrics with caching
user_metrics = CacheManager.get_cached_data(get_user_metrics)

if not user_metrics:
    st.warning("⚠️ Unable to retrieve user metrics.")
    user_metrics = {
        'total_users': 0,
        'total_supervisors': 0,
        'total_call_agents': 0,
        'total_field_agents': 0,
        'active_users': 0,
        'total_hours': 0
    }

# Display metrics in two rows of cards
user_col1, user_col2, user_col3 = st.columns(3)
    
with user_col1:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Total Users</div>
            <div class="metric-value">{user_metrics['total_users']:,}</div>
            <div class="metric-description">Total users on the platform</div>
            {view_all_button("User_Details")}
        </div>
        
        <div class="metric-card">
            <div class="metric-title">Total Call Agents</div>
            <div class="metric-value">{user_metrics['total_call_agents']:,}</div>
            <div class="metric-description">Call center agents on platform</div>
        </div>
    """, unsafe_allow_html=True)
    
with user_col2:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Total Supervisors</div>
            <div class="metric-value">{user_metrics['total_supervisors']:,}</div>
            <div class="metric-description">Supervisors managing teams</div>
        </div>
        
        <div class="metric-card">
            <div class="metric-title">Total Field Agents</div>
            <div class="metric-value">{user_metrics['total_field_agents']:,}</div>
            <div class="metric-description">Field executives on platform</div>
        </div>
    """, unsafe_allow_html=True)
    
with user_col3:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Active Users</div>
            <div class="metric-value">{user_metrics['active_users']:,}</div>
            <div class="metric-description">Users with active status</div>
        </div>
        
        <div class="metric-card">
            <div class="metric-title">Total Time Spent</div>
            <div class="metric-value">{user_metrics['total_hours']:,} hrs</div>
            <div class="metric-description">Cumulative platform usage time</div>
        </div>
    """, unsafe_allow_html=True)

# Agency Onboarding Section
st.markdown('<h2 class="section-heading">Agency Onboarding</h2>', unsafe_allow_html=True)

# Get agency onboarding metrics
def get_agency_onboarding_metrics():
    try:
        # Create a new connection for agency metrics
        cur = db.get_entity_cursor()
        if not cur:
            return None
        
        # Get agency onboarding metrics
        cur.execute("""
            WITH agency_status AS (
                SELECT
                    COUNT(*) as total_agencies,
                    COUNT(CASE WHEN status = 'accepted' THEN 1 END) as fully_onboarded,
                    COUNT(CASE WHEN status = 'pending' THEN 1 END) as document_pending,
                    COUNT(CASE WHEN status = 'agency_pending' THEN 1 END) as agreement_pending
                FROM agency_allocators_association
            )
            SELECT * FROM agency_status;
        """)
        
        agency_metrics = cur.fetchone()
        return agency_metrics
    except Exception as e:
        st.error(f"Error getting agency onboarding metrics: {str(e)}")
        return None

# Get agency onboarding metrics with caching
agency_metrics = CacheManager.get_cached_data(get_agency_onboarding_metrics)

if not agency_metrics:
    agency_metrics = {
        'total_agencies': 0,
        'fully_onboarded': 0,
        'document_pending': 0,
        'agreement_pending': 0
    }

# Display agency metrics in cards
agency_col1, agency_col2, agency_col3, agency_col4 = st.columns(4)
    
with agency_col1:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Total Agencies</div>
            <div class="metric-value">{agency_metrics['total_agencies']:,}</div>
            <div class="metric-description">Agencies in onboarding process</div>
            {view_all_button("Agency_Onboarding")}
        </div>
    """, unsafe_allow_html=True)
        
with agency_col2:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Fully Onboarded</div>
            <div class="metric-value">{agency_metrics['fully_onboarded']:,}</div>
            <div class="metric-description">Agencies with completed onboarding</div>
        </div>
    """, unsafe_allow_html=True)
        
with agency_col3:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Document Pending</div>
            <div class="metric-value">{agency_metrics['document_pending']:,}</div>
            <div class="metric-description">Agencies with documents pending</div>
        </div>
    """, unsafe_allow_html=True)
        
with agency_col4:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Agreement Pending</div>
            <div class="metric-value">{agency_metrics['agreement_pending']:,}</div>
            <div class="metric-description">Agencies with pending agreements</div>
        </div>
    """, unsafe_allow_html=True)
