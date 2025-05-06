import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# Add the parent directory to Python path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from db_manager import DatabaseManager
from cache_manager import CacheManager

# Initialize database manager
db = DatabaseManager()

def get_earliest_date():
    try:
        cur = db.get_ingestion_cursor()
        if not cur:
            return (datetime.now() - timedelta(days=30)).date()
            
        cur.execute("""
            SELECT MIN(created_at)::date
            FROM allocation_files
            WHERE created_at IS NOT NULL;
        """)
        result = cur.fetchone()
        return result['min'] if result and result['min'] else (datetime.now() - timedelta(days=30)).date()
    except Exception as e:
        st.error(f"Error getting earliest date: {str(e)}")
        return (datetime.now() - timedelta(days=30)).date()

# Always get the earliest date first
earliest_date = get_earliest_date()

# Initialize dates from main dashboard's session state
start_date = st.session_state.start_date if 'start_date' in st.session_state else earliest_date
end_date = st.session_state.end_date if 'end_date' in st.session_state else datetime.now().date()

# Initialize session state for this page's dates
if 'onboarding_start_date' not in st.session_state:
    st.session_state.onboarding_start_date = start_date
if 'onboarding_end_date' not in st.session_state:
    st.session_state.onboarding_end_date = end_date

start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# Page config
st.set_page_config(
    page_title="Agency Onboarding",
    page_icon="🏢",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .section-heading {
        font-size: 1.5rem !important;
        font-weight: 600 !important;
        margin: 1.5rem 0 1rem 0 !important;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #3498db;
    }
    .status-accepted {
        color: #28a745;
        font-weight: 500;
    }
    .status-pending {
        color: #ffc107;
        font-weight: 500;
    }
    .status-rejected {
        color: #dc3545;
        font-weight: 500;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize cache manager
cache = CacheManager()

@st.cache_data(ttl=3600)
def get_agency_onboarding_data():
    """Get agency onboarding data with caching"""
    try:
        db = DatabaseManager()
        cur = db.get_entity_cursor()
        if not cur:
            return pd.DataFrame()

        query = """
            SELECT
                yucollect_agency_id as "Agency ID",
                'Agency ' || SUBSTRING(yucollect_agency_id::text, 1, 8) as "Agency Name",
                INITCAP(status) as "Status",
                CURRENT_DATE - INTERVAL '30 days' as "Onboarding Started Date",
                CASE 
                    WHEN status = 'accepted' THEN CURRENT_DATE
                    ELSE NULL 
                END as "Onboarding Completed Date",
                CASE
                    WHEN status = 'accepted' THEN 
                        30
                    ELSE 
                        30
                END as "Days in Process"
            FROM agency_allocators_association
            WHERE yucollect_agency_id IS NOT NULL
            ORDER BY status DESC;
        """
        
        cur.execute(query)
        data = cur.fetchall()
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        
        # Format dates
        if not df.empty:
            df["Onboarding Started Date"] = pd.to_datetime(df["Onboarding Started Date"]).dt.strftime('%Y-%m-%d')
            df["Onboarding Completed Date"] = pd.to_datetime(df["Onboarding Completed Date"]).dt.strftime('%Y-%m-%d')
            
            # Add status styling
            df["Status"] = df["Status"].apply(lambda x: f'<span class="status-{x.lower()}">{x}</span>')
            
        return df
    except Exception as e:
        st.error(f"Error fetching agency onboarding data: {str(e)}")
        return pd.DataFrame()

# Title
st.title("Agency Onboarding")

# Date filter section
date_col1, date_col2 = st.columns(2)

def update_dates(key, value):
    """Update dates in session state"""
    if key.endswith('_start_date'):
        st.session_state.start_date = value
        st.session_state.onboarding_start_date = value
    elif key.endswith('_end_date'):
        st.session_state.end_date = value
        st.session_state.onboarding_end_date = value

with date_col1:
    start_date = st.date_input(
        "Start Date",
        value=st.session_state.onboarding_start_date,
        key="onboarding_start_date",
        on_change=lambda: update_dates('onboarding_start_date', st.session_state.onboarding_start_date)
    )

with date_col2:
    end_date = st.date_input(
        "End Date",
        value=st.session_state.onboarding_end_date,
        key="onboarding_end_date",
        on_change=lambda: update_dates('onboarding_end_date', st.session_state.onboarding_end_date)
    )

# Show loading spinner while fetching data
with st.spinner("Loading agency onboarding data..."):
    # Get agency onboarding data with caching
    df_agency = get_agency_onboarding_data()

if not df_agency.empty:
    # Agency List with Onboarding Status
    st.markdown('<h2 class="section-heading">Agency Onboarding List</h2>', unsafe_allow_html=True)
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Agencies", len(df_agency))
    with col2:
        st.metric("Fully Onboarded", len(df_agency[df_agency["Status"].str.contains("accepted", case=False, na=False)]))
    with col3:
        st.metric("Document Pending", len(df_agency[df_agency["Status"].str.contains("pending", case=False, na=False)]))
    with col4:
        st.metric("Agreement Pending", len(df_agency[df_agency["Status"].str.contains("agency_pending", case=False, na=False)]))
    
    # Display agency data table
    st.write(df_agency.to_html(escape=False, index=False), unsafe_allow_html=True)
    
    # Download button
    csv = df_agency.to_csv(index=False)
    st.download_button(
        "Download Agency Onboarding Data",
        csv,
        "agency_onboarding.csv",
        "text/csv",
        key='download-agency-csv'
    )
else:
    st.info("No agency onboarding data found.")
