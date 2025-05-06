import streamlit as st
import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# Page config - must be the first Streamlit command
st.set_page_config(
    page_title="LOB Details",
    page_icon="📊",
    layout="wide"
)

# Add the parent directory to Python path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from db_manager import DatabaseManager

# Add custom CSS to override Streamlit's default styling for sidebar nav
st.markdown("""
<style>
    .css-1aumxhk {
        font-size: 1rem !important;
        text-transform: capitalize !important;
    }
</style>
""", unsafe_allow_html=True)

# Custom CSS
st.markdown("""
    <style>
    .section-heading {
        font-size: 1.5rem !important;
        font-weight: 600 !important;
        color: #2c3e50;
        margin: 1.5rem 0 1rem 0 !important;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #3498db;
    }
    .dataframe-container {
        overflow-x: auto;
        width: 100%;
    }
    .dataframe {
        width: 100%;
        border-collapse: collapse;
    }
    .dataframe th {
        background-color: #f1f3f6;
        padding: 8px 12px;
        text-align: left;
        font-weight: 600;
        color: #2c3e50;
        border-bottom: 2px solid #ddd;
    }
    .dataframe td {
        padding: 8px 12px;
        border-bottom: 1px solid #ddd;
    }
    .dataframe tr:hover {
        background-color: #f5f5f5;
    }
    .filter-container {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1.5rem;
        border: 1px solid #eee;
    }
    </style>
""", unsafe_allow_html=True)

# Format amount function
def format_amount(amount):
    """Format amount based on its size:
    - If < 1 lakh: show exact amount with commas
    - If < 1 crore: show in lakhs
    - If >= 1 crore: show in crores
    """
    if amount is None:
        return "₹0.00"
    
    try:
        amount = float(amount)
        if amount < 100000:  # Less than 1 lakh
            return f"₹{amount:,.2f}"
        elif amount < 10000000:  # Less than 1 crore
            return f"₹{amount/100000:.2f}L"
        else:  # 1 crore or more
            return f"₹{amount/10000000:.2f}Cr"
    except (ValueError, TypeError):
        return "₹0.00"

# Enable caching for database queries
@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_lob_data(start_date_str, end_date_str):
    """Get LOB data with caching for better performance"""
    try:
        # Initialize database connection
        db = DatabaseManager()
        cur = db.get_ingestion_cursor()
        
        if not cur:
            return None
            
        # Get LOB details with metrics - optimized query
        query = f"""
            SELECT 
                CONCAT(af.allocator_name, ' - ', 
                       REPLACE(COALESCE(TRIM(BOTH '"' FROM af.product::text), 'Unknown'), '[', ''), ' - ', 
                       REPLACE(COALESCE(TRIM(BOTH '"' FROM af.bucket::text), 'Unknown'), '[', '')) as "LOB Name",
                SUM(af.total_records) as "Accounts",
                SUM(af.total_outstanding) as "Total Outstanding",
                COALESCE(SUM(ap.collection_amount), 0) as "Collection",
                CASE 
                    WHEN SUM(af.total_outstanding) > 0 
                    THEN ROUND((COALESCE(SUM(ap.collection_amount), 0)::float / SUM(af.total_outstanding) * 100)::numeric, 2)
                    ELSE 0 
                END as "Collection Rate (%)"
            FROM allocation_files af
            LEFT JOIN allocation_payments ap ON af.allocation_id = ap.allocation_id
            WHERE af.created_at BETWEEN '{start_date_str}' AND '{end_date_str}'
            GROUP BY 
                af.allocator_name, 
                REPLACE(COALESCE(TRIM(BOTH '"' FROM af.product::text), 'Unknown'), '[', ''), 
                REPLACE(COALESCE(TRIM(BOTH '"' FROM af.bucket::text), 'Unknown'), '[', '')
            ORDER BY SUM(af.total_outstanding) DESC
            LIMIT 1000;  -- Add limit to prevent excessive data loading
        """
        cur.execute(query)
        lob_details = cur.fetchall()
        
        if lob_details:
            # Convert to DataFrame
            df_lob_details = pd.DataFrame(lob_details)
            return df_lob_details
        else:
            return None
    except Exception as e:
        st.error(f"Error in get_lob_data: {str(e)}")
        return None
    finally:
        # Close database connection
        if 'cur' in locals() and cur:
            cur.close()

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
if 'lob_start_date' not in st.session_state:
    st.session_state.lob_start_date = start_date
if 'lob_end_date' not in st.session_state:
    st.session_state.lob_end_date = end_date

start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# Title
st.title("LOB Details")

# Date filter
st.markdown('<h3>Filter by Date Range</h3>', unsafe_allow_html=True)
date_col1, date_col2 = st.columns(2)

def update_dates(key, value):
    """Update dates in session state"""
    if key.endswith('_start_date'):
        st.session_state.start_date = value
        st.session_state.lob_start_date = value
    elif key.endswith('_end_date'):
        st.session_state.end_date = value
        st.session_state.lob_end_date = value

with date_col1:
    start_date = st.date_input(
        "Start Date",
        value=st.session_state.lob_start_date,
        key="lob_start_date",
        on_change=lambda: update_dates('lob_start_date', st.session_state.lob_start_date)
    )

with date_col2:
    end_date = st.date_input(
        "End Date",
        value=st.session_state.lob_end_date,
        key="lob_end_date",
        on_change=lambda: update_dates('lob_end_date', st.session_state.lob_end_date)
    )

# Convert dates to strings for queries
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# Display current date range
st.markdown(f"**Date Range:** {start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}")

# Show loading spinner while fetching data
with st.spinner("Loading LOB data..."):
    # Get LOB data with caching
    df_lob_details = get_lob_data(start_date_str, end_date_str)

if df_lob_details is not None:
    # Format currency columns
    df_lob_details["Total Outstanding (Formatted)"] = df_lob_details["Total Outstanding"].apply(format_amount)
    df_lob_details["Collection (Formatted)"] = df_lob_details["Collection"].apply(format_amount)
    
    # Clean up LOB Name display - remove array brackets and quotes
    df_lob_details["LOB Name"] = df_lob_details["LOB Name"].apply(
        lambda x: x.replace('["', '').replace('"]', '').replace('\\"', '').replace('\\', '').replace('[', '').replace(']', '') if isinstance(x, str) else x
    )
    
    # Section: LOB List
    st.markdown('<h2 class="section-heading">LOB List</h2>', unsafe_allow_html=True)
    
    # Add filter section
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    
    with filter_col1:
        search = st.text_input("🔍 Search LOBs", key="lob_search")
    with filter_col2:
        # Extract allocator names for filtering
        allocator_names = sorted(set([name.split(' - ')[0] for name in df_lob_details["LOB Name"]]))
        allocator_filter = st.multiselect("Filter by Allocator", options=allocator_names, key="lob_allocator_filter")
    with filter_col3:
        sort_col = st.selectbox("Sort by", df_lob_details.columns, index=1, key="lob_sort")  # Default sort by Accounts
    
    sort_order = st.radio("Sort order", ["Descending", "Ascending"], horizontal=True, key="lob_order")
    
    # Apply filters
    filtered_df = df_lob_details.copy()
    if search:
        search_lower = search.lower()
        filtered_df = filtered_df[
            filtered_df.apply(lambda row: any(str(val).lower().find(search_lower) >= 0 for val in row), axis=1)
        ]
    
    if allocator_filter:
        filtered_df = filtered_df[
            filtered_df["LOB Name"].apply(lambda x: any(allocator in x for allocator in allocator_filter))
        ]
    
    # Apply sorting
    if sort_order == "Descending":
        filtered_df = filtered_df.sort_values(by=sort_col, ascending=False)
    else:
        filtered_df = filtered_df.sort_values(by=sort_col, ascending=True)
    
    # Select and reorder columns for display
    display_columns = [
        "LOB Name", "Accounts", "Total Outstanding (Formatted)", 
        "Collection (Formatted)", "Collection Rate (%)"
    ]
    
    # Create display DataFrame - limit to 100 rows for faster rendering
    display_df = filtered_df[display_columns].head(100)
    
    # Show total count and note about limiting display
    total_count = len(filtered_df)
    displayed_count = len(display_df)
    
    st.info(f"Showing {displayed_count} of {total_count} LOBs. {'' if displayed_count == total_count else 'Results limited to 100 rows for performance.'}")
    
    # Display data with formatting
    st.write(
        """
        <div class="dataframe-container">
        """,
        unsafe_allow_html=True
    )
    
    # Convert DataFrame to HTML with custom formatting
    html_table = display_df.to_html(
        index=False,
        escape=False,
        classes='dataframe'
    )
    
    st.write(html_table, unsafe_allow_html=True)
    st.write("</div>", unsafe_allow_html=True)
    
    # Add download button
    st.download_button(
        label="📥 Download LOB Details",
        data=filtered_df.to_csv(index=False).encode('utf-8'),
        file_name=f"lob_details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        key="download_lob_details",
        help="Download the LOB details as a CSV file"
    )
    
else:
    st.warning("No LOB details found for the selected date range.")
