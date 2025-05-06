import streamlit as st
import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# Add the parent directory to Python path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from db_manager import DatabaseManager

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
if 'allocator_start_date' not in st.session_state:
    st.session_state.allocator_start_date = start_date
if 'allocator_end_date' not in st.session_state:
    st.session_state.allocator_end_date = end_date

start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# Page config
st.set_page_config(
    page_title="Allocator Details",
    page_icon="💼",
    layout="wide"
)

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
    </style>
""", unsafe_allow_html=True)

# Title
st.title("Allocator Details")

# Date filter
st.markdown('<h3>Filter by Date Range</h3>', unsafe_allow_html=True)
date_col1, date_col2 = st.columns(2)

def update_dates(key, value):
    """Sync dates with main dashboard"""
    if key.endswith('_start_date'):
        st.session_state.start_date = value
        st.session_state.allocator_start_date = value
    elif key.endswith('_end_date'):
        st.session_state.end_date = value
        st.session_state.allocator_end_date = value

with date_col1:
    start_date = st.date_input(
        "Start Date",
        value=st.session_state.allocator_start_date,
        key="allocator_start_date",
        on_change=lambda: update_dates('allocator_start_date', st.session_state.allocator_start_date)
    )
with date_col2:
    end_date = st.date_input(
        "End Date",
        value=st.session_state.allocator_end_date,
        key="allocator_end_date",
        on_change=lambda: update_dates('allocator_end_date', st.session_state.allocator_end_date)
    )

# Convert dates to strings for queries
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# Display current date range
st.markdown(f"**Date Range:** {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

try:
    # Initialize database connection
    cur = db.get_ingestion_cursor()
    
    if not cur:
        st.error("Could not connect to database")
        st.stop()

    # Get allocator details with metrics
    query = f"""
        WITH allocator_metrics AS (
            SELECT 
                af.allocator_id,
                MAX(af.allocator_name) as allocator_name,
                COUNT(DISTINCT af.allocation_id) as allocation_count,
                COUNT(DISTINCT CONCAT(af.product->>'name', '-', af.bucket->>'name')) as lob_count,
                SUM(af.total_records) as total_records,
                SUM(af.total_outstanding) as total_outstanding,
                SUM(COALESCE(ap.collection_amount, 0)) as total_collections
            FROM allocation_files af
            LEFT JOIN allocation_payments ap ON af.allocation_id = ap.allocation_id
            WHERE af.allocator_id IS NOT NULL
            AND af.created_at BETWEEN '{start_date_str}' AND '{end_date_str}'
            GROUP BY af.allocator_id
        )
        SELECT 
            allocator_id as "Allocator ID",
            allocator_name as "Allocator Name",
            allocation_count as "Allocation Count",
            lob_count as "Unique LOBs",
            total_records as "Total Records",
            total_outstanding as "Total Outstanding",
            total_collections as "Collection",
            CASE 
                WHEN total_outstanding > 0 
                THEN ROUND((total_collections::float / total_outstanding * 100)::numeric, 2)
                ELSE 0 
            END as "Collection %"
        FROM allocator_metrics
        ORDER BY total_outstanding DESC;
    """
    cur.execute(query)
    allocator_details = cur.fetchall()
    
    if allocator_details:
        df_allocator_details = pd.DataFrame(allocator_details)
        
        # Section: Allocator List
        st.markdown('<h2 class="section-heading">Allocator List</h2>', unsafe_allow_html=True)
        
        # Add filters in columns
        filter_col1, filter_col2 = st.columns(2)
        with filter_col1:
            search = st.text_input("🔍 Search Allocators", key="allocator_search")
        with filter_col2:
            sort_col = st.selectbox("Sort by", df_allocator_details.columns, index=5, key="allocator_sort")  # Default sort by Outstanding
        
        sort_order = st.radio("Sort order", ["Descending", "Ascending"], horizontal=True, key="allocator_order")
        
        # Apply filters
        filtered_df = df_allocator_details.copy()
        if search:
            filtered_df = filtered_df[
                filtered_df["Allocator Name"].str.contains(search, case=False, na=False)
            ]
        
        filtered_df = filtered_df.sort_values(
            by=sort_col, 
            ascending=sort_order=="Ascending"
        )
        
        # Format the currency columns
        def format_amount(amount):
            """Format amount based on its size:
            - If < 1 lakh: show exact amount with commas
            - If < 1 crore: show in lakhs
            - If >= 1 crore: show in crores
            """
            if pd.isna(amount) or amount is None:
                return "₹0.00"
            
            if amount < 100000:  # Less than 1 lakh
                return f"₹{amount:,.2f}"
            elif amount < 10000000:  # Less than 1 crore
                return f"₹{amount/100000:.2f}L"
            else:  # 1 crore or more
                return f"₹{amount/10000000:.2f}Cr"
        
        # Create display dataframe with formatted values
        display_df = filtered_df.copy()
        display_df["Total Outstanding"] = display_df["Total Outstanding"].apply(lambda x: format_amount(x))
        display_df["Collection"] = display_df["Collection"].apply(lambda x: format_amount(x))
        
        # Display data with formatting
        st.write(
            """
            <style>
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
            </style>
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
            "📥 Download Allocator Details",
            filtered_df.to_csv(index=False).encode('utf-8'),
            "allocator_details.csv",
            "text/csv",
            key='download-allocator-csv'
        )
    else:
        st.warning("No allocator data found. Please check your database connection and ensure the allocation_files table has data.")

except Exception as e:
    st.error(f"Error: {str(e)}")
    print(f"Error details: {str(e)}")  # For debugging
