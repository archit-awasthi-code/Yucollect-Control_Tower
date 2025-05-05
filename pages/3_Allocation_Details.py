import streamlit as st
import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import time

# Page config - must be the first Streamlit command
st.set_page_config(
    page_title="Allocation Details",
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
def get_allocation_data(start_date_str, end_date_str):
    """Get allocation data with caching for better performance"""
    try:
        # Initialize database connection
        db = DatabaseManager()
        cur = db.get_ingestion_cursor()
        
        if not cur:
            return None
            
        # Get allocation details with metrics - optimized query
        query = f"""
            SELECT 
                af.allocation_id as "Allocation ID",
                af.allocation_name as "Allocation Name",
                af.allocator_name as "Allocator",
                af.agency_name as "Agency",
                af.product::text as "Product",
                af.bucket::text as "Bucket",
                af.total_records as "Accounts",
                af.total_outstanding as "Total Outstanding",
                CASE 
                    WHEN af.channel->>'digital' = 'true' THEN 'Digital'
                    WHEN af.channel->>'call' = 'true' THEN 'Call'
                    WHEN af.channel->>'field' = 'true' THEN 'Field'
                    ELSE 'Not Assigned'
                END as "Channel (AF)",
                COALESCE(SUM(ap.collection_amount), 0) as "Collection",
                CASE 
                    WHEN af.total_outstanding > 0 
                    THEN ROUND((COALESCE(SUM(ap.collection_amount), 0)::float / af.total_outstanding * 100)::numeric, 2)
                    ELSE 0 
                END as "Collection Rate (%)"
            FROM allocation_files af
            LEFT JOIN allocation_payments ap ON af.allocation_id = ap.allocation_id
            WHERE af.created_at BETWEEN '{start_date_str}' AND '{end_date_str}'
            GROUP BY 
                af.allocation_id
            ORDER BY af.total_outstanding DESC
            LIMIT 1000;  -- Add limit to prevent excessive data loading
        """
        cur.execute(query)
        allocation_details = cur.fetchall()
        
        if allocation_details:
            # Convert to DataFrame
            df_allocation_details = pd.DataFrame(allocation_details)
            return df_allocation_details
        else:
            return None
    except Exception as e:
        st.error(f"Error in get_allocation_data: {str(e)}")
        return None
    finally:
        # Close database connection
        if 'cur' in locals() and cur:
            cur.close()

@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_channel_mapping():
    """Get channel mapping data with caching for better performance"""
    try:
        # Initialize database connection
        db = DatabaseManager()
        ucf_cur = db.get_ucf_cursor()
        
        if not ucf_cur:
            return {}
            
        # Get channel assigned from borrower_details table
        channel_query = """
            SELECT 
                allocation_record_id,
                MAX(channel) as channel
            FROM 
                borrower_details
            WHERE 
                allocation_record_id IS NOT NULL
            GROUP BY 
                allocation_record_id
        """
        ucf_cur.execute(channel_query)
        channel_data = ucf_cur.fetchall()
        
        # Create channel mapping
        channel_mapping = {}
        if channel_data:
            for row in channel_data:
                if row['allocation_record_id'] and row['channel']:
                    channel_mapping[row['allocation_record_id']] = row['channel']
        
        return channel_mapping
    except Exception as e:
        st.error(f"Error in get_channel_mapping: {str(e)}")
        return {}
    finally:
        # Close database connection
        if 'ucf_cur' in locals() and ucf_cur:
            ucf_cur.close()

# Title
st.title("Allocation Details")

# Date filter
st.markdown('<h3>Filter by Date Range</h3>', unsafe_allow_html=True)
date_col1, date_col2 = st.columns(2)
with date_col1:
    start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=30), key="allocation_start_date")
with date_col2:
    end_date = st.date_input("End Date", value=datetime.now(), key="allocation_end_date")
    
# Convert dates to strings for SQL query
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# Show loading spinner while fetching data
with st.spinner("Loading allocation data..."):
    # Get allocation data with caching
    df_allocation_details = get_allocation_data(start_date_str, end_date_str)
    
    # Get channel mapping with caching
    channel_mapping = get_channel_mapping()

if df_allocation_details is not None:
    # Add UCF channel to the DataFrame
    df_allocation_details["Channel Assigned"] = df_allocation_details["Allocation ID"].apply(
        lambda x: channel_mapping.get(x, "Not Assigned")
    )
    
    # Format currency columns
    df_allocation_details["Total Outstanding (Formatted)"] = df_allocation_details["Total Outstanding"].apply(format_amount)
    df_allocation_details["Collection (Formatted)"] = df_allocation_details["Collection"].apply(format_amount)
    
    # Clean up product and bucket display
    df_allocation_details["Product"] = df_allocation_details["Product"].apply(
        lambda x: x.strip('[]"\'').replace('\\"', '').replace('\\', '') if isinstance(x, str) else x
    )
    df_allocation_details["Bucket"] = df_allocation_details["Bucket"].apply(
        lambda x: x.strip('[]"\'').replace('\\"', '').replace('\\', '') if isinstance(x, str) else x
    )
    
    # Section: Allocation List
    st.markdown('<h2 class="section-heading">Allocation List</h2>', unsafe_allow_html=True)
    
    # Add filter section
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    
    with filter_col1:
        search = st.text_input("🔍 Search Allocations", key="allocation_search")
    with filter_col2:
        allocator_filter = st.multiselect("Filter by Allocator", options=sorted(df_allocation_details["Allocator"].unique()), key="allocator_filter")
    with filter_col3:
        sort_col = st.selectbox("Sort by", df_allocation_details.columns, index=7, key="allocation_sort")  # Default sort by Outstanding
    
    sort_order = st.radio("Sort order", ["Descending", "Ascending"], horizontal=True, key="allocation_order")
    
    # Apply filters
    filtered_df = df_allocation_details.copy()
    if search:
        search_lower = search.lower()
        filtered_df = filtered_df[
            filtered_df.apply(lambda row: any(str(val).lower().find(search_lower) >= 0 for val in row), axis=1)
        ]
    
    if allocator_filter:
        filtered_df = filtered_df[filtered_df["Allocator"].isin(allocator_filter)]
    
    # Apply sorting
    if sort_order == "Descending":
        filtered_df = filtered_df.sort_values(by=sort_col, ascending=False)
    else:
        filtered_df = filtered_df.sort_values(by=sort_col, ascending=True)
    
    # Select and reorder columns for display
    display_columns = [
        "Allocation ID", "Allocation Name", "Allocator", "Agency", "Product", "Bucket", 
        "Accounts", "Total Outstanding (Formatted)", "Channel Assigned", 
        "Collection (Formatted)", "Collection Rate (%)"
    ]
    
    # Create display DataFrame - limit to 100 rows for faster rendering
    display_df = filtered_df[display_columns].head(100)
    
    # Show total count and note about limiting display
    total_count = len(filtered_df)
    displayed_count = len(display_df)
    
    st.info(f"Showing {displayed_count} of {total_count} allocations. {'' if displayed_count == total_count else 'Results limited to 100 rows for performance.'}")
    
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
        label="📥 Download Allocation Details",
        data=filtered_df.to_csv(index=False).encode('utf-8'),
        file_name=f"allocation_details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        key="download_allocation_details",
        help="Download the allocation details as a CSV file"
    )
    
else:
    st.warning("No allocation details found for the selected date range.")
