import streamlit as st
import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# Page config - must be the first Streamlit command
st.set_page_config(
    page_title="User Details",
    page_icon="👥",
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

# Format date function
def format_date(date):
    if date is None:
        return "N/A"
    try:
        if isinstance(date, str):
            date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        return date.strftime("%d-%b-%Y %H:%M")
    except:
        return str(date)

# Format time function
def format_time_spent(hours):
    if hours is None:
        return "0 hrs"
    try:
        hours = float(hours)
        if hours < 1:
            return f"{int(hours * 60)} mins"
        else:
            return f"{hours:.1f} hrs"
    except:
        return "0 hrs"

# Enable caching for database queries
@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_user_data():
    """Get user data with caching for better performance"""
    try:
        # Initialize database connection
        db = DatabaseManager()
        cur = db.get_entity_cursor()
        
        if not cur:
            return None
            
        # Get user details with metrics - optimized query
        query = """
            SELECT 
                ya.id as "User ID",
                ya.name as "User Name",
                ya.role as "Role",
                CASE
                    WHEN ya.role = 'FIELD EXECUTIVE' THEN 'Field'
                    WHEN ya.role = 'AGENT' THEN 'Call'
                    ELSE 'N/A'
                END as "Channel",
                COALESCE(a.name, 'Not Assigned') as "Agency",
                ya.created_at as "Added On",
                ya.updated_at as "Last Active On",
                CASE 
                    WHEN ya.shift_start_time IS NOT NULL AND ya.shift_end_time IS NOT NULL 
                    THEN 
                        EXTRACT(HOUR FROM (
                            CAST(ya.shift_end_time AS TIME) - CAST(ya.shift_start_time AS TIME)
                        ))
                    ELSE NULL
                END as "Total Time Spent (Hours)"
            FROM yucollect_agent ya
            LEFT JOIN agency a ON ya.agency_id = a.agency_id
            ORDER BY ya.updated_at DESC NULLS LAST
            LIMIT 1000;  -- Add limit to prevent excessive data loading
        """
        cur.execute(query)
        user_details = cur.fetchall()
        
        if user_details:
            # Convert to DataFrame
            df_user_details = pd.DataFrame(user_details)
            return df_user_details
        else:
            return None
    except Exception as e:
        st.error(f"Error in get_user_data: {str(e)}")
        return None
    finally:
        # Close database connection
        if 'cur' in locals() and cur:
            cur.close()

# Title
st.title("User Details")

# Show loading spinner while fetching data
with st.spinner("Loading user data..."):
    # Get user data with caching
    df_user_details = get_user_data()

if df_user_details is not None:
    # Format date columns
    df_user_details["Added On (Formatted)"] = df_user_details["Added On"].apply(format_date)
    df_user_details["Last Active On (Formatted)"] = df_user_details["Last Active On"].apply(format_date)
    df_user_details["Total Time Spent"] = df_user_details["Total Time Spent (Hours)"].apply(format_time_spent)
    
    # Section: User List
    st.markdown('<h2 class="section-heading">User List</h2>', unsafe_allow_html=True)
    
    # Add filter section
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    
    with filter_col1:
        search = st.text_input("🔍 Search Users", key="user_search")
    with filter_col2:
        role_options = sorted(df_user_details["Role"].unique())
        role_filter = st.multiselect("Filter by Role", options=role_options, key="user_role_filter")
    with filter_col3:
        channel_options = sorted(df_user_details["Channel"].unique())
        channel_filter = st.multiselect("Filter by Channel", options=channel_options, key="user_channel_filter")
    
    sort_col = st.selectbox("Sort by", df_user_details.columns, index=df_user_details.columns.get_loc("Last Active On") if "Last Active On" in df_user_details.columns else 0, key="user_sort")
    sort_order = st.radio("Sort order", ["Descending", "Ascending"], horizontal=True, key="user_order")
    
    # Apply filters
    filtered_df = df_user_details.copy()
    if search:
        search_lower = search.lower()
        filtered_df = filtered_df[
            filtered_df.apply(lambda row: any(str(val).lower().find(search_lower) >= 0 for val in row), axis=1)
        ]
    
    if role_filter:
        filtered_df = filtered_df[filtered_df["Role"].isin(role_filter)]
    
    if channel_filter:
        filtered_df = filtered_df[filtered_df["Channel"].isin(channel_filter)]
    
    # Apply sorting
    if sort_order == "Descending":
        filtered_df = filtered_df.sort_values(by=sort_col, ascending=False)
    else:
        filtered_df = filtered_df.sort_values(by=sort_col, ascending=True)
    
    # Select and reorder columns for display
    display_columns = [
        "User ID", "User Name", "Role", "Channel", "Agency", "Added On (Formatted)", 
        "Last Active On (Formatted)", "Total Time Spent"
    ]
    
    # Create display DataFrame - limit to 100 rows for faster rendering
    display_df = filtered_df[display_columns].head(100)
    
    # Show total count and note about limiting display
    total_count = len(filtered_df)
    displayed_count = len(display_df)
    
    st.info(f"Showing {displayed_count} of {total_count} users. {'' if displayed_count == total_count else 'Results limited to 100 rows for performance.'}")
    
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
        label="📥 Download User Details",
        data=filtered_df.to_csv(index=False).encode('utf-8'),
        file_name=f"user_details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        key="download_user_details",
        help="Download the user details as a CSV file"
    )
    
else:
    st.warning("No user data found. Please check the database connection.")
