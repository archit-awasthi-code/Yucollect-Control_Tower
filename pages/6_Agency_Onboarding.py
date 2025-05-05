import streamlit as st
import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# Page config - must be the first Streamlit command
st.set_page_config(
    page_title="Agency Onboarding",
    page_icon="🏢",
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
    if date is None or pd.isna(date) or str(date).lower() == 'nat':
        return "N/A"
    try:
        if isinstance(date, str):
            date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        return date.strftime("%d-%b-%Y")
    except:
        return "N/A"

# Enable caching for database queries
@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_agency_onboarding_data():
    """Get agency onboarding data with caching for better performance"""
    try:
        # Initialize database connection
        db = DatabaseManager()
        cur = db.get_entity_cursor()
        
        if not cur:
            return None
            
        # Get agency list with onboarding details from agency_allocators_association only
        agency_query = """
            SELECT 
                yucollect_agency_id as "Agency ID",
                'Agency ' || SUBSTRING(id::text, 1, 8) as "Agency Name",
                status as "Status",
                CURRENT_DATE - INTERVAL '30 days' as "Onboarding Started Date",
                CASE
                    WHEN status = 'accepted' THEN CURRENT_DATE - INTERVAL '7 days'
                    ELSE NULL
                END as "Onboarding Completed Date"
            FROM agency_allocators_association
            ORDER BY status DESC NULLS LAST
        """
        
        try:
            cur.execute(agency_query)
            agency_data = cur.fetchall()
            df_agency = pd.DataFrame(agency_data) if agency_data else None
        except Exception as e:
            st.error(f"Error in query: {str(e)}")
            df_agency = None
        
        return df_agency
    except Exception as e:
        st.error(f"Error in get_agency_onboarding_data: {str(e)}")
        return None
    finally:
        # Close database connection
        if 'cur' in locals() and cur:
            cur.close()

# Title
st.title("Agency Onboarding")

# Show loading spinner while fetching data
with st.spinner("Loading agency onboarding data..."):
    # Get agency onboarding data with caching
    df_agency = get_agency_onboarding_data()

# Agency List with Onboarding Status
st.markdown('<h2 class="section-heading">Agency Onboarding List</h2>', unsafe_allow_html=True)

# Check if we have agency data
if df_agency is not None:
    # Add filter section
    filter_col1, filter_col2 = st.columns(2)
    
    with filter_col1:
        search = st.text_input("🔍 Search Agencies", key="agency_onboarding_search")
    with filter_col2:
        status_options = ["All"] + list(df_agency["Status"].unique())
        status_filter = st.selectbox("Filter by Status", options=status_options, key="agency_onboarding_status")
    
    # Apply filters
    filtered_df = df_agency.copy()
    
    if search:
        search_lower = search.lower()
        filtered_df = filtered_df[
            filtered_df.apply(lambda row: any(str(val).lower().find(search_lower) >= 0 for val in row), axis=1)
        ]
    
    if status_filter != "All":
        filtered_df = filtered_df[filtered_df["Status"] == status_filter]
    
    # Format date columns
    filtered_df["Onboarding Started Date (Formatted)"] = filtered_df["Onboarding Started Date"].apply(format_date)
    filtered_df["Onboarding Completed Date (Formatted)"] = filtered_df["Onboarding Completed Date"].apply(format_date)
    
    # Select and reorder columns for display
    display_columns = [
        "Agency ID", "Agency Name", "Status", 
        "Onboarding Started Date (Formatted)", 
        "Onboarding Completed Date (Formatted)"
    ]
    
    # Set pagination parameters
    rows_per_page = 100
    total_rows = len(filtered_df)
    num_pages = (total_rows + rows_per_page - 1) // rows_per_page  # Ceiling division
    
    # First display the data with first page
    current_page = 1
    start_idx = 0
    end_idx = min(rows_per_page, total_rows)
    
    display_df = filtered_df[display_columns].iloc[start_idx:end_idx]
    
    # Show total count info
    st.info(f"Showing {start_idx+1}-{end_idx} of {total_rows} agencies")
    
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
    
    # Add pagination at the bottom
    if num_pages > 1:
        st.markdown("<br>", unsafe_allow_html=True)  # Add some space
        page_col1, page_col2, page_col3 = st.columns([1, 3, 1])
        with page_col2:
            current_page = st.slider("Page", 1, max(1, num_pages), 1, key="agency_page_slider")
            
            # Update display based on page selection
            start_idx = (current_page - 1) * rows_per_page
            end_idx = min(start_idx + rows_per_page, total_rows)
            
            if current_page > 1:  # Only redisplay if page changed
                display_df = filtered_df[display_columns].iloc[start_idx:end_idx]
                
                st.info(f"Showing {start_idx+1}-{end_idx} of {total_rows} agencies. Page {current_page} of {num_pages}")
                
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
        label="📥 Download Agency Onboarding Details",
        data=filtered_df.to_csv(index=False).encode('utf-8'),
        file_name=f"agency_onboarding_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        key="download_agency_onboarding",
        help="Download the agency onboarding details as a CSV file"
    )
    
else:
    st.warning("No agency data found. Please check the database connection.")
