import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

# Add the parent directory to Python path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from db_manager import DatabaseManager

# Page config
st.set_page_config(
    page_title="Agency Details",
    layout="wide"
)

# Title
st.title("Agency Details")

try:
    # Initialize database connection
    db = DatabaseManager()
    cur = db.get_ingestion_cursor()
    
    if not cur:
        st.error("Could not connect to database")
        st.stop()

    # Get detailed agency data
    cur.execute("""
        WITH agency_metrics AS (
            SELECT 
                af.agency_id,
                af.agency_name,
                COUNT(DISTINCT af.allocation_id) as allocation_count,
                COUNT(DISTINCT CONCAT(af.allocator_id, '-', af.product->>'name', '-', af.bucket->>'name')) as lob_count,
                SUM(af.total_records) as records_count,
                SUM(af.total_outstanding) as total_outstanding,
                COALESCE(SUM(ap.collection_amount), 0) as total_collections
            FROM allocation_files af
            LEFT JOIN allocation_payments ap ON af.allocation_id = ap.allocation_id
            WHERE af.agency_id != '{agencyId}' AND af.agency_name IS NOT NULL
            GROUP BY af.agency_id, af.agency_name
        )
        SELECT 
            agency_name as "Agency Name",
            allocation_count as "Allocations",
            lob_count as "LOBs",
            records_count as "Records",
            ROUND(total_outstanding::numeric/10000000, 2) as "Outstanding (Cr)",
            ROUND(total_collections::numeric/10000000, 2) as "Collections (Cr)",
            CASE 
                WHEN total_outstanding > 0 
                THEN ROUND((total_collections::float / total_outstanding * 100)::numeric, 2)
                ELSE 0 
            END as "Collection Rate (%)"
        FROM agency_metrics
        ORDER BY total_outstanding DESC;
    """)
    agency_details = cur.fetchall()
    
    if agency_details:
        df_agency_details = pd.DataFrame(agency_details)
        
        # Add filters in columns
        filter_col1, filter_col2 = st.columns(2)
        with filter_col1:
            search = st.text_input("🔍 Search Agencies", key="agency_search")
        with filter_col2:
            sort_col = st.selectbox("Sort by", df_agency_details.columns, key="agency_sort")
        
        sort_order = st.radio("Sort order", ["Descending", "Ascending"], horizontal=True, key="agency_order")
        
        # Apply filters
        if search:
            df_agency_details = df_agency_details[
                df_agency_details["Agency Name"].str.contains(search, case=False, na=False)
            ]
        
        df_agency_details = df_agency_details.sort_values(
            by=sort_col, 
            ascending=sort_order=="Ascending"
        )
        
        # Display data with formatting
        st.dataframe(
            df_agency_details.style.format({
                "Outstanding (Cr)": "{:,.2f}",
                "Collections (Cr)": "{:,.2f}",
                "Collection Rate (%)": "{:,.2f}%"
            }),
            use_container_width=True
        )
        
        # Add download button
        st.download_button(
            "📥 Download Agency Details",
            df_agency_details.to_csv(index=False).encode('utf-8'),
            "agency_details.csv",
            "text/csv",
            key='download-agency-csv'
        )
        
        # Add visualizations
        st.subheader("Agency Visualizations")
        viz_col1, viz_col2 = st.columns(2)
        
        with viz_col1:
            # Top 10 agencies by outstanding
            top_10_agencies = df_agency_details.nlargest(10, "Outstanding (Cr)")
            fig = px.bar(
                top_10_agencies,
                x="Agency Name",
                y=["Outstanding (Cr)", "Collections (Cr)"],
                title="Top 10 Agencies by Outstanding Amount",
                barmode="group"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with viz_col2:
            # Collection efficiency
            fig = px.scatter(
                df_agency_details,
                x="Outstanding (Cr)",
                y="Collection Rate (%)",
                size="Allocations",
                color="LOBs",
                hover_data=["Agency Name"],
                title="Agency Collection Performance"
            )
            st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Error: {str(e)}")
    print(f"Error details: {str(e)}")  # For debugging
