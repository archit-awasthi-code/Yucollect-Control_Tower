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
    page_title="LOB Details",
    layout="wide"
)

# Title
st.title("LOB Details")

try:
    # Initialize database connection
    db = DatabaseManager()
    cur = db.get_ingestion_cursor()
    
    if not cur:
        st.error("Could not connect to database")
        st.stop()

    # Get detailed LOB data
    cur.execute("""
        WITH lob_metrics AS (
            SELECT 
                af.allocator_name,
                af.product->>'name' as product_name,
                af.bucket->>'name' as bucket_name,
                COUNT(af.allocation_id) as allocation_count,
                COUNT(DISTINCT af.agency_id) as agency_count,
                SUM(af.total_records) as records_count,
                SUM(af.total_outstanding) as total_outstanding,
                COALESCE(SUM(ap.collection_amount), 0) as total_collections
            FROM allocation_files af
            LEFT JOIN allocation_payments ap ON af.allocation_id = ap.allocation_id
            WHERE af.agency_id != '{agencyId}'
            GROUP BY af.allocator_name, af.product->>'name', af.bucket->>'name'
        )
        SELECT 
            allocator_name as "Allocator",
            product_name as "Product",
            bucket_name as "Bucket",
            allocation_count as "Allocations",
            agency_count as "Agencies",
            records_count as "Records",
            ROUND(total_outstanding::numeric/10000000, 2) as "Outstanding (Cr)",
            ROUND(total_collections::numeric/10000000, 2) as "Collections (Cr)",
            CASE 
                WHEN total_outstanding > 0 
                THEN ROUND((total_collections::float / total_outstanding * 100)::numeric, 2)
                ELSE 0 
            END as "Collection Rate (%)"
        FROM lob_metrics
        ORDER BY total_outstanding DESC;
    """)
    lob_details = cur.fetchall()
    
    if lob_details:
        df_lob_details = pd.DataFrame(lob_details)
        
        # Add filters in columns
        filter_col1, filter_col2 = st.columns(2)
        with filter_col1:
            search = st.text_input("🔍 Search LOBs", key="lob_search")
        with filter_col2:
            sort_col = st.selectbox("Sort by", df_lob_details.columns, key="lob_sort")
        
        sort_order = st.radio("Sort order", ["Descending", "Ascending"], horizontal=True, key="lob_order")
        
        # Apply filters
        if search:
            df_lob_details = df_lob_details[
                df_lob_details.apply(lambda row: 
                    search.lower() in str(row['Allocator']).lower() or 
                    search.lower() in str(row['Product']).lower() or 
                    search.lower() in str(row['Bucket']).lower(), 
                    axis=1)
            ]
        
        df_lob_details = df_lob_details.sort_values(
            by=sort_col, 
            ascending=sort_order=="Ascending"
        )
        
        # Display data with formatting
        st.dataframe(
            df_lob_details.style.format({
                "Outstanding (Cr)": "{:,.2f}",
                "Collections (Cr)": "{:,.2f}",
                "Collection Rate (%)": "{:,.2f}%"
            }),
            use_container_width=True
        )
        
        # Add download button
        st.download_button(
            "📥 Download LOB Details",
            df_lob_details.to_csv(index=False).encode('utf-8'),
            "lob_details.csv",
            "text/csv",
            key='download-lob-csv'
        )
        
        # Add visualizations
        st.subheader("LOB Visualizations")
        viz_col1, viz_col2 = st.columns(2)
        
        with viz_col1:
            # Outstanding Amount by LOB
            fig = px.treemap(
                df_lob_details,
                path=[px.Constant("All"), "Allocator", "Product", "Bucket"],
                values="Outstanding (Cr)",
                color="Collection Rate (%)",
                title="Outstanding Amount Distribution",
                color_continuous_scale="RdYlGn"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with viz_col2:
            # Collection Rate vs Outstanding
            fig = px.scatter(
                df_lob_details,
                x="Outstanding (Cr)",
                y="Collection Rate (%)",
                size="Allocations",
                color="Agencies",
                hover_data=["Allocator", "Product", "Bucket"],
                title="Collection Performance by LOB"
            )
            st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Error: {str(e)}")
    print(f"Error details: {str(e)}")  # For debugging
