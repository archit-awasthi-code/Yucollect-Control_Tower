import streamlit as st
import pandas as pd
import plotly.express as px
from db_manager import DatabaseManager

# Page config
st.set_page_config(
    page_title="Control Tower Metrics",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .metric-card {
        background-color: #f8f9fa;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.1);
        transition: transform 0.2s;
        position: relative;
        min-height: 160px;
    }
    .metric-card:hover {
        transform: scale(1.02);
        background-color: #e9ecef;
    }
    .metric-title {
        font-size: 1.2em;
        font-weight: bold;
        color: #2c3e50;
        margin-bottom: 10px;
    }
    .metric-value {
        font-size: 2em;
        color: #3498db;
        margin-bottom: 5px;
    }
    .metric-description {
        color: #666;
        font-size: 0.9em;
        margin-bottom: 10px;
    }
    .metric-cta {
        font-size: 0.8em;
        color: #3498db;
        text-decoration: none;
        padding: 4px 8px;
        border: 1px solid #3498db;
        border-radius: 4px;
        transition: all 0.2s;
        position: absolute;
        right: 20px;
        top: 50%;
        transform: translateY(-50%);
        cursor: pointer;
    }
    .metric-cta:hover {
        background-color: #3498db;
        color: white;
    }
    .metric-content {
        width: calc(100% - 100px);  /* Leave space for the button */
    }
    </style>
""", unsafe_allow_html=True)

try:
    # Initialize database connection
    db = DatabaseManager()
    cur = db.get_ingestion_cursor()
    
    if not cur:
        st.error("Could not connect to database")
        st.stop()

    # Title with description
    st.title("Control Tower Metrics Dashboard")
    st.markdown("Comprehensive overview of allocation and collection metrics across all agencies")

    # Get metrics data
    cur.execute("""
        WITH metrics AS (
            SELECT 
                COUNT(DISTINCT af.agency_id) as total_agencies,
                COUNT(af.allocation_id) as total_allocations,
                COUNT(DISTINCT CONCAT(af.allocator_id, '-', af.product->>'name', '-', af.bucket->>'name')) as unique_lobs,
                SUM(af.total_records) as records_count,
                SUM(af.total_outstanding) as total_outstanding,
                (
                    SELECT COALESCE(SUM(ap.collection_amount), 0)
                    FROM allocation_payments ap
                ) as total_collections
            FROM allocation_files af
            WHERE af.agency_id != '{agencyId}'
        )
        SELECT 
            total_agencies,
            total_allocations,
            unique_lobs,
            records_count,
            total_outstanding,
            total_collections,
            CASE 
                WHEN total_outstanding > 0 
                THEN ROUND((total_collections::float / NULLIF(total_outstanding, 0) * 100)::numeric, 2)
                ELSE 0 
            END as collection_percentage
        FROM metrics;
    """)
    metrics = cur.fetchone()

    # Display metrics in two rows
    row1_col1, row1_col2, row1_col3 = st.columns(3)
    row2_col1, row2_col2, row2_col3 = st.columns(3)

    with row1_col1:
        st.markdown(f"""
            <div class="metric-card">
                <div class="metric-content">
                    <div class="metric-title">Total Agencies</div>
                    <div class="metric-value">{metrics['total_agencies']:,}</div>
                    <div class="metric-description">Active agencies in the system</div>
                </div>
                <a href="1_Agency_Details" target="_self" class="metric-cta">View Agencies →</a>
            </div>
        """, unsafe_allow_html=True)

    with row1_col2:
        st.markdown(f"""
            <div class="metric-card">
                <div class="metric-content">
                    <div class="metric-title">Total Allocations</div>
                    <div class="metric-value">{metrics['total_allocations']:,}</div>
                    <div class="metric-description">Allocations across all agencies</div>
                </div>
            </div>
        """, unsafe_allow_html=True)

    with row1_col3:
        st.markdown(f"""
            <div class="metric-card">
                <div class="metric-content">
                    <div class="metric-title">Total Records</div>
                    <div class="metric-value">{metrics['records_count']:,}</div>
                    <div class="metric-description">Total records across all allocations</div>
                </div>
            </div>
        """, unsafe_allow_html=True)

    with row2_col1:
        st.markdown(f"""
            <div class="metric-card">
                <div class="metric-content">
                    <div class="metric-title">Unique LOBs</div>
                    <div class="metric-value">{metrics['unique_lobs']:,}</div>
                    <div class="metric-description">Unique allocator-product-bucket combinations</div>
                </div>
                <a href="2_LOB_Details" target="_self" class="metric-cta">View LOBs →</a>
            </div>
        """, unsafe_allow_html=True)

    with row2_col2:
        st.markdown(f"""
            <div class="metric-card">
                <div class="metric-content">
                    <div class="metric-title">Total Outstanding</div>
                    <div class="metric-value">₹{metrics['total_outstanding']/10000000:,.2f}Cr</div>
                    <div class="metric-description">Total outstanding amount</div>
                </div>
            </div>
        """, unsafe_allow_html=True)

    with row2_col3:
        st.markdown(f"""
            <div class="metric-card">
                <div class="metric-content">
                    <div class="metric-title">Collections</div>
                    <div class="metric-value">₹{metrics['total_collections']/10000000:,.2f}Cr ({metrics['collection_percentage']:.1f}%)</div>
                    <div class="metric-description">Total collected amount and percentage</div>
                </div>
            </div>
        """, unsafe_allow_html=True)

except Exception as e:
    st.error(f"Error: {str(e)}")
    print(f"Error details: {str(e)}")  # For debugging

# Refresh button
if st.button("🔄 Refresh Dashboard"):
    st.experimental_rerun()
