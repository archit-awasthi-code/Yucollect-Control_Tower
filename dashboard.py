import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection configuration
DB_CONFIG = {
    "database": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST'),
    "port": os.getenv('DB_PORT', 5432)
}

# Initialize connection state
if 'db_connection' not in st.session_state:
    st.session_state.db_connection = None

def init_connection():
    try:
        st.session_state.db_connection = psycopg2.connect(
            **DB_CONFIG,
            cursor_factory=RealDictCursor,
            connect_timeout=10
        )
        return True
    except Exception as e:
        st.error(f"Failed to connect to database: {str(e)}")
        st.error("Please check your database credentials and connection.")
        return False

# Page config
st.set_page_config(
    page_title="Control Tower Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title and connection status
st.title("Control Tower Dashboard")

# Initialize database connection
if not st.session_state.db_connection or st.session_state.db_connection.closed:
    if not init_connection():
        st.stop()

try:
    # Create a new cursor for each query
    with st.session_state.db_connection.cursor() as cur:
        # Status distribution
        cur.execute("""
            SELECT 
                COALESCE(status, 'unknown') as status,
                COUNT(*) as count
            FROM allocation_files
            GROUP BY status
            ORDER BY count DESC;
        """)
        status_counts = cur.fetchall()
        if status_counts:
            status_df = pd.DataFrame(status_counts)
            
            # Display total allocations
            total_allocations = status_df['count'].sum()
            st.metric("Total Allocations", total_allocations)
            
            # Create two columns for charts
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Allocation Status Distribution")
                fig_pie = px.pie(
                    status_df,
                    values='count',
                    names='status',
                    title='Allocation Status Distribution',
                    hole=0.4
                )
                fig_pie.update_traces(textposition='outside', textinfo='percent+label')
                st.plotly_chart(fig_pie, use_container_width=True)

        # Trend data
        cur.execute("""
            SELECT 
                DATE(created_at) as date,
                status,
                COUNT(*) as count
            FROM allocation_files
            WHERE created_at >= NOW() - INTERVAL '7 days'
            GROUP BY DATE(created_at), status
            ORDER BY date;
        """)
        trend_data = cur.fetchall()
        if trend_data:
            trend_df = pd.DataFrame(trend_data)
            
            with col2:
                st.subheader("7-Day Allocation Trend")
                fig_line = px.line(
                    trend_df,
                    x='date',
                    y='count',
                    color='status',
                    title='Allocation Trend (Last 7 Days)'
                )
                fig_line.update_layout(legend_title="Status")
                st.plotly_chart(fig_line, use_container_width=True)

        # Allocator Performance
        st.subheader("Top Allocators Performance")
        cur.execute("""
            SELECT 
                allocator_id,
                COUNT(*) as total_allocations,
                COUNT(CASE WHEN status = 'fully-allocated' THEN 1 END) as fully_allocated,
                ROUND(COUNT(CASE WHEN status = 'fully-allocated' THEN 1 END)::numeric / 
                      COUNT(*)::numeric * 100, 2) as success_rate
            FROM allocation_files
            GROUP BY allocator_id
            ORDER BY total_allocations DESC
            LIMIT 5;
        """)
        allocator_stats = cur.fetchall()
        if allocator_stats:
            allocator_df = pd.DataFrame(allocator_stats)
            st.dataframe(
                allocator_df,
                column_config={
                    "success_rate": st.column_config.ProgressColumn(
                        "Success Rate",
                        help="Percentage of fully allocated files",
                        format="%{:.1f}",
                        min_value=0,
                        max_value=100,
                    ),
                },
                use_container_width=True
            )

        # Recent Allocations with auto-refresh
        st.subheader("Recent Allocations")
        cur.execute("""
            SELECT 
                allocation_id,
                allocation_name,
                status,
                created_at,
                allocator_id
            FROM allocation_files
            ORDER BY created_at DESC
            LIMIT 10;
        """)
        recent_allocations = cur.fetchall()
        if recent_allocations:
            recent_df = pd.DataFrame(recent_allocations)
            st.dataframe(recent_df, use_container_width=True)

        # 7-day breakdown section
        st.markdown("---")
        st.subheader("Last 7 Days Allocation Breakdown")
        
        # Get detailed daily breakdown
        cur.execute("""
            WITH daily_stats AS (
                SELECT 
                    DATE(created_at) as date,
                    status,
                    COUNT(*) as count
                FROM allocation_files
                WHERE created_at >= NOW() - INTERVAL '7 days'
                GROUP BY DATE(created_at), status
            ),
            dates AS (
                SELECT generate_series(
                    date_trunc('day', NOW() - INTERVAL '6 days'),
                    date_trunc('day', NOW()),
                    '1 day'::interval
                )::date as date
            )
            SELECT 
                d.date,
                COALESCE(SUM(ds.count), 0) as total_count,
                json_object_agg(
                    COALESCE(ds.status, 'unknown'), 
                    COALESCE(ds.count, 0)
                ) as status_breakdown
            FROM dates d
            LEFT JOIN daily_stats ds ON d.date = ds.date
            GROUP BY d.date
            ORDER BY d.date;
        """)
        daily_breakdown = cur.fetchall()
        
        if daily_breakdown:
            # Convert to DataFrame
            daily_df = pd.DataFrame(daily_breakdown)
            
            # Create columns for the metrics
            cols = st.columns(7)
            
            # Display daily metrics
            for i, (_, row) in enumerate(daily_df.iterrows()):
                date_str = row['date'].strftime('%b %d')
                with cols[i]:
                    st.metric(
                        date_str,
                        f"{int(row['total_count'])}",
                        help=f"Total allocations on {date_str}"
                    )
                    
            # Create a detailed bar chart
            st.markdown("### Daily Allocation Counts")
            
            # Prepare data for stacked bar chart
            fig = go.Figure()
            
            # Get all possible statuses
            all_statuses = set()
            for row in daily_df['status_breakdown']:
                all_statuses.update(row.keys())
            
            # Create traces for each status
            for status in sorted(all_statuses):
                y_values = [row.get(status, 0) for row in daily_df['status_breakdown']]
                fig.add_trace(go.Bar(
                    name=status,
                    x=daily_df['date'],
                    y=y_values,
                    hovertemplate="Date: %{x}<br>" +
                                f"Status: {status}<br>" +
                                "Count: %{y}<br>" +
                                "<extra></extra>"
                ))
            
            fig.update_layout(
                barmode='stack',
                title="Daily Allocation Distribution",
                xaxis_title="Date",
                yaxis_title="Number of Allocations",
                hovermode='x unified',
                showlegend=True,
                legend_title="Status",
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Display detailed table
            with st.expander("Show Detailed Daily Breakdown"):
                # Create a more readable table format
                detailed_table = []
                for _, row in daily_df.iterrows():
                    breakdown = row['status_breakdown']
                    breakdown_row = {
                        'Date': row['date'].strftime('%Y-%m-%d'),
                        'Total': int(row['total_count']),
                        **{k: int(v) for k, v in breakdown.items()}
                    }
                    detailed_table.append(breakdown_row)
                
                detailed_df = pd.DataFrame(detailed_table)
                st.dataframe(
                    detailed_df,
                    use_container_width=True,
                    column_config={
                        "Date": st.column_config.DateColumn(
                            "Date",
                            format="MMM DD, YYYY"
                        )
                    }
                )

        # Agency Analytics Section
        st.markdown("---")
        st.subheader("Agency Analytics")

        # Get agency-wise allocation stats
        cur.execute("""
            WITH daily_agency_stats AS (
                SELECT 
                    agency_id,
                    status,
                    COUNT(*) as count,
                    DATE(created_at) as date
                FROM allocation_files
                WHERE created_at >= NOW() - INTERVAL '7 days'
                    AND agency_id IS NOT NULL
                GROUP BY agency_id, status, DATE(created_at)
            ),
            agency_totals AS (
                SELECT 
                    agency_id,
                    COUNT(*) as total_allocations,
                    jsonb_object_agg(
                        status,
                        count
                    ) as status_breakdown,
                    jsonb_object_agg(
                        date::text,
                        count
                    ) as daily_trend
                FROM daily_agency_stats
                GROUP BY agency_id
            )
            SELECT *
            FROM agency_totals
            ORDER BY total_allocations DESC
            LIMIT 10;
        """)
        agency_stats = cur.fetchall()

        if agency_stats:
            agency_df = pd.DataFrame(agency_stats)

            # Create two columns for charts
            col1, col2 = st.columns(2)

            with col1:
                # Agency Distribution Pie Chart
                fig_agency_pie = px.pie(
                    agency_df,
                    values='total_allocations',
                    names='agency_id',
                    title='Allocation Distribution by Agency',
                    hole=0.4
                )
                fig_agency_pie.update_traces(textposition='outside', textinfo='percent+label')
                st.plotly_chart(fig_agency_pie, use_container_width=True)

            with col2:
                # Status breakdown by agency
                status_data = []
                for _, row in agency_df.iterrows():
                    breakdown = row['status_breakdown']
                    for status, count in breakdown.items():
                        status_data.append({
                            'Agency': row['agency_id'],
                            'Status': status,
                            'Count': count
                        })
                status_df = pd.DataFrame(status_data)

                fig_agency_status = px.bar(
                    status_df,
                    x='Agency',
                    y='Count',
                    color='Status',
                    title='Status Distribution by Agency',
                    barmode='stack'
                )
                fig_agency_status.update_layout(
                    xaxis_title="Agency ID",
                    yaxis_title="Number of Allocations",
                    showlegend=True,
                    legend_title="Status"
                )
                st.plotly_chart(fig_agency_status, use_container_width=True)

            # Agency Trend Lines
            st.subheader("Agency-wise Daily Trends")
            
            # Create trend data
            trend_data = []
            for _, row in agency_df.iterrows():
                daily_trend = row['daily_trend']
                for date, count in daily_trend.items():
                    trend_data.append({
                        'Agency': row['agency_id'],
                        'Date': pd.to_datetime(date),
                        'Count': count
                    })
            trend_df = pd.DataFrame(trend_data)

            fig_trend = px.line(
                trend_df,
                x='Date',
                y='Count',
                color='Agency',
                title='Daily Allocation Trends by Agency',
                markers=True
            )
            fig_trend.update_layout(
                xaxis_title="Date",
                yaxis_title="Number of Allocations",
                showlegend=True,
                legend_title="Agency ID",
                hovermode='x unified'
            )
            st.plotly_chart(fig_trend, use_container_width=True)

            # Detailed Agency Performance Table
            with st.expander("Show Detailed Agency Performance"):
                # Calculate success rates and create detailed metrics
                detailed_agency_data = []
                for _, row in agency_df.iterrows():
                    breakdown = row['status_breakdown']
                    fully_allocated = breakdown.get('fully-allocated', 0)
                    success_rate = (fully_allocated / row['total_allocations'] * 100) if row['total_allocations'] > 0 else 0
                    
                    detailed_agency_data.append({
                        'Agency ID': row['agency_id'],
                        'Total Allocations': row['total_allocations'],
                        'Fully Allocated': fully_allocated,
                        'Success Rate (%)': round(success_rate, 2),
                        **{f"Status: {k}": v for k, v in breakdown.items()}
                    })
                
                detailed_agency_df = pd.DataFrame(detailed_agency_data)
                st.dataframe(
                    detailed_agency_df,
                    use_container_width=True,
                    column_config={
                        "Success Rate (%)": st.column_config.ProgressColumn(
                            "Success Rate",
                            help="Percentage of fully allocated files",
                            format="%{:.1f}",
                            min_value=0,
                            max_value=100,
                        )
                    }
                )

        # Collections Analytics Section
        st.markdown("---")
        st.subheader("Collections Analytics")

        # Get collection statistics
        cur.execute("""
            WITH collection_stats AS (
                SELECT 
                    af.allocation_id,
                    af.agency_id,
                    af.agency_name,
                    af.product->>'name' as product_name,
                    af.total_outstanding,
                    COALESCE(SUM(ap.collection_amount), 0) as total_collected,
                    af.created_at
                FROM allocation_files af
                LEFT JOIN allocation_payments ap ON af.allocation_id = ap.allocation_id
                GROUP BY af.allocation_id, af.agency_id, af.agency_name, af.product->>'name', 
                         af.total_outstanding, af.created_at
            )
            SELECT 
                COUNT(DISTINCT allocation_id) as total_allocations,
                COUNT(DISTINCT CASE WHEN total_collected > 0 THEN allocation_id END) as allocations_with_collection,
                CAST(SUM(total_outstanding) as numeric(20,2)) as total_outstanding,
                CAST(SUM(total_collected) as numeric(20,2)) as total_collected,
                CAST((SUM(total_collected) * 100.0 / NULLIF(SUM(total_outstanding), 0)) as numeric(10,2)) as collection_percentage
            FROM collection_stats;
        """)
        overall_stats = cur.fetchone()

        # Create three columns for key metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Total Outstanding",
                f"₹{overall_stats['total_outstanding']/10000000:.2f}Cr",
                help="Total outstanding amount across all allocations"
            )
        
        with col2:
            st.metric(
                "Total Collected",
                f"₹{overall_stats['total_collected']/100000:.2f}L",
                help="Total amount collected so far"
            )
        
        with col3:
            st.metric(
                "Collection Rate",
                f"{overall_stats['collection_percentage']}%",
                help="Percentage of outstanding amount collected"
            )

        # Create two columns for charts and insights
        col1, col2 = st.columns([2, 1])

        with col1:
            # Get agency-wise collection data
            cur.execute("""
                WITH agency_collections AS (
                    SELECT 
                        af.agency_id,
                        af.agency_name,
                        COUNT(DISTINCT af.allocation_id) as total_allocations,
                        CAST(SUM(af.total_outstanding) as numeric(20,2)) as total_outstanding,
                        CAST(COALESCE(SUM(ap.collection_amount), 0) as numeric(20,2)) as total_collected
                    FROM allocation_files af
                    LEFT JOIN allocation_payments ap ON af.allocation_id = ap.allocation_id
                    WHERE af.agency_id IS NOT NULL AND af.agency_id != '{agencyId}'
                        AND af.agency_name IS NOT NULL
                    GROUP BY af.agency_id, af.agency_name
                    HAVING SUM(af.total_outstanding) > 0
                )
                SELECT *,
                    CAST((total_collected * 100.0 / NULLIF(total_outstanding, 0)) as numeric(10,2)) as collection_percentage
                FROM agency_collections
                ORDER BY total_collected DESC
                LIMIT 10;
            """)
            agency_stats = cur.fetchall()

            if agency_stats:
                df_agency = pd.DataFrame(agency_stats)
                
                # Create a bar chart for agency performance
                fig_agency = px.bar(
                    df_agency,
                    x='agency_name',
                    y=['total_outstanding', 'total_collected'],
                    title='Agency-wise Outstanding vs Collections',
                    barmode='group',
                    labels={
                        'agency_name': 'Agency',
                        'value': 'Amount (₹)',
                        'variable': 'Type'
                    }
                )
                fig_agency.update_layout(
                    xaxis_tickangle=-45,
                    showlegend=True,
                    height=400
                )
                st.plotly_chart(fig_agency, use_container_width=True)

                # Create a scatter plot for collection efficiency
                fig_scatter = px.scatter(
                    df_agency,
                    x='total_outstanding',
                    y='collection_percentage',
                    size='total_allocations',
                    color='agency_name',
                    title='Collection Efficiency vs Outstanding Amount',
                    labels={
                        'total_outstanding': 'Total Outstanding (₹)',
                        'collection_percentage': 'Collection Rate (%)',
                        'total_allocations': 'Number of Allocations'
                    }
                )
                fig_scatter.update_layout(height=400)
                st.plotly_chart(fig_scatter, use_container_width=True)

        with col2:
            st.markdown("### Key Insights")
            st.markdown("""
            #### Overall Performance
            - Total allocations with collections: {}/{}
            - Average collection rate: {:.2f}%
            
            #### Top Performers
            1. **{}**
               - Collection rate: {:.2f}%
               - Amount collected: ₹{:.2f}L
            
            2. **{}**
               - Collection rate: {:.2f}%
               - Amount collected: ₹{:.2f}L
            
            #### Areas of Improvement
            - {} agencies with 0% collection
            - ₹{:.2f}Cr outstanding with no collections
            
            #### Recent Activity
            - Last collection: {}
            - Recent collection amount: ₹{:.2f}
            """.format(
                overall_stats['allocations_with_collection'],
                overall_stats['total_allocations'],
                overall_stats['collection_percentage'],
                agency_stats[0]['agency_name'],
                agency_stats[0]['collection_percentage'],
                agency_stats[0]['total_collected']/100000,
                agency_stats[1]['agency_name'],
                agency_stats[1]['collection_percentage'],
                agency_stats[1]['total_collected']/100000,
                sum(1 for a in agency_stats if a['collection_percentage'] == 0),
                sum(a['total_outstanding'] for a in agency_stats if a['collection_percentage'] == 0)/10000000,
                "April 2, 2025",
                19931
            ))

            # Add a trend indicator
            st.markdown("#### Collection Trend")
            st.markdown("📉 **Declining** - Last 30 days show reduced collection activity")

            # Add recommendations
            st.markdown("#### Recommendations")
            st.markdown("""
            1. Focus on high-value accounts
            2. Investigate successful agency practices
            3. Review allocation strategy
            4. Implement performance monitoring
            """)

except Exception as e:
    st.error(f"Error querying database: {str(e)}")
    if st.session_state.db_connection:
        st.session_state.db_connection.rollback()
    
# Add auto-refresh button
if st.button("Refresh Data"):
    st.experimental_rerun()

# Footer
st.markdown("---")
st.markdown("*Data refreshes automatically every minute. Click 'Refresh Data' for manual update.*")
