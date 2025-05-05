import os
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set page config
st.set_page_config(
    page_title="Borrower Details Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection function
def get_db_connection():
    """Create a connection to the UCF database"""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_UCF_DB_NAME', 'qa-ucf'),
            user=os.getenv('POSTGRES_UCF_DB_USER', 'ucf_svc'),
            password=os.getenv('POSTGRES_UCF_DB_PASSWORD', 'VfdKEJvqb4ki2qeQGL'),
            host=os.getenv('POSTGRES_UCF_DB_HOST', 'qa-postgres-rds.yucollect.in'),
            port=os.getenv('POSTGRES_UCF_DB_PORT', 5432),
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None

# Function to format currency values
def format_currency(value):
    if pd.isna(value) or value is None:
        return "₹0"
    
    if value < 100000:  # Less than 1 lakh
        return f"₹{value:,.2f}"
    elif value < 10000000:  # Less than 1 crore
        return f"₹{value/100000:.2f}L"
    else:  # 1 crore or more
        return f"₹{value/10000000:.2f}Cr"

# Function to load borrower data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_borrower_data(limit=None):
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        with conn.cursor() as cur:
            query = """
                SELECT 
                    id, name, phone_number, email, loan_sanctioned_date, 
                    loan_sanctioned_amount, total_dues, total_outstanding_amount,
                    due_date, channel, is_closed, customer_city, customer_state,
                    loan_type, credit_score, employment_status, monthly_income,
                    gender, date_of_birth, status, days_past_due, risk_tagging,
                    bucket_type, last_payment_date, last_payment_amount,
                    created_at, updated_at
                FROM borrower_details
                WHERE is_active = true
            """
            
            if limit:
                query += f" LIMIT {limit}"
                
            cur.execute(query)
            data = cur.fetchall()
            return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None
    finally:
        conn.close()

# Function to get summary metrics
def get_summary_metrics():
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) as total_borrowers,
                    COUNT(CASE WHEN is_closed = true THEN 1 END) as closed_accounts,
                    COUNT(CASE WHEN is_closed = false OR is_closed IS NULL THEN 1 END) as active_accounts,
                    SUM(loan_sanctioned_amount) as total_sanctioned,
                    SUM(total_outstanding_amount) as total_outstanding,
                    AVG(CASE WHEN credit_score IS NOT NULL THEN credit_score ELSE NULL END) as avg_credit_score,
                    COUNT(DISTINCT loan_type) as loan_types,
                    COUNT(DISTINCT customer_state) as states_covered
                FROM borrower_details
                WHERE is_active = true
            """)
            return cur.fetchone()
    except Exception as e:
        st.error(f"Error getting metrics: {e}")
        return None
    finally:
        conn.close()

# Main dashboard
def main():
    # Title and description
    st.title("Borrower Details Dashboard")
    st.markdown("Dashboard showing key metrics from the borrower_details table")
    
    # Date filters
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", datetime.now() - timedelta(days=30))
    with col2:
        end_date = st.date_input("End Date", datetime.now())
    
    # Load data
    with st.spinner("Loading data..."):
        df = load_borrower_data()
        
    if df is None or df.empty:
        st.warning("No data available. Please check your database connection.")
        return
    
    # Get summary metrics
    metrics = get_summary_metrics()
    if metrics:
        # Display metrics in cards
        st.subheader("Summary Metrics")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Borrowers", metrics['total_borrowers'])
            st.metric("Active Accounts", metrics['active_accounts'])
        
        with col2:
            st.metric("Total Sanctioned", format_currency(metrics['total_sanctioned']))
            st.metric("Total Outstanding", format_currency(metrics['total_outstanding']))
        
        with col3:
            st.metric("Avg Credit Score", f"{metrics['avg_credit_score']:.1f}" if metrics['avg_credit_score'] else "N/A")
            st.metric("Loan Types", metrics['loan_types'])
        
        with col4:
            st.metric("Closed Accounts", metrics['closed_accounts'])
            st.metric("States Covered", metrics['states_covered'])
    
    # Data exploration section
    st.subheader("Data Exploration")
    
    # Tabs for different visualizations
    tab1, tab2, tab3, tab4 = st.tabs(["Loan Distribution", "Geographic Analysis", "Risk Analysis", "Raw Data"])
    
    with tab1:
        st.subheader("Loan Distribution")
        
        # Loan type distribution
        if 'loan_type' in df.columns:
            loan_type_counts = df['loan_type'].value_counts().reset_index()
            loan_type_counts.columns = ['Loan Type', 'Count']
            
            fig = px.pie(loan_type_counts, values='Count', names='Loan Type', 
                         title='Distribution by Loan Type',
                         hole=0.4)
            st.plotly_chart(fig, use_container_width=True)
        
        # Outstanding amount distribution
        if 'total_outstanding_amount' in df.columns:
            # Create buckets for outstanding amounts
            df['outstanding_bucket'] = pd.cut(
                df['total_outstanding_amount'], 
                bins=[0, 100000, 500000, 1000000, 5000000, float('inf')],
                labels=['0-1L', '1L-5L', '5L-10L', '10L-50L', '50L+']
            )
            
            outstanding_counts = df['outstanding_bucket'].value_counts().reset_index()
            outstanding_counts.columns = ['Outstanding Range', 'Count']
            
            fig = px.bar(outstanding_counts, x='Outstanding Range', y='Count',
                         title='Distribution by Outstanding Amount',
                         color='Outstanding Range')
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("Geographic Analysis")
        
        # State-wise distribution
        if 'customer_state' in df.columns:
            state_counts = df['customer_state'].value_counts().reset_index()
            state_counts.columns = ['State', 'Count']
            
            fig = px.bar(state_counts.head(10), x='State', y='Count',
                         title='Top 10 States by Borrower Count',
                         color='Count')
            st.plotly_chart(fig, use_container_width=True)
        
        # City-wise distribution
        if 'customer_city' in df.columns:
            city_counts = df['customer_city'].value_counts().reset_index()
            city_counts.columns = ['City', 'Count']
            
            fig = px.bar(city_counts.head(10), x='City', y='Count',
                         title='Top 10 Cities by Borrower Count',
                         color='Count')
            st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("Risk Analysis")
        
        # Credit score distribution
        if 'credit_score' in df.columns:
            # Filter out null values
            credit_df = df[df['credit_score'].notna()]
            
            # Create buckets for credit scores
            credit_df['credit_bucket'] = pd.cut(
                credit_df['credit_score'], 
                bins=[0, 300, 500, 700, 900],
                labels=['Poor (0-300)', 'Fair (300-500)', 'Good (500-700)', 'Excellent (700-900)']
            )
            
            credit_counts = credit_df['credit_bucket'].value_counts().reset_index()
            credit_counts.columns = ['Credit Score Range', 'Count']
            
            fig = px.pie(credit_counts, values='Count', names='Credit Score Range', 
                         title='Distribution by Credit Score',
                         color_discrete_sequence=px.colors.sequential.RdBu)
            st.plotly_chart(fig, use_container_width=True)
        
        # Risk tagging distribution
        if 'risk_tagging' in df.columns:
            risk_counts = df['risk_tagging'].value_counts().reset_index()
            risk_counts.columns = ['Risk Category', 'Count']
            
            fig = px.bar(risk_counts, x='Risk Category', y='Count',
                         title='Distribution by Risk Category',
                         color='Risk Category')
            st.plotly_chart(fig, use_container_width=True)
        
        # Days past due distribution
        if 'days_past_due' in df.columns:
            # Filter out null values
            dpd_df = df[df['days_past_due'].notna()]
            
            # Create buckets for days past due
            dpd_df['dpd_bucket'] = pd.cut(
                dpd_df['days_past_due'], 
                bins=[-1, 0, 30, 60, 90, float('inf')],
                labels=['Current', '1-30 days', '31-60 days', '61-90 days', '90+ days']
            )
            
            dpd_counts = dpd_df['dpd_bucket'].value_counts().reset_index()
            dpd_counts.columns = ['DPD Range', 'Count']
            
            fig = px.bar(dpd_counts, x='DPD Range', y='Count',
                         title='Distribution by Days Past Due',
                         color='DPD Range',
                         color_discrete_sequence=px.colors.sequential.Reds)
            st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.subheader("Raw Data")
        st.dataframe(df.head(100))
        
        # Allow downloading the data
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download Data as CSV",
            data=csv,
            file_name="borrower_details.csv",
            mime="text/csv",
        )

if __name__ == "__main__":
    main()
