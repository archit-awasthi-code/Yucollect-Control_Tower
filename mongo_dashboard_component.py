import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import dns.resolver
import socket
import time

# Load environment variables
load_dotenv()

def init_mongo_connection():
    """Initialize MongoDB connection with multiple fallback options"""
    # Direct URI approach
    mongo_uri = "mongodb+srv://qa_ihs_mongo_svc:cwUo8Zzy52CBhUhf@nonprd-yucollect-cluster-pl-0.ax1ah.mongodb.net/qa_ihs"
    
    try:
        # Try to resolve the hostname first to check connectivity
        print("Checking MongoDB hostname resolution...")
        try:
            # Set custom DNS resolver with shorter timeout
            resolver = dns.resolver.Resolver()
            resolver.timeout = 3
            resolver.lifetime = 3
            answers = resolver.resolve('nonprd-yucollect-cluster-pl-0.ax1ah.mongodb.net', 'A')
            print(f"MongoDB hostname resolved to: {answers[0].address}")
            hostname_resolvable = True
        except Exception as dns_error:
            print(f"MongoDB hostname resolution failed: {str(dns_error)}")
            hostname_resolvable = False
        
        if not hostname_resolvable:
            print("Cannot resolve MongoDB hostname. Connection will likely fail.")
            return None
            
        # Try to connect with direct URI
        print("Attempting to connect to MongoDB...")
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        # Test connection
        client.admin.command('ping')
        print("Successfully connected to MongoDB")
        return client.get_database("qa_ihs")
    except Exception as e:
        print(f"MongoDB connection error: {str(e)}")
        st.error(f"Failed to connect to MongoDB: {str(e)}")
        return None

def render_mongo_dashboard():
    """Render MongoDB dashboard components"""
    st.markdown("## MongoDB Metrics")
    
    # Try to connect to MongoDB
    mongo_db = init_mongo_connection()
    
    if not mongo_db:
        st.warning("⚠️ Could not connect to MongoDB. Please check your network connection or VPN.")
        st.info("The MongoDB server (nonprd-yucollect-cluster-pl-0.ax1ah.mongodb.net) is not accessible from your current network.")
        
        # Show placeholder metrics
        st.markdown("### Sample MongoDB Metrics (Placeholder)")
        st.markdown("These are placeholder metrics. Connect to the MongoDB server to see actual data.")
        
        # Create some placeholder metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Documents", "N/A", delta=None)
        with col2:
            st.metric("Collections", "N/A", delta=None)
        with col3:
            st.metric("Average Document Size", "N/A", delta=None)
            
        # Create a placeholder chart
        placeholder_data = pd.DataFrame({
            'Category': ['A', 'B', 'C', 'D', 'E'],
            'Value': [10, 20, 15, 25, 30]
        })
        
        fig = px.bar(
            placeholder_data,
            x='Category',
            y='Value',
            title='Sample Distribution (Placeholder)',
            labels={'Value': 'Count', 'Category': 'Type'}
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        return
    
    # If we successfully connected to MongoDB
    st.success("✅ Successfully connected to MongoDB")
    
    # Get all collections
    collections = mongo_db.list_collection_names()
    
    # Display basic metrics
    total_collections = len(collections)
    total_documents = sum(mongo_db[coll].count_documents({}) for coll in collections)
    
    # Display metrics in cards
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Collections", total_collections)
    with col2:
        st.metric("Total Documents", f"{total_documents:,}")
    
    # Display collection breakdown
    st.subheader("Collection Breakdown")
    
    # Get document counts for each collection
    collection_stats = []
    for coll in collections:
        doc_count = mongo_db[coll].count_documents({})
        collection_stats.append({
            "Collection": coll,
            "Documents": doc_count
        })
    
    # Convert to DataFrame and sort
    df_collections = pd.DataFrame(collection_stats)
    df_collections = df_collections.sort_values("Documents", ascending=False)
    
    # Display as bar chart
    fig = px.bar(
        df_collections,
        x="Collection",
        y="Documents",
        title="Document Count by Collection",
        labels={"Documents": "Number of Documents", "Collection": "Collection Name"}
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show collection details
    st.subheader("Collection Details")
    
    # Create tabs for each collection
    tabs = st.tabs(collections)
    
    for i, tab in enumerate(tabs):
        coll_name = collections[i]
        with tab:
            # Get sample document
            sample = mongo_db[coll_name].find_one()
            
            if sample:
                # Show document structure
                st.markdown(f"#### Document Structure for `{coll_name}`")
                
                # Extract field names and types
                fields = []
                for key, value in sample.items():
                    field_type = type(value).__name__
                    fields.append({
                        "Field": key,
                        "Type": field_type,
                        "Example": str(value)[:50] + ('...' if len(str(value)) > 50 else '')
                    })
                
                # Display as table
                st.table(pd.DataFrame(fields))
                
                # Suggest metrics
                st.markdown("#### Potential Metrics")
                
                # Time-based fields
                time_fields = [f["Field"] for f in fields if any(time_word in f["Field"].lower() for time_word in ['time', 'date', 'created', 'updated', 'timestamp'])]
                if time_fields:
                    st.markdown(f"- Time-series analysis based on: {', '.join(time_fields)}")
                
                # Numeric fields
                numeric_fields = [f["Field"] for f in fields if f["Type"] in ['int', 'float', 'long']]
                if numeric_fields:
                    st.markdown(f"- Quantity metrics using: {', '.join(numeric_fields)}")
                
                # Status/category fields
                status_fields = [f["Field"] for f in fields if any(status_word in f["Field"].lower() for status_word in ['status', 'state', 'type', 'category'])]
                if status_fields:
                    st.markdown(f"- Distribution analysis based on: {', '.join(status_fields)}")
            else:
                st.info(f"No documents found in collection {coll_name}")

if __name__ == "__main__":
    # Page config
    st.set_page_config(
        page_title="MongoDB Metrics Dashboard",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("MongoDB Metrics Dashboard")
    render_mongo_dashboard()
