import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Metrics Visualizer",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title
st.title("Metrics Data Visualizer")

# File upload section
uploaded_file = st.file_uploader("Upload your metrics Excel file", type=['xlsx', 'xls'])

if uploaded_file is not None:
    try:
        # Read Excel file
        df = pd.read_excel(uploaded_file)
        
        # File info
        st.sidebar.success(f"""
        📊 File loaded successfully!
        - Rows: {len(df)}
        - Columns: {len(df.columns)}
        """)
        
        # Column selection
        st.sidebar.header("Configure Visualizations")
        
        # Get numeric columns for metrics
        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
        # Get datetime and string columns for dimensions
        dimension_cols = df.select_dtypes(include=['datetime64', 'object']).columns.tolist()
        
        # Visualization options
        viz_type = st.sidebar.selectbox(
            "Select Visualization Type",
            ["Line Chart", "Bar Chart", "Scatter Plot", "Pie Chart", "Area Chart", "Box Plot"]
        )
        
        # Main metrics selection
        metric_col = st.sidebar.selectbox("Select Metric (Y-axis)", numeric_cols)
        
        # Dimension selection (for grouping/x-axis)
        dimension_col = st.sidebar.selectbox("Select Dimension (X-axis/Grouping)", dimension_cols)
        
        # Additional options based on chart type
        if viz_type in ["Scatter Plot"]:
            size_col = st.sidebar.selectbox("Select Size Metric", ["None"] + numeric_cols)
            color_col = st.sidebar.selectbox("Select Color Dimension", ["None"] + dimension_cols)
        
        # Create visualization
        st.header("Visualization")
        
        if viz_type == "Line Chart":
            fig = px.line(
                df,
                x=dimension_col,
                y=metric_col,
                title=f"{metric_col} over {dimension_col}"
            )
            
        elif viz_type == "Bar Chart":
            agg_func = st.sidebar.selectbox("Aggregation", ["sum", "mean", "count"])
            df_grouped = df.groupby(dimension_col)[metric_col].agg(agg_func).reset_index()
            fig = px.bar(
                df_grouped,
                x=dimension_col,
                y=metric_col,
                title=f"{agg_func.capitalize()}of {metric_col} by {dimension_col}"
            )
            
        elif viz_type == "Scatter Plot":
            fig = px.scatter(
                df,
                x=dimension_col,
                y=metric_col,
                size=None if size_col == "None" else size_col,
                color=None if color_col == "None" else color_col,
                title=f"{metric_col} vs {dimension_col}"
            )
            
        elif viz_type == "Pie Chart":
            df_grouped = df.groupby(dimension_col)[metric_col].sum().reset_index()
            fig = px.pie(
                df_grouped,
                values=metric_col,
                names=dimension_col,
                title=f"Distribution of {metric_col} by {dimension_col}"
            )
            
        elif viz_type == "Area Chart":
            fig = px.area(
                df,
                x=dimension_col,
                y=metric_col,
                title=f"{metric_col} Area over {dimension_col}"
            )
            
        elif viz_type == "Box Plot":
            fig = px.box(
                df,
                x=dimension_col,
                y=metric_col,
                title=f"Distribution of {metric_col} by {dimension_col}"
            )
        
        # Update layout
        fig.update_layout(
            height=600,
            showlegend=True,
            xaxis_title=dimension_col,
            yaxis_title=metric_col
        )
        
        # Display the plot
        st.plotly_chart(fig, use_container_width=True)
        
        # Data Summary
        st.header("Data Summary")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Basic Statistics")
            st.dataframe(df[metric_col].describe())
        
        with col2:
            st.subheader("Top Values")
            st.dataframe(df.nlargest(5, metric_col)[[dimension_col, metric_col]])
        
        # Download options
        st.header("Export Options")
        
        # Export visualization
        if st.button("Export Visualization"):
            # Save plot as HTML
            fig.write_html(f"visualization_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
            st.success("Visualization exported as HTML!")
        
        # Export processed data
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Download Data as CSV",
            csv,
            f"metrics_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            "text/csv",
            key='download-csv'
        )
        
    except Exception as e:
        st.error(f"Error processing file: {str(e)}")
else:
    st.info("Please upload an Excel file to begin visualization.")
    
    # Example format
    st.header("Expected Data Format")
    st.markdown("""
    Your Excel file should contain:
    1. At least one numeric column for metrics
    2. One or more columns for dimensions (dates, categories, etc.)
    
    Example format:
    | Date | Category | Revenue | Customers | Conversion Rate |
    |------|----------|---------|-----------|-----------------|
    | ...  | ...      | ...     | ...       | ...            |
    """)
