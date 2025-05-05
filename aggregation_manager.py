import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class AggregationManager:
    """
    Centralized aggregation management for the Control Tower dashboard.
    Provides consistent data transformation and aggregation across all dashboard pages.
    """
    
    @staticmethod
    def format_currency(amount, precision=2):
        """Format amount as currency with ₹ symbol"""
        if amount is None or pd.isna(amount):
            return "₹0"
        
        if amount < 100000:  # Less than 1 lakh
            return f"₹{amount:,.{precision}f}"
        elif amount < 10000000:  # Less than 1 crore
            return f"₹{amount/100000:.{precision}f}L"
        else:  # 1 crore or more
            return f"₹{amount/10000000:.{precision}f}Cr"
    
    @staticmethod
    def format_date(date, format_str="%d-%b-%Y"):
        """Format date consistently across the dashboard"""
        if date is None or pd.isna(date) or str(date).lower() == 'nat':
            return "N/A"
        try:
            if isinstance(date, str):
                date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
            return date.strftime(format_str)
        except:
            return "N/A"
    
    @staticmethod
    def calculate_percentage(numerator, denominator, precision=2):
        """Calculate percentage with proper handling of edge cases"""
        if denominator is None or pd.isna(denominator) or denominator == 0:
            return 0.0
        if numerator is None or pd.isna(numerator):
            return 0.0
        return round((numerator / denominator) * 100, precision)
    
    @staticmethod
    def aggregate_by_time(df, date_column, value_column, freq='D', agg_func='sum'):
        """
        Aggregate data by time period
        
        Parameters:
        - df: DataFrame containing the data
        - date_column: Column name containing dates
        - value_column: Column name containing values to aggregate
        - freq: Frequency for aggregation ('D' for daily, 'W' for weekly, 'M' for monthly)
        - agg_func: Aggregation function ('sum', 'mean', 'count', etc.)
        
        Returns:
        - Aggregated DataFrame
        """
        if df is None or df.empty:
            return pd.DataFrame()
        
        # Ensure date column is datetime
        df[date_column] = pd.to_datetime(df[date_column])
        
        # Group by time period and aggregate
        grouped = df.groupby(pd.Grouper(key=date_column, freq=freq))
        
        if agg_func == 'sum':
            result = grouped[value_column].sum().reset_index()
        elif agg_func == 'mean':
            result = grouped[value_column].mean().reset_index()
        elif agg_func == 'count':
            result = grouped[value_column].count().reset_index()
        else:
            result = grouped.agg({value_column: agg_func}).reset_index()
        
        return result
    
    @staticmethod
    def calculate_growth(current_value, previous_value):
        """Calculate growth percentage between two values"""
        if previous_value is None or pd.isna(previous_value) or previous_value == 0:
            return None
        if current_value is None or pd.isna(current_value):
            return -100.0
        
        growth = ((current_value - previous_value) / previous_value) * 100
        return round(growth, 2)
    
    @staticmethod
    def filter_dataframe(df, filters):
        """
        Apply multiple filters to a DataFrame
        
        Parameters:
        - df: DataFrame to filter
        - filters: Dictionary of {column: value} pairs or {column: [list of values]}
        
        Returns:
        - Filtered DataFrame
        """
        if df is None or df.empty:
            return pd.DataFrame()
        
        filtered_df = df.copy()
        
        for column, value in filters.items():
            if column not in filtered_df.columns:
                continue
                
            if isinstance(value, list):
                filtered_df = filtered_df[filtered_df[column].isin(value)]
            else:
                filtered_df = filtered_df[filtered_df[column] == value]
        
        return filtered_df
    
    @staticmethod
    def search_dataframe(df, search_term, columns=None):
        """
        Search for a term across specified columns (or all columns if not specified)
        
        Parameters:
        - df: DataFrame to search
        - search_term: Term to search for
        - columns: List of columns to search in (None for all columns)
        
        Returns:
        - DataFrame with matching rows
        """
        if df is None or df.empty or not search_term:
            return df
        
        search_columns = columns if columns is not None else df.columns
        search_term = str(search_term).lower()
        
        mask = pd.Series(False, index=df.index)
        for column in search_columns:
            if column in df.columns:
                column_mask = df[column].astype(str).str.lower().str.contains(search_term, na=False)
                mask = mask | column_mask
        
        return df[mask]
