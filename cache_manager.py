import streamlit as st
import time
from datetime import datetime, timedelta

class CacheManager:
    """
    Centralized cache management for the Control Tower dashboard.
    Provides consistent caching behavior across all dashboard pages.
    """
    
    @staticmethod
    def get_cached_data(query_func, *args, **kwargs):
        """
        Generic caching wrapper for any data retrieval function.
        Uses Streamlit's built-in caching mechanism.
        
        Parameters:
        - query_func: The function that retrieves data from the database
        - args, kwargs: Arguments to pass to the query function
        
        Returns:
        - The cached result of the query function
        """
        # Apply Streamlit's cache_data decorator dynamically
        cached_func = st.cache_data(ttl=3600)(query_func)  # Cache for 1 hour
        
        try:
            start_time = time.time()
            result = cached_func(*args, **kwargs)
            elapsed_time = time.time() - start_time
            print(f"Query executed in {elapsed_time:.2f} seconds")
            return result
        except Exception as e:
            print(f"Error in cached query: {str(e)}")
            return None
    
    @staticmethod
    def get_date_filtered_data(query_func, start_date, end_date, *args, **kwargs):
        """
        Caching wrapper specifically for date-filtered queries.
        
        Parameters:
        - query_func: The function that retrieves data from the database
        - start_date, end_date: Date range for filtering
        - args, kwargs: Additional arguments to pass to the query function
        
        Returns:
        - The cached result of the query function
        """
        # Apply Streamlit's cache_data decorator dynamically
        cached_func = st.cache_data(ttl=3600)(query_func)  # Cache for 1 hour
        
        try:
            start_time = time.time()
            result = cached_func(start_date, end_date, *args, **kwargs)
            elapsed_time = time.time() - start_time
            print(f"Date-filtered query executed in {elapsed_time:.2f} seconds")
            return result
        except Exception as e:
            print(f"Error in cached date-filtered query: {str(e)}")
            return None
    
    @staticmethod
    def clear_all_caches():
        """Clear all cached data across the dashboard"""
        st.cache_data.clear()
        print("All caches cleared")
