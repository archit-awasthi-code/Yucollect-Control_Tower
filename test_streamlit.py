import streamlit as st
import os

st.write("Current environment:")
st.write(f"DYLD_LIBRARY_PATH: {os.environ.get('DYLD_LIBRARY_PATH')}")
st.write(f"DYLD_FALLBACK_LIBRARY_PATH: {os.environ.get('DYLD_FALLBACK_LIBRARY_PATH')}")
st.write(f"PATH: {os.environ.get('PATH')}")

try:
    import psycopg2
    st.success("Successfully imported psycopg2")
    st.write(f"psycopg2 version: {psycopg2.__version__}")
except Exception as e:
    st.error(f"Error importing psycopg2: {e}")
    import sys
    st.write(f"Python version: {sys.version}")
    st.write(f"Python path: {sys.path}")
