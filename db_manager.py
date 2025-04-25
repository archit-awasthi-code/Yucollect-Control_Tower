import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient
import streamlit as st

# Load environment variables
load_dotenv()

class DatabaseManager:
    def __init__(self):
        # Ingestion Service Database Config
        self.pg_ingestion_config = {
            "dbname": os.getenv('POSTGRES_INGESTION_DB_NAME'),
            "user": os.getenv('POSTGRES_INGESTION_DB_USER'),
            "password": os.getenv('POSTGRES_INGESTION_DB_PASSWORD'),
            "host": os.getenv('POSTGRES_INGESTION_DB_HOST'),
            "port": os.getenv('POSTGRES_INGESTION_DB_PORT', 5432)
        }
        
        # Entity Management Database Config
        self.pg_entity_config = {
            "dbname": os.getenv('POSTGRES_ENTITY_DB_NAME'),
            "user": os.getenv('POSTGRES_ENTITY_DB_USER'),
            "password": os.getenv('POSTGRES_ENTITY_DB_PASSWORD'),
            "host": os.getenv('POSTGRES_ENTITY_DB_HOST'),
            "port": os.getenv('POSTGRES_ENTITY_DB_PORT', 5432)
        }
        
        self.mongo_config = {
            "database": os.getenv('MONGO_DB_NAME'),
            "username": os.getenv('MONGO_DB_USER'),
            "password": os.getenv('MONGO_DB_PASSWORD'),
            "cluster": os.getenv('MONGO_DB_CLUSTER')
        }
        
        # Initialize connections as None
        self.pg_ingestion_conn = None
        self.pg_entity_conn = None
        self.mongo_client = None
        self.mongo_db = None

    def init_postgres_ingestion(self):
        """Initialize Ingestion Service PostgreSQL connection"""
        try:
            if not self.pg_ingestion_conn or self.pg_ingestion_conn.closed:
                print(f"Connecting to ingestion DB with config: {self.pg_ingestion_config}")  # Debug print
                self.pg_ingestion_conn = psycopg2.connect(
                    cursor_factory=RealDictCursor,
                    connect_timeout=10,
                    **self.pg_ingestion_config
                )
            return True
        except Exception as e:
            print(f"Ingestion DB connection error: {str(e)}")  # Debug print
            st.error(f"Failed to connect to Ingestion Service DB: {str(e)}")
            return False

    def init_postgres_entity(self):
        """Initialize Entity Management PostgreSQL connection"""
        try:
            if not self.pg_entity_conn or self.pg_entity_conn.closed:
                print(f"Connecting to entity DB with config: {self.pg_entity_config}")  # Debug print
                self.pg_entity_conn = psycopg2.connect(
                    cursor_factory=RealDictCursor,
                    connect_timeout=10,
                    **self.pg_entity_config
                )
            return True
        except Exception as e:
            print(f"Entity DB connection error: {str(e)}")  # Debug print
            st.error(f"Failed to connect to Entity Management DB: {str(e)}")
            return False

    def init_mongo(self):
        """Initialize MongoDB connection"""
        try:
            if not self.mongo_client:
                # MongoDB Atlas connection string format
                mongo_uri = f"mongodb+srv://{self.mongo_config['username']}:{self.mongo_config['password']}@{self.mongo_config['cluster']}/?retryWrites=true&w=majority"
                self.mongo_client = MongoClient(mongo_uri)
                self.mongo_db = self.mongo_client[self.mongo_config['database']]
            return True
        except Exception as e:
            print(f"MongoDB connection error: {str(e)}")  # Debug print
            return False

    def get_ingestion_cursor(self):
        """Get an Ingestion Service PostgreSQL cursor"""
        if self.init_postgres_ingestion():
            return self.pg_ingestion_conn.cursor()
        return None

    def get_entity_cursor(self):
        """Get an Entity Management PostgreSQL cursor"""
        if self.init_postgres_entity():
            return self.pg_entity_conn.cursor()
        return None

    def get_mongo_db(self):
        """Get MongoDB database connection"""
        if self.init_mongo():
            return self.mongo_db
        return None

    def close_connections(self):
        """Close all database connections"""
        if self.pg_ingestion_conn:
            self.pg_ingestion_conn.close()
        if self.pg_entity_conn:
            self.pg_entity_conn.close()
        if self.mongo_client:
            self.mongo_client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connections()
