from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Load environment variables
load_dotenv()

app = FastAPI(title="Control Tower API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    return psycopg2.connect(
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT', 5432),
        cursor_factory=RealDictCursor
    )

@app.get("/api/allocation-stats")
async def get_allocation_stats():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get status counts
        cur.execute("""
            SELECT 
                status,
                COUNT(*) as count
            FROM allocation_files
            GROUP BY status
            ORDER BY count DESC;
        """)
        status_counts = cur.fetchall()

        # Get trend data (last 7 days)
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

        # Get allocator stats
        cur.execute("""
            SELECT 
                allocator_id,
                COUNT(*) as total_allocations,
                COUNT(CASE WHEN status = 'fully-allocated' THEN 1 END) as fully_allocated
            FROM allocation_files
            GROUP BY allocator_id
            ORDER BY total_allocations DESC
            LIMIT 5;
        """)
        allocator_stats = cur.fetchall()

        cur.close()
        conn.close()

        return {
            "status_counts": status_counts,
            "trend_data": trend_data,
            "allocator_stats": allocator_stats
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/allocation-details")
async def get_allocation_details(
    status: str = None,
    allocator_id: str = None,
    start_date: str = None,
    end_date: str = None,
    limit: int = 10,
    offset: int = 0
):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        query = "SELECT * FROM allocation_files WHERE 1=1"
        params = []

        if status:
            query += " AND status = %s"
            params.append(status)
        if allocator_id:
            query += " AND allocator_id = %s"
            params.append(allocator_id)
        if start_date:
            query += " AND created_at >= %s"
            params.append(start_date)
        if end_date:
            query += " AND created_at <= %s"
            params.append(end_date)

        query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        cur.execute(query, params)
        results = cur.fetchall()

        cur.close()
        conn.close()

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
