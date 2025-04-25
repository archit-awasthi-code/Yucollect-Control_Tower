# Yucollect Control Tower Dashboard

A comprehensive metrics visualization dashboard for tracking allocation, collection, and user performance across different agencies.

## Features

- Interactive dashboard with multiple metric sections
- Flexible date range filtering
- Key performance metrics visualization
- Agency and user distribution insights
- Platform usage analytics

## Current Sections

1. Summary Section
   - Total Agencies
   - Total Allocators
   - Total Allocations
   - Unique LOBs
   - Total Records
   - Total Outstanding
   - Total Collections
   - Collection Rate

2. User Distribution Section
   - Total Users
   - Total Supervisors
   - Total Call Agents
   - Total Field Agents
   - Active Users
   - Total Time Spent
   - Role Distribution Visualization

## Technical Stack

- Backend: Python
- Frontend: Streamlit
- Visualization: Plotly, Altair
- Database: PostgreSQL
  - Entity Management DB
  - Ingestion Service DB

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables in `.env`:
```
# PostgreSQL Credentials
POSTGRES_INGESTION_DB_PASSWORD=your_password
POSTGRES_ENTITY_DB_PASSWORD=your_password

# Database Configuration
POSTGRES_INGESTION_DB_HOST=your_host
POSTGRES_INGESTION_DB_PORT=5432
POSTGRES_INGESTION_DB_NAME=ingestion-service
POSTGRES_INGESTION_DB_USER=ingestion_svc

POSTGRES_ENTITY_DB_HOST=your_host
POSTGRES_ENTITY_DB_PORT=5432
POSTGRES_ENTITY_DB_NAME=entity-management
POSTGRES_ENTITY_DB_USER=entity_management_svc
```

3. Run the dashboard:
```bash
streamlit run control_tower_dashboard.py
```

## Project Structure

```
control-tower/
├── control_tower_dashboard.py  # Main dashboard application
├── db_manager.py              # Database connection management
├── get_user_metrics.py        # User metrics calculation
├── metrics_visualizer.py      # Visualization components
├── requirements.txt           # Project dependencies
└── .env                      # Environment variables
```

## Current State

- Dashboard layout and styling completed
- Basic metrics integration done
- Database connections established
- Interactive date filtering implemented

## Next Steps

1. Add more detailed metrics
2. Implement advanced filtering
3. Add trend analysis
4. Enhance visualizations
5. Add export functionality

## Version History

- v0.1.0 (Current)
  - Initial dashboard layout
  - Basic metrics implementation
  - User distribution visualization
  - Date range filtering
