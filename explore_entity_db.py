from db_manager import DatabaseManager
from tabulate import tabulate

def print_schema():
    try:
        with DatabaseManager() as db:
            cur = db.get_entity_cursor()
            if not cur:
                print("Failed to connect to Entity Management database")
                return

            # Get all tables
            cur.execute("""
                SELECT 
                    t.table_name,
                    t.table_type
                FROM information_schema.tables t
                WHERE t.table_schema = 'public'
                ORDER BY t.table_name;
            """)
            tables = cur.fetchall()

            print("\n=== Entity Management Database Schema ===\n")

            for table in tables:
                print(f"\n📋 Table: {table['table_name']}")
                print("=" * (len(table['table_name']) + 8))

                # Get columns for this table
                cur.execute("""
                    SELECT 
                        c.column_name,
                        c.data_type,
                        c.character_maximum_length,
                        c.column_default,
                        c.is_nullable,
                        pgd.description as column_description
                    FROM information_schema.columns c
                    LEFT JOIN pg_catalog.pg_statio_all_tables st 
                        ON c.table_schema = st.schemaname AND c.table_name = st.relname
                    LEFT JOIN pg_catalog.pg_description pgd
                        ON pgd.objoid = st.relid 
                        AND pgd.objsubid = c.ordinal_position
                    WHERE c.table_schema = 'public' 
                        AND c.table_name = %s
                    ORDER BY c.ordinal_position;
                """, (table['table_name'],))
                columns = cur.fetchall()

                # Format column info
                column_data = []
                for col in columns:
                    data_type = col['data_type']
                    if col['character_maximum_length']:
                        data_type += f"({col['character_maximum_length']})"
                    
                    column_data.append([
                        col['column_name'],
                        data_type,
                        'YES' if col['is_nullable'] == 'YES' else 'NO',
                        col['column_default'] or '',
                        col['column_description'] or ''
                    ])

                print(tabulate(
                    column_data,
                    headers=['Column', 'Type', 'Nullable', 'Default', 'Description'],
                    tablefmt='grid'
                ))

                # Get primary key
                cur.execute("""
                    SELECT 
                        tc.constraint_name,
                        kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_schema = 'public' 
                        AND tc.table_name = %s
                        AND tc.constraint_type = 'PRIMARY KEY';
                """, (table['table_name'],))
                pks = cur.fetchall()
                
                if pks:
                    print("\n🔑 Primary Key:")
                    pk_columns = [pk['column_name'] for pk in pks]
                    print(f"    {', '.join(pk_columns)}")

                # Get foreign keys
                cur.execute("""
                    SELECT
                        tc.constraint_name,
                        kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage ccu
                        ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.table_schema = 'public' 
                        AND tc.table_name = %s
                        AND tc.constraint_type = 'FOREIGN KEY';
                """, (table['table_name'],))
                fks = cur.fetchall()

                if fks:
                    print("\n🔗 Foreign Keys:")
                    for fk in fks:
                        print(f"    {fk['column_name']} → {fk['foreign_table_name']}.{fk['foreign_column_name']}")

                # Get indexes
                cur.execute("""
                    SELECT
                        i.relname as index_name,
                        a.attname as column_name,
                        ix.indisunique as is_unique,
                        ix.indisprimary as is_primary
                    FROM pg_class t
                    JOIN pg_index ix ON t.oid = ix.indrelid
                    JOIN pg_class i ON ix.indexrelid = i.oid
                    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                    WHERE t.relkind = 'r'
                        AND t.relname = %s
                        AND i.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
                    ORDER BY i.relname, a.attnum;
                """, (table['table_name'],))
                indexes = cur.fetchall()

                if indexes:
                    print("\n📇 Indexes:")
                    current_index = None
                    index_columns = []
                    
                    for idx in indexes:
                        if current_index != idx['index_name']:
                            if current_index:
                                print(f"    {current_index} ({', '.join(index_columns)})"
                                      f"{' [UNIQUE]' if is_unique else ''}")
                                index_columns = []
                            current_index = idx['index_name']
                            is_unique = idx['is_unique']
                        
                        if not idx['is_primary']:  # Skip primary key indexes
                            index_columns.append(idx['column_name'])
                    
                    if current_index and index_columns:
                        print(f"    {current_index} ({', '.join(index_columns)})"
                              f"{' [UNIQUE]' if is_unique else ''}")

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    print_schema()
