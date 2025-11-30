"""
Script to load CSV files into PostgreSQL raw schema tables.
"""

import csv
import psycopg2
from pathlib import Path
import sys

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'sl_db',
    'user': None,  # Will use default PostgreSQL user
    'password': None
}

CSV_FILES = {
    'org': 'csv_data/org.csv',
    'platform': 'csv_data/platform.csv',
    'outlet': 'csv_data/outlet.csv',
    'listing': 'csv_data/listing.csv',
    'orders': 'csv_data/orders.csv',
    'orders_daily': 'csv_data/orders_daily.csv',
    'ratings_agg': 'csv_data/ratings_agg.csv',
    'rank': 'csv_data/rank.csv',
}

# Weather CSV files (optional - loaded from data/weather/)
WEATHER_CSV_DIR = 'data/weather'

def get_table_schema(table_name):
    """Return CREATE TABLE SQL for each table."""
    schemas = {
        'org': """
            CREATE TABLE IF NOT EXISTS raw.org (
                id INTEGER,
                name VARCHAR,
                timestamp VARCHAR
            );
        """,
        'platform': """
            CREATE TABLE IF NOT EXISTS raw.platform (
                id INTEGER,
                "group" VARCHAR,
                name VARCHAR,
                country VARCHAR
            );
        """,
        'outlet': """
            CREATE TABLE IF NOT EXISTS raw.outlet (
                id INTEGER,
                org_id INTEGER,
                name VARCHAR,
                latitude VARCHAR,
                longitude VARCHAR,
                timestamp VARCHAR
            );
        """,
        'listing': """
            CREATE TABLE IF NOT EXISTS raw.listing (
                id INTEGER,
                outlet_id INTEGER,
                platform_id INTEGER,
                timestamp VARCHAR
            );
        """,
        'orders': """
            CREATE TABLE IF NOT EXISTS raw.orders (
                listing_id INTEGER,
                order_id INTEGER,
                placed_at VARCHAR,
                status VARCHAR
            );
        """,
        'orders_daily': """
            CREATE TABLE IF NOT EXISTS raw.orders_daily (
                date VARCHAR,
                listing_id INTEGER,
                orders INTEGER,
                timestamp VARCHAR
            );
        """,
        'ratings_agg': """
            CREATE TABLE IF NOT EXISTS raw.ratings_agg (
                date VARCHAR,
                listing_id INTEGER,
                cnt_ratings INTEGER,
                avg_rating NUMERIC
            );
        """,
        'rank': """
            CREATE TABLE IF NOT EXISTS raw.rank (
                listing_id INTEGER,
                date VARCHAR,
                timestamp VARCHAR,
                is_online VARCHAR,
                rank VARCHAR
            );
        """,
        'weather': """
            CREATE TABLE IF NOT EXISTS raw.weather (
                outlet_id INTEGER,
                outlet_name VARCHAR,
                latitude NUMERIC,
                longitude NUMERIC,
                datetime VARCHAR,
                wind_speed_10m NUMERIC,
                temperature_2m NUMERIC,
                relative_humidity_2m NUMERIC
            );
        """,
    }
    return schemas.get(table_name)

def load_csv_to_table(conn, table_name, csv_path):
    """Load CSV file into PostgreSQL table."""
    print(f"Loading {table_name}...", end=" ")
    
    # Create table
    cursor = conn.cursor()
    cursor.execute(get_table_schema(table_name))
    conn.commit()
    
    # Truncate table if it exists
    cursor.execute(f'TRUNCATE TABLE raw.{table_name};')
    conn.commit()
    
    # Read CSV and insert data
    csv_file = Path(__file__).parent.parent / csv_path
    if not csv_file.exists():
        print(f"✗ File not found: {csv_path}")
        return False
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        
        if not rows:
            print("✗ No data")
            return False
        
        # Get column names
        columns = list(rows[0].keys())
        
        # Prepare insert statement
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join([f'"{col}"' if col in ['group', 'date'] else col for col in columns])
        
        insert_sql = f'INSERT INTO raw.{table_name} ({column_names}) VALUES ({placeholders})'
        
        # Insert rows in batches
        batch_size = 1000
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            values = [[row[col] for col in columns] for row in batch]
            cursor.executemany(insert_sql, values)
            conn.commit()
    
    cursor.close()
    print(f"✓ {len(rows)} rows loaded")
    return True

def main():
    """Main function."""
    project_root = Path(__file__).parent.parent
    
    print("=" * 60)
    print("Loading CSV files into PostgreSQL")
    print("=" * 60)
    
    try:
        # Connect to database
        conn = psycopg2.connect(**{k: v for k, v in DB_CONFIG.items() if v is not None})
        print(f"Connected to database: {DB_CONFIG['database']}")
        print()
        
        # Load each CSV file
        success_count = 0
        for table_name, csv_path in CSV_FILES.items():
            if load_csv_to_table(conn, table_name, csv_path):
                success_count += 1
        
        # Load weather CSV files (append, don't truncate)
        weather_dir = project_root / WEATHER_CSV_DIR
        if weather_dir.exists():
            weather_files = sorted(weather_dir.glob('weather_*.csv'))
            if weather_files:
                print()
                print("Loading weather data...")
                cursor = conn.cursor()
                cursor.execute(get_table_schema('weather'))
                conn.commit()
                
                weather_count = 0
                for weather_file in weather_files:
                    print(f"Loading {weather_file.name}...", end=" ")
                    try:
                        with open(weather_file, 'r', encoding='utf-8') as f:
                            reader = csv.DictReader(f)
                            rows = list(reader)
                            
                            if rows:
                                csv_columns = list(rows[0].keys())
                                table_columns = ['datetime' if col == 'time' else col for col in csv_columns]
                                
                                placeholders = ', '.join(['%s'] * len(table_columns))
                                column_names = ', '.join(table_columns)
                                insert_sql = f'INSERT INTO raw.weather ({column_names}) VALUES ({placeholders})'
                                
                                batch_size = 1000
                                for i in range(0, len(rows), batch_size):
                                    batch = rows[i:i+batch_size]
                                    values = [[row[col] for col in csv_columns] for row in batch]
                                    cursor.executemany(insert_sql, values)
                                    conn.commit()
                                
                                weather_count += len(rows)
                                print(f"✓ {len(rows)} rows")
                            else:
                                print("✗ No data")
                    except Exception as e:
                        print(f"✗ Error: {e}")
                
                cursor.close()
                if weather_count > 0:
                    print(f"Total weather records loaded: {weather_count}")
        
        conn.close()
        
        print()
        print("=" * 60)
        print(f"✓ Successfully loaded {success_count}/{len(CSV_FILES)} tables")
        if weather_count > 0:
            print(f"✓ Loaded {weather_count} weather records")
        print("=" * 60)
        
    except psycopg2.OperationalError as e:
        print(f"\n✗ Database connection error: {e}")
        print("\nMake sure PostgreSQL is running:")
        print("  brew services start postgresql@15")
        print("\nOr start manually:")
        print("  pg_ctl -D /opt/homebrew/var/postgresql@15 start")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

