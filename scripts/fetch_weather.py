"""
Simple script to fetch hourly weather data from Open-Meteo API using requests library.
Fetches weather for all outlets from outlet.csv for the last 24 hours.
New outlets are automatically included when added to the CSV.
"""

from typing import Any


import requests
import csv
from datetime import datetime, timedelta
from pathlib import Path


def load_outlets(csv_path):
    """
    Load outlets from CSV file, filter valid coordinates, and deduplicate.
    
    Returns:
        List of dictionaries with outlet information
    """
    outlets = {}
    
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            outlet_id = int(row['id'])
            lat = float(row['latitude']) if row['latitude'] else 0.0
            lon = float(row['longitude']) if row['longitude'] else 0.0
            
            # Skip invalid coordinates
            if lat == 0.0 and lon == 0.0:
                continue
            
            # Keep only first occurrence of each outlet (deduplicate)
            if outlet_id not in outlets:
                outlets[outlet_id] = {
                    'id': outlet_id,
                    'name': row['name'],
                    'latitude': lat,
                    'longitude': lon
                }
    
    return list[Any](outlets.values())


def fetch_weather_data(latitude, longitude, start_date, end_date):
    """
    Fetch hourly weather data from Open-Meteo API.
    
    Args:
        latitude: Latitude of the location
        longitude: Longitude of the location
        start_date: Start date (datetime object)
        end_date: End date (datetime object)
    
    Returns:
        Dictionary with hourly weather data or None if error
    """
    url = "https://api.open-meteo.com/v1/forecast"
    
    # API only accepts dates, not datetime. We'll filter results later.
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "hourly": "wind_speed_10m,temperature_2m,relative_humidity_2m",
        "timezone": "America/New_York"
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None


def parse_weather_data(weather_json, outlet, start_date, end_date):
    """
    Parse weather JSON response into list of records with outlet information.
    Filters to only include data within the specified time range (last 24 hours).
    
    Args:
        weather_json: JSON response from Open-Meteo API
        outlet: Dictionary with outlet information (id, name, latitude, longitude)
        start_date: Start datetime for filtering
        end_date: End datetime for filtering
    
    Returns:
        List of dictionaries with weather data including outlet info
    """
    if not weather_json or "hourly" not in weather_json:
        return []
    
    hourly = weather_json["hourly"]
    times = hourly.get("time", [])
    wind_speeds = hourly.get("wind_speed_10m", [])
    temperatures = hourly.get("temperature_2m", [])
    humidities = hourly.get("relative_humidity_2m", [])
    
    records = []
    for i in range(len(times)):
        # Parse the time string to datetime for comparison
        time_str = times[i]
        try:
            # Open-Meteo returns times in ISO format: "2025-11-30T10:00"
            # Parse without timezone info for comparison
            time_dt = datetime.fromisoformat(time_str.replace('Z', ''))
            
            # Only include records within the last 24 hours (between start_date and end_date)
            if start_date <= time_dt <= end_date:
                records.append({
                    "outlet_id": outlet['id'],
                    "outlet_name": outlet['name'],
                    "latitude": outlet['latitude'],
                    "longitude": outlet['longitude'],
                    "time": times[i],
                    "wind_speed_10m": wind_speeds[i] if i < len(wind_speeds) else None,
                    "temperature_2m": temperatures[i] if i < len(temperatures) else None,
                    "relative_humidity_2m": humidities[i] if i < len(humidities) else None,
                })
        except (ValueError, AttributeError) as e:
            # If parsing fails, skip this record
            continue
    
    return records


def save_to_csv(records, output_path):
    """
    Save weather records to CSV file.
    
    Args:
        records: List of dictionaries with weather data
        output_path: Path to output CSV file
    """
    if not records:
        print("No records to save")
        return
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    fieldnames = [
        "outlet_id", "outlet_name", "latitude", "longitude", "time",
        "wind_speed_10m", "temperature_2m", "relative_humidity_2m"
    ]
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    
    print(f"Saved {len(records)} records to {output_path}")


def weather_fetch_pipeline():
    """
    Main function to fetch and save weather data for all outlets.
    Always fetches last 24 hours for all outlets.
    New outlets added to CSV are automatically included.
    """
    project_root = Path(__file__).parent.parent
    outlets_csv = project_root / "csv_data" / "outlet.csv"
    
    # Load outlets dynamically from CSV
    outlets = load_outlets(outlets_csv)
    print(f"Found {len(outlets)} outlets with valid coordinates")
    
    # Always fetch last 24 hours for all outlets
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=24)
    
    print(f"Fetching hourly weather data for last 24 hours")
    print(f"Time range: {start_date.strftime('%Y-%m-%d %H:%M')} to {end_date.strftime('%Y-%m-%d %H:%M')}")
    print()
    
    # Fetch weather for all outlets
    all_records = []
    for outlet in outlets:
        print(f"Fetching weather for {outlet['name']} (ID: {outlet['id']})...", end=" ")
        
        weather_json = fetch_weather_data(
            outlet['latitude'],
            outlet['longitude'],
            start_date,
            end_date
        )
        
        if weather_json:
            records = parse_weather_data(weather_json, outlet, start_date, end_date)
            all_records.extend(records)
            print(f"✓ {len(records)} hourly records")
        else:
            print("✗ Failed")
    
    # Save to CSV
    if all_records:
        output_dir = project_root / "data" / "weather"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = output_dir / f"weather_{timestamp}.csv"
        
        save_to_csv(all_records, output_path)
        print(f"\nTotal: {len(all_records)} records from {len(outlets)} outlets")
    else:
        print("\nNo weather data was successfully fetched")
