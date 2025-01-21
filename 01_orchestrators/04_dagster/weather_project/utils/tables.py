import sqlite3

def create_table():
    conn = sqlite3.connect('weather_data.db')
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        time TEXT,
        temperature REAL,
        condition TEXT,
        humidity INTEGER,
        location_name TEXT,
        region TEXT,
        country TEXT,
        latitude REAL,
        longitude REAL,
        local_time TEXT
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_name TEXT,
        date TEXT,
        max_temp REAL,
        min_temp REAL,
        condition TEXT,
        avg_humidity REAL,
        UNIQUE(location_name, date)  -- Prevents duplicates based on location and date
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS global_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_name TEXT,
        max_temp REAL,
        min_temp REAL,
        condition TEXT,
        avg_humidity REAL,
        UNIQUE(location_name)  -- Prevents duplicates based on location
    )
    """)
    conn.commit()
    conn.close()