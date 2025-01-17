import sqlite3

def connect_to_db():
    """Create or connect to an SQLite database"""
    return sqlite3.connect('weather_data.db')


def create_table(conn):
    """Create a weather table if it doesn't exist"""
    try:
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
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")


def insert_weather_data(conn, data):
    """Insert weather data into the SQLite database"""
    try:
        cursor = conn.cursor()

        for forecast_day in data['forecast']['forecastday']:
            date = forecast_day['date']
            for hour in forecast_day['hour']:
                time = hour['time']
                temp = hour['temp_c']
                condition = hour['condition']['text']
                humidity = hour['humidity']
                location = data['location']
                name = location['name']
                region = location['region']
                country = location['country']
                lat = location['lat']
                lon = location['lon']
                localtime = location['localtime']

                # Insert data only if this specific row doesn't exist
                cursor.execute("""
                INSERT INTO weather (date, time, temperature, condition, humidity,
                location_name, region, country, latitude, longitude, local_time)
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM weather WHERE date = ? AND time = ? AND location_name = ?
                )
                """, (date, time, temp, condition, humidity, name, region, country,
                      lat, lon, localtime, date, time, name))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error inserting data: {e}")
