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
    conn.commit()
    conn.close()

def insert_weather_data(data):
  
    conn = sqlite3.connect('weather_data.db')
    cursor = conn.cursor()

    location = data['location']
    forecast = data['forecast']['forecastday']

    for forecast_day in forecast:
        date = forecast_day['date']
        for hour_data in forecast_day['hour']:
            time = hour_data['time']
            temp = hour_data['temp_c']
            condition = hour_data['condition']['text']
            humidity = hour_data['humidity']
            localtime = location['localtime']

            # Avoid duplicates
            cursor.execute("""
            INSERT INTO weather (date, time, temperature, condition, humidity,
                location_name, region, country, latitude, longitude, local_time)
            SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM weather WHERE date = ? AND time = ? AND location_name = ?
            )
            """, (date, time, temp, condition, humidity,
                  location['name'], location['region'], location['country'],
                  location['lat'], location['lon'], localtime,
                  date, time, location['name']))

    conn.commit()
    conn.close()
