import sqlite3


def insert_weather_data(data):
    conn = sqlite3.connect('weather_data.db')
    cursor = conn.cursor()

    location = data['location']
    forecast = data['forecast']['forecastday']

    for forecast_day in forecast:
        date = forecast_day['date']
        
        
        for hour_data in forecast_day['hour']:
            temp = hour_data['temp_c']
            humidity = hour_data['humidity']


            time = hour_data['time']
            condition = hour_data['condition']['text']
            localtime = location['localtime']

            cursor.execute("""
            INSERT INTO weather (date, time, temperature, condition, humidity,
                location_name, region, country, latitude, longitude, local_time)
            SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM weather WHERE date = ? AND time = ? AND location_name = ?
            )
            """, (date, time, temp, condition, humidity,
                  location['name'].title(), location['region'], location['country'],
                  location['lat'], location['lon'], localtime,
                  date, time, location['name']))

    conn.commit()
    conn.close()

