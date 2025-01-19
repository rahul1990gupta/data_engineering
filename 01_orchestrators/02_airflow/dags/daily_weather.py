import sqlite3

def fetch_day_average(city):
    conn = sqlite3.connect('weather_data.db')
    cursor = conn.cursor()    

    cursor.execute("""
    INSERT OR REPLACE INTO daily_weather (location_name, date, max_temp, min_temp, condition, avg_humidity)
    SELECT location_name, 
           date, 
           MAX(temperature) AS max_temp, 
           MIN(temperature) AS min_temp, 
           condition, 
           ROUND(AVG(humidity), 2) AS avg_humidity
    FROM weather
    WHERE location_name = ?
    GROUP BY location_name, date;
    """, (city,))

    print(f"Average data for {city} has been inserted into daily_weather.")
    
    conn.commit()
    conn.close()
