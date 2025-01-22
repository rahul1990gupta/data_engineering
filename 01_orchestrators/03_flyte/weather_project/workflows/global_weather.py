import sqlite3

def fetch_global_average(city):
    conn = sqlite3.connect('weather_data.db')
    cursor = conn.cursor()

   
    cursor.execute("""
    INSERT OR REPLACE INTO global_weather (location_name, max_temp, min_temp, avg_humidity)
    SELECT location_name, 
           ROUND(AVG(max_temp), 2) AS max_temp, 
           ROUND(AVG(min_temp), 2) AS min_temp, 
           ROUND(AVG(avg_humidity), 2) AS avg_humidity
    FROM daily_weather 
    WHERE location_name = ?
    GROUP BY location_name;
    """, (city,))


    print(f"Global average data for {city} has been inserted into global_weather.")


    conn.commit()
    conn.close()

