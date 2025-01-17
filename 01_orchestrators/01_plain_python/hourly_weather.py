import sqlite3
import requests
import csv
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("API_KEY")

def connect_to_db():
    """Create a connection to the SQLite database"""
    conn = sqlite3.connect('weather_data.db')
    return conn

def create_table():
    """Create the table to store weather data"""
    conn = connect_to_db()
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
    )""")
    conn.commit()
    conn.close()

def insert_weather_data(data):
    """Insert weather data into the SQLite database"""
    conn = connect_to_db()
    cursor = conn.cursor()

    for forecast_day in data['forecast']['forecastday']:
        date = forecast_day['date']
        for hour in forecast_day['hour']:
            time = hour['time']

            current_time = datetime.now()
            current_hour = current_time.strftime("%Y-%m-%d %H:00")
            if time == current_hour:
                break 

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

            cursor.execute("""
            INSERT INTO weather (date, time, temperature, condition, humidity,
            location_name, region, country, latitude, longitude, local_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (date, time, temp, condition, humidity, 
                  name, region, country, lat, lon, localtime))

    conn.commit()
    conn.close()

def fetch_hourly_weather(city, date):
    """Fetch the hourly weather data"""
    url = f"http://api.weatherapi.com/v1/history.json?key={API_KEY}&q={city}&dt={date}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {date}: {e}")
        return None

if __name__ == "__main__":
    city_name = input("Enter the city name: ")
    today_date = datetime.now().strftime("%Y-%m-%d")  
    create_table()

    weather_data = fetch_hourly_weather(city_name, today_date)
    
    if weather_data:
        insert_weather_data(weather_data)
        print(f"Weather data for {city_name} has been inserted into the database.")
    else:
        print("Failed to fetch weather data.")

    conn = sqlite3.connect('weather_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather")
    rows = cursor.fetchall()
    print("Data in SQLite Database:")
    for row in rows:
        print(row)
    conn.close()
