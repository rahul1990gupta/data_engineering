from datetime import datetime
from dotenv import load_dotenv
import os
from db_operations import connect_to_db, create_table, insert_weather_data
from utils import fetch_hourly_weather

load_dotenv()

API_KEY = os.getenv("API_KEY")

if __name__ == "__main__":
    city_name = input("Enter the city name: ")
    today_date = datetime.now().strftime("%Y-%m-%d")  

    with connect_to_db() as conn:
        
        create_table(conn)

        weather_data = fetch_hourly_weather(API_KEY, city_name, today_date)

        if weather_data:
            
            insert_weather_data(conn, weather_data)
            print(f"Weather data for {city_name} has been inserted into the database.")
        else:
            print("Failed to fetch weather data.")
