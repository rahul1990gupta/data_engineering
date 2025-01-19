from datetime import datetime
from hourly_weather import insert_weather_data
import requests
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")

def fetch_and_store_weather(city: str, date: str):
    
    try:
        date_obj = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        return

    if date_obj > datetime.now():
        print("The date provided is in the future. Please enter a valid past date.")
        return

    url = f"http://api.weatherapi.com/v1/history.json?key={API_KEY}&q={city}&dt={date}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if date == datetime.now().strftime("%Y-%m-%d"):
            current_hour = datetime.now().hour
            filtered_hours = [
                hour for hour in data['forecast']['forecastday'][0]['hour']
                if int(hour['time'].split(' ')[1].split(':')[0]) <= current_hour
            ]
            data['forecast']['forecastday'][0]['hour'] = filtered_hours

        insert_weather_data(data)
        print(f"Weather data for {city} on {date} has been inserted into the database.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data: {e}")
        
