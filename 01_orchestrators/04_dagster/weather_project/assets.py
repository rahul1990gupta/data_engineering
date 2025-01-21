from .utils.weather_utils import fetch_and_store_weather
from .utils.daily_weather import fetch_day_average
from .utils.tables import create_table
from .utils.global_weather import fetch_global_average
from dagster import asset
from pytz import timezone
import logging
import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

IST = timezone("Asia/Kolkata")
city = "Pune"
date = '2025-01-18'

@asset(
        description='Table Creation for the Weather Data',
        metadata={
            'description': 'Creates databse tables needed for weather data.',
            'created_at': datetime.datetime.now().isoformat()
        }
)
def setup_database():
    logger.info("Creating database tables...")
    create_table()
    logger.info("Database tables created successfully.")

@asset(
        description="The hourly data",
        metadata={
            'description': 'Fetches weather data for a given city and date.',
            'city and date': f"{city} on {date}",
            'created_at': datetime.datetime.now().isoformat()
        }
)
def fetch_weather(setup_database):
    logger.info(f"Fetching weather for {city} on {date}...")
    weather_data = fetch_and_store_weather(city, date)
    logger.info(f"Weather data for {city} on {date} fetched and stored successfully.")
    return weather_data

@asset(
        description="The day Average",
        metadata={
            'description': 'Fetches daily average weather for a given city.',
            'city and date': f"{city} on {date}",
            'created_at': datetime.datetime.now().isoformat()
        }
)
def fetch_daily_weather(fetch_weather):
    logger.info(f"Fetching daily average weather for {city}...")
    daily_avg_weather = fetch_day_average(city.title())
    logger.info(f"Daily average weather for {city} fetched successfully.")
    return daily_avg_weather

@asset(
        description="The Whole Average",
        metadata={
            'description': 'Fetches global average weather for a given city.',
            'city': city,
            'created_at': datetime.datetime.now().isoformat()
        }
)  
def global_weather(fetch_daily_weather):
    logger.info(f"Fetching global average weather for {city}...")
    global_avg_weather = fetch_global_average(city.title())
    logger.info(f"Global average weather for {city} fetched successfully.")
    return global_avg_weather
