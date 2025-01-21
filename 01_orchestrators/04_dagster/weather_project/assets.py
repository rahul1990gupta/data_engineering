from .utils.weather_utils import fetch_and_store_weather
from .utils.daily_weather import fetch_day_average
from .utils.tables import create_table
from .utils.global_weather import fetch_global_average
from dagster import asset, MaterializeResult, MetadataValue
from pytz import timezone
import logging
import datetime
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

IST = timezone("Asia/Kolkata")
city = "Bhopal"
date = '2025-01-19'

@asset(
        description='Table Creation for the Weather Data',
        metadata={
            'description': 'Creates databse tables needed for weather data.',
            'created_at': datetime.datetime.now().isoformat()
        }
)
def setup_database() -> None:
    logger.info("Creating database tables...")
    create_table()
    logger.info("Database tables created successfully.")



@asset(
        deps=[setup_database],
        description="The hourly data",
        metadata={
            'description': 'Fetches weather data for a given city and date.',
            'city and date': f"{city} on {date}",
            'created_at': datetime.datetime.now().isoformat()
        }
)
def fetch_weather():
    logger.info(f"Fetching weather for {city} on {date}...")
    weather_data = fetch_and_store_weather(city, date)

    logger.info(f"Weather data for {city} on {date} fetched and stored successfully.")

    return MaterializeResult(
        metadata={
            'number of rows': weather_data
        }
        
    )

@asset(
        deps=[fetch_weather],
        description="The day Average",
        metadata={
            'description': 'Fetches daily average weather for a given city.',
            'city and date': f"{city} on {date}",
            'created_at': datetime.datetime.now().isoformat()
        }
)
def fetch_daily_weather():
    logger.info(f"Fetching daily average weather for {city}...")
    
    weather_data = fetch_day_average(city)  
    logger.info(f"Daily average weather for {city} fetched successfully.")

    columns = ["ID", "City", "Date", "Max Temp (°C)", "Min Temp (°C)", "Condition", "Avg Humidity (%)"]
    weather_df = pd.DataFrame(weather_data, columns=columns)

    return MaterializeResult(
        metadata={
            "Row added" : MetadataValue.md(weather_df.head().to_markdown()),
        }
    )


@asset(
        deps=[fetch_daily_weather],
        description="The Whole Average",
        metadata={
            'description': 'Fetches global average weather for a given city.',
            'city': city,
            'created_at': datetime.datetime.now().isoformat()
        }
)  
def global_weather():
    logger.info(f"Fetching global average weather for {city}...")
    fetch_global_average(city.title())
    logger.info(f"Global average weather for {city} fetched successfully.")
   
