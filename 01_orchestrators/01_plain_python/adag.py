import datetime
from weather_project.weather_utils import fetch_and_store_weather
from weather_project.daily_weather import fetch_day_average
from weather_project.tables import create_table
from weather_project.global_weather import fetch_global_average
from airflow.decorators import dag, task
from pytz import timezone
import logging

IST = timezone("Asia/Kolkata")

@dag(
    dag_id="weather_dag",
    schedule_interval="0 0 * * *",  # Daily at midnight
    start_date=datetime.datetime(2025, 1, 19, tzinfo=IST),
    catchup=False,
    dagrun_timeout=datetime.timedelta(hours=24),
)
def weather_dag():
    @task()
    def create_tables():
        try:
            
            logger = logging.getLogger("airflow.task")  
            logger.info("Creating tables...")
            
            create_table()  
            logger.info("Tables created successfully.")
        
        except Exception as e:
            logger.error(f"Failed to create tables: {str(e)}")
            raise

    @task()
    def fetch_weather(city: str, date: str):
        try:
            
            logger = logging.getLogger("airflow.task")
            logger.info(f"Fetching weather for {city} on {date}...")

            fetch_and_store_weather(city, date)  
            logger.info(f"Weather data for {city} on {date} fetched and stored successfully.")
        
        except Exception as e:
            logger.error(f"Failed to fetch weather for {city} on {date}: {str(e)}")
            raise

    @task()
    def fetch_daily_weather(city: str):
        try:
            logger = logging.getLogger("airflow.task")
            logger.info(f"Fetching daily average weather for {city}...")

            fetch_day_average(city.title())  
            logger.info(f"Daily average weather for {city} fetched successfully.")
        
        except Exception as e:
            logger.error(f"Failed to fetch daily average weather for {city}: {str(e)}")
            raise

    @task()
    def global_average(city: str):
        try:
            logger = logging.getLogger("airflow.task")
            logger.info(f"Fetching global average weather for {city}...")

            fetch_global_average(city.title())  
            logger.info(f"Global average weather for {city} fetched successfully.")
        
        except Exception as e:
            logger.error(f"Failed to fetch global average weather for {city}: {str(e)}")
            raise

    create_task = create_tables()
    fetch_weather_task = fetch_weather("Alwar", "2025-01-19")
    fetch_daily_weather_task = fetch_daily_weather("Alwar")
    global_average_task = global_average("Alwar")

    create_task >> fetch_weather_task >> fetch_daily_weather_task >> global_average_task


weather_dag_instance = weather_dag()