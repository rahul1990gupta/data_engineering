import typing
import logging
from .weather_utils import fetch_and_store_weather
from .daily_weather import fetch_day_average
from .tables import create_table
from .global_weather import fetch_global_average
from flytekit import task, workflow


# image_definition = ImageSpec(
#    name="flytekit",  # default docker image name.
#    base_image="ghcr.io/flyteorg/flytekit:py3.11-1.10.2",  # the base image that flytekit will use to build your image.
#    packages=["pandas"],  # python packages to install.
#    registry="ghcr.io/unionai-oss", # the registry your image will be pushed to.
#    python_version="3.11"  # Optional if python is installed in the base image.
# )

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



@task()
def setup_database():
    logger.info("Creating database tables...")
    create_table()
    logger.info("Database tables created successfully.")



@task()
def fetch_weather(city: str, date: str):
    logger.info(f"Fetching weather for {city} on {date}...")
    fetch_and_store_weather(city, date)

    logger.info(f"Weather data for {city} on {date} fetched and stored successfully.")

@task() 
def fetch_daily_weather(city: str):
    logger.info(f"Fetching daily average weather for {city}...")
    
    fetch_day_average(city)  
    logger.info(f"Daily average weather for {city} fetched successfully.")

@task()
def global_weather(city: str):
    logger.info(f"Fetching global average weather for {city}...")
    fetch_global_average(city.title())
    logger.info(f"Global average weather for {city} fetched successfully.")

@workflow
def wf(city: str='Noida', date: str='2025-01-17') -> typing.Tuple[str, int]:
    
    setup_database()
    fetch_weather(city, date)
    fetch_daily_weather(city)
    global_weather(city)
    return f"Workflow executed successfully for {city} on {date}", 0


if __name__ == "__main__":

    print(f"Running wf() {wf()}")
