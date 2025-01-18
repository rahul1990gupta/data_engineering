import datetime
from weather_utils import fetch_and_store_weather
from daily_weather import fetch_day_average
from tables import create_table
from global_weather import fetch_global_average
from airflow.decorators import dag, task



@dag(
    dag_id="weather_dag",
    schedule_interval="0 0 * * *",
    start_date=datetime.datetime(2025, 1, 18, tzinfo=datetime.timezone.utc),
    catchup=False,
    dagrun_timeout=datetime.timedelta(hours=24),
)
def weather_dag():

    @task()
    def create_tables():
        create_table()

    @task()
    def fetch_weather(city: str, date: str):
        fetch_and_store_weather(city, date)

    @task()
    def fetch_daily_weather(city: str):
        fetch_day_average(city.capitalize())

    @task()
    def global_average(city: str):
        fetch_global_average(city.capitalize())

    create_task = create_tables()
    fetch_weather_task = fetch_weather("New York", "2025-01-18")
    fetch_daily_weather_task = fetch_daily_weather("New York")
    global_average_task = global_average("New York")

    create_task >> fetch_weather_task >> fetch_daily_weather_task >> global_average_task


dag = weather_dag()
