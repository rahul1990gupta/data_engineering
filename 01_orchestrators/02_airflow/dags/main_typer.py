import typer
from weather_utils import fetch_and_store_weather
from daily_weather import fetch_day_average
from tables import create_table
from global_weather import fetch_global_average

app = typer.Typer()

@app.command()
def fetch_weather(city: str, date: str):
    
    fetch_and_store_weather(city, date)

@app.command()
def fetch_daily_weather(city: str):
    
    fetch_day_average(city.capitalize())    

@app.command()
def global_average(city: str):
    
    fetch_global_average(city.capitalize())    

if __name__ == "__main__":
    create_table()
    app()
