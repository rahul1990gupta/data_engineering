import typer
from weather_utils import fetch_and_store_weather

app = typer.Typer()

@app.command()
def fetch_weather(city: str, date: str):
    
    fetch_and_store_weather(city, date)

if __name__ == "__main__":
    app()
