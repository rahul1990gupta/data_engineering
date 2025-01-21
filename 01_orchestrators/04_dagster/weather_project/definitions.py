from dagster import (
    Definitions,
    AssetSelection,
    ScheduleDefinition, 
    load_assets_from_modules )

from weather_project import assets  

all_assets = load_assets_from_modules([assets])

weather_schedule = ScheduleDefinition(
    name="weather_schedule",
    target=AssetSelection.all(),
    cron_schedule="0 * * * *",
)

defs = Definitions(
    assets=all_assets,
    schedules=[weather_schedule],
)
