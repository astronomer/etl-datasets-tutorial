import requests
import pandas as pd
from include.global_variables import airflow_conf_variables as gv


def get_lat_long_for_cityname(city: str):
    """Converts a string of a city name provided into
    lat/long coordinates."""

    try:
        r = requests.get(f"https://photon.komoot.io/api/?q={city}")
        lon = r.json()["features"][0]["geometry"]["coordinates"][0]
        lat = r.json()["features"][0]["geometry"]["coordinates"][1]

        # log the coordinates retrieved
        gv.task_log.info(f"Coordinates for {city}: {lat}/{lon}")

    # if the coordinates cannot be retrieved log a warning
    except (AttributeError, KeyError, ValueError) as err:
        gv.task_log.warn(
            f"""Coordinates for {city}: could not be retrieved.
            Error: {err}"""
        )
        lat = "NA"
        lon = "NA"

    city_coordinates = {"city": city, "lat": lat, "lon": lon}

    return city_coordinates


def get_current_weather_from_city_coordinates(coordinates, timestamp):
    """Queries an open weather API for the current weather at the
    coordinates provided."""

    lat = coordinates["lat"]
    lon = coordinates["lon"]
    city = coordinates["city"]

    r = requests.get(
        f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
    )

    # if the API call is successful log the current temp
    if r.status_code == 200:
        current_weather = r.json()["current_weather"]

        gv.task_log.info(
            "The current temperature in {0} is {1}°C".format(
                city, current_weather["temperature"]
            )
        )

        data = {
            "city": city,
            "lat": lat,
            "long": lon,
            "temperature": current_weather["temperature"],
            "windspeed": current_weather["windspeed"],
            "winddirection": current_weather["winddirection"],
            "weathercode": current_weather["weathercode"],
            "time": f"{timestamp}",
            "API_response": r.status_code,
        }

    # if the API call is not successful, log a warning
    else:

        data = {
            "city": city,
            "lat": lat,
            "long": lon,
            "temperature": "NULL",
            "windspeed": "NULL",
            "winddirection": "NULL",
            "weathercode": "NULL",
            "time": f"{timestamp}",
            "API_response": r.status_code,
        }

        gv.task_log.warn(
            f"""
                Could not retrieve current temperature for {city} at
                {lat}/{lon} from https://api.open/meteo.com.
                Request returned {r.status_code}.
            """
        )

    return [data]


def get_historical_weather_from_city_coordinates(coordinates):
    """Queries an open weather API for the historical weather at the
    coordinates provided."""

    lat = coordinates["lat"]
    lon = coordinates["lon"]
    city = coordinates["city"]

    r = requests.get(
        f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date=1960-01-01&end_date=2023-01-01&daily=temperature_2m_max&timezone=auto"
    )

    # if the API call is successful log the current temp
    if r.status_code == 200:
        max_temp_per_day = pd.DataFrame(r.json()["daily"])
        max_temp_per_day["city"] = city
        max_temp_per_day["lat"] = lat
        max_temp_per_day["long"] = lon

    else:

        max_temp_per_day = pd.DataFrame(
            {
                "time": ["Null"],
                "temperature_2m_max": ["Null"],
                "city": [city],
                "lat": [lat],
                "long": [lon],
            }
        )

        gv.task_log.warn(
            f"""
                Could not retrieve historical temperature for {city} at
                {lat}/{lon} from https://api.open/meteo.com.
                Request returned {r.status_code}.
            """
        )

    return max_temp_per_day
