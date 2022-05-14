# Info: https://openweathermap.org/stations

import requests
import datetime
import time
import pandas
import json

from libs.logger import log
from libs.config import parse
import libs.cache as cache
from libs.calculations import *


class weatherdata:
    def __init__(self):
        self.dt = None # Unix timestamp.
        self.temperature = 0.0 # in C
        self.wind_speed = 0.0 # At the moment.
        self.wind_deg = 0.0 # Instant in degrees.
        self.humidity = 0.0 # Instant V
        self.rain_1h = 0.0 # Accumulated in 1 hour in MM V
        self.rain_6h = 0.0 # Accumulated in 6 hours in MM V
        self.rain_24h = 0.0 # Accumulated in 6 hours in MM V


def send(appid, stationid, data):
    config = parse()

    try:
        url = 'http://api.openweathermap.org/data/3.0/measurements?'

        headers = {'content-type':   'application/json'}

        params = {'appid':           str(appid)}

        data = json.dumps([{
            'station_id':       str(stationid),
            'dt':               int(data['dt']),
            'temperature':      float(data['temperature']),
            'wind_speed':       float(data['wind_speed']),
            'wind_deg':         float(data['wind_deg']),
            'humidity':         float(data['humidity']),
            'rain_1h':          float(data['rain_1h']),
            'rain_6h':          float(data['rain_6h']),
            'rain_24h':         float(data['rain_24h'])}])
        
        result = requests.post(url=url, headers=headers, params=params, data=data)
        result.raise_for_status()

        if result.content != b'' or result.status_code != 204:
            raise Exception(f"OpenWeathermap didn't acknowledge the success of the update. Response of the server:\n - Status-code: {result.status_code}\n - Content: {result.content}") 

        if config.getboolean("OpenWeathermap","Debug") == True:
            log(f'OpenWeathermap: Response to "{result.url}" with body "{result.request.body}": \n- Headers: {result.headers}\n- Content:{result.content}' ,"debug")


    except Exception as error:
        log(f"OpenWeathermap: Could not send results: {error}", "error")


def create(appid, external_id , name, latitude, longitude, altitude):
    url = "http://api.openweathermap.org/data/3.0/stations"

    try:
        headers = {'content-type':   'application/json'}

        params = {'appid':           str(appid)}

        json = {
            'external_id':           str(external_id),
            'name':                  str(name),
            'latitude':              float(latitude),
            'longitude':             float(longitude),
            'altitude':              float(altitude)}
        
        result = requests.post(url=url, headers=headers, params=params, json=json)
        result.raise_for_status()

    except Exception as error:
        log(f"OpenWeathermap: Could not create a new station: {error}", "error")
        return 0
    else:
        return result.json()['ID']


openweathermap_converted_weatherreports = 0
def convertAndSend():
    global openweathermap_converted_weatherreports

    config = parse()

    send_openweathermap_cache_connection = cache.create_connection(config["Cache"]["File"]) # Opens a connection to the cache. Every thread needs its own connection.

    weatherreports = cache.get_weather(send_openweathermap_cache_connection)

    if len(weatherreports) > 0:
        weatherreports['timestamp'] = pandas.to_datetime(weatherreports['timestamp'],unit='s')

        weatherreports_1day = weatherreports[weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(days=1), datetime.datetime.utcnow())]
        weatherreports_1day = weatherreports_1day.groupby('device_id').agg({'rainfall_mm': 'sum'}) # Get the total rainfall within the last 1 day.
        weatherreports_1day = weatherreports_1day.add_prefix('1day_')

        weatherreports_6hours = weatherreports[weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(hours=6), datetime.datetime.utcnow())]
        weatherreports_6hours = weatherreports_6hours.groupby('device_id').agg({'rainfall_mm': 'sum'}) # Get the total rainfall within the last 6 hours.
        weatherreports_6hours = weatherreports_6hours.add_prefix('6hours_')

        weatherreports_1hour = weatherreports[weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(hours=1), datetime.datetime.utcnow())]
        weatherreports_1hour = weatherreports_1hour.groupby('device_id').agg({'rainfall_mm': 'sum'}) # Get the total rainfall within the last 1 hour.
        weatherreports_1hour = weatherreports_1hour.add_prefix('1hour_')

        weatherreports_since_last_observation = weatherreports[weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(minutes=int(config['OpenWeathermap']['SendIntervalInMinutes'])), datetime.datetime.utcnow())]
        weatherreports_since_last_observation = weatherreports_since_last_observation.groupby('device_id').agg({'temperature_c':'mean', # Get the average temperature, humidity, wind speed and winddirection of the last minute. These values are being used as the instantaneous values. The 1 minute is to make sure that any errors within the reading are being averaged out.
                                                                               'humidity':'mean',
                                                                               'wind_speed_avg_ms':'mean',
                                                                               'wind_direction_degrees': 'mean'})
        weatherreports_since_last_observation = weatherreports_since_last_observation.add_prefix('since_last_observation_')

        weatherreports_concatenated = pandas.merge(weatherreports_1day, weatherreports_6hours, left_index=True, right_index=True)
        weatherreports_concatenated = pandas.merge(weatherreports_concatenated, weatherreports_1hour, left_index=True, right_index=True)
        weatherreports_concatenated = pandas.merge(weatherreports_concatenated, weatherreports_since_last_observation, left_index=True, right_index=True)

        for index, device in weatherreports_concatenated.iterrows():
            line = weatherdata() # Use the weatherdata class of the weatherunderground library. This class makes it able to store values and return them in the format required for sending messages to weatherunderground.
            line.dt = int(time.time())
            line.temperature = round(float(device["since_last_observation_temperature_c"]), 2)
            line.wind_speed = round(float(device["since_last_observation_wind_speed_avg_ms"]), 2)
            line.wind_deg = round(float(device["since_last_observation_wind_direction_degrees"]), 2)
            line.humidity = round(float(device["since_last_observation_humidity"]), 2)
            line.rain_1h = round(float(device["1hour_rainfall_mm"]), 2)
            line.rain_6h = round(float(device["6hours_rainfall_mm"]), 2)
            line.rain_24h = round(float(device["1day_rainfall_mm"]), 2)

            send(config["OpenWeathermap"]["AppID"], config["OpenWeathermap"]["StationID"], line.__dict__)

            openweathermap_converted_weatherreports += 1
            if openweathermap_converted_weatherreports % config.getint("OpenWeathermap","NtfyEveryXWeatherreports")  == 0:
                log(f"OpenWeathermap: Saved '{openweathermap_converted_weatherreports}' weatherreports to OpenWeathermap.\nCurrent weather:\n - Temperature: {device['since_last_observation_temperature_c']} C\n - Humidity: {device['since_last_observation_humidity']} %\n - Rainfall: {device['1hour_rainfall_mm']} mm since last hour.", "report")

    elif len(weatherreports) > 0 and len(weatherreports) <= 20:
        log(f"OpenWeathermap: Not enough weatherreports to summarize. There need to be at least 20. Currently there are {len(weatherreports)} weatherreports. Please make sure that the receiving antenna is close enough to the weatherstation.", "info")
    else:
        log(f"OpenWeathermap: There are no weatherreports to summarize. Please make sure that the weatherdevice has been turned on.", "info")
