
# Info: https://support.weather.com/s/article/PWS-Upload-Protocol?language=en_US

import requests
import datetime
import pandas

from libs.logger import log
from libs.config import parse
import libs.cache as cache
from libs.calculations import *


class weatherdata:
    def __init__(self):
        self.dateutc = None
        self.winddir = 0.0 # Instant V
        self.windspeedmph = 0.0 # Instant V
        self.windspdmph_avg2m = 0.0 # 2 min V
        self.winddir_avg2m = 0.0 # 2 min V
        self.humidity = 0.0 # Instant V
        self.dewptf = 0.0 # Instant V
        self.tempf = 0.0 # Instant V
        self.rainin = 0.0 # Hourly V
        self.dailyrainin = 0.0 # Daily V


def send(id, key, softwaretype, data):
    config = parse()

    try:
        url = 'http://rtupdate.wunderground.com/weatherstation/updateweatherstation.php'

        params = {
            'action':           'updateraw',
            'ID':               str(id),
            'PASSWORD':         str(key),
            'dateutc':          str(data['dateutc']),
            'winddir':          float(data['winddir']),
            'windspeedmph':     float(data['windspeedmph']),
            'windspdmph_avg2m': float(data['windspdmph_avg2m']),
            'winddir_avg2m':    float(data['winddir_avg2m']),
            'humidity':         float(data['humidity']),
            'dewptf':           float(data['dewptf']),
            'tempf':            float(data['tempf']),
            'rainin':           float(data['rainin']), 
            'dailyrainin':      float(data['dailyrainin']),
            'softwaretype':     str(softwaretype)}

        result = requests.post(url=url, params=params)
        result.raise_for_status()

        if result.content != b'success\n' or result.status_code != 200:
            raise Exception(f"WeatherUnderground didn't acknowledge the success of the update. Response of the server:\n - Status-code: {result.status_code}\n - Content: {result.content}") 

        if config.getboolean("WeatherUnderground","Debug") == True:
            log(f'WeatherUnderground: Response to "{result.url}": \n- Headers: {result.headers}\n- Content:{result.content}' ,"debug")


    except Exception as error:
        log(f"Weather Underground: Could not send results: {error}", "error")

weatherunderground_converted_weatherreports = 0
def convertAndSend():
    global weatherunderground_converted_weatherreports

    config = parse()

    send_weatherunderground_cache_connection = cache.create_connection(config["Cache"]["File"]) # Opens a connection to the cache. Every thread needs its own connection.

    weatherreports = cache.get_weather(send_weatherunderground_cache_connection)

    if len(weatherreports) > 0:
        weatherreports['timestamp'] = pandas.to_datetime(weatherreports['timestamp'],unit='s')

        today = datetime.datetime.utcnow().date()
        weatherreports_current_day = weatherreports[weatherreports['timestamp'].between(datetime.datetime(today.year, today.month, today.day), datetime.datetime.utcnow())]
        weatherreports_current_day = weatherreports_current_day.groupby('device_id').agg({'rainfall_mm': 'sum'}) # Get the the total rainfall since the beginning of the current day.
        weatherreports_current_day = weatherreports_current_day.add_prefix('current_day_')

        weatherreports_1hour = weatherreports[weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(hours=1), datetime.datetime.utcnow())]
        weatherreports_1hour = weatherreports_1hour.groupby('device_id').agg({'rainfall_mm': 'sum'}) # Get the total rainfall within the last 1 hour.
        weatherreports_1hour = weatherreports_1hour.add_prefix('1hour_')

        weatherreports_2min = weatherreports[weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(minutes=2), datetime.datetime.utcnow())]
        weatherreports_2min = weatherreports_2min.groupby('device_id').agg({'wind_direction_degrees': 'mean', 'wind_speed_avg_ms':'mean'}) # Get the average wind direction and wind speef within the last 2 minutes.
        weatherreports_2min = weatherreports_2min.add_prefix('2min_')

        weatherreports_since_last_observation = weatherreports[weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(minutes=int(config['WeatherUnderground']['SendIntervalInMinutes'])), datetime.datetime.utcnow())]
        weatherreports_since_last_observation = weatherreports_since_last_observation.groupby('device_id').agg({'temperature_c':'mean', # Get the average temperature, humidity, wind speed and winddirection of the last minute. These values are being used as the instantaneous values. The 1 minute is to make sure that any errors within the reading are being averaged out.
                                                                               'humidity':'mean',
                                                                               'wind_speed_avg_ms':'mean',
                                                                               'wind_direction_degrees': 'mean'})
        weatherreports_since_last_observation = weatherreports_since_last_observation.add_prefix('since_last_observation_')

        weatherreports_concatenated = pandas.merge(weatherreports_current_day, weatherreports_1hour, left_index=True, right_index=True)
        weatherreports_concatenated = pandas.merge(weatherreports_concatenated, weatherreports_2min, left_index=True, right_index=True)
        weatherreports_concatenated = pandas.merge(weatherreports_concatenated, weatherreports_since_last_observation, left_index=True, right_index=True)

        for index, device in weatherreports_concatenated.iterrows():
            line = weatherdata() # Use the weatherdata class of the weatherunderground library. This class makes it able to store values and return them in the format required for sending messages to weatherunderground.
            line.dateutc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            line.tempf = round(float(celcius_to_fahrenheit(device["since_last_observation_temperature_c"])), 2)
            line.humidity = round(float(device["since_last_observation_humidity"]), 2)

            line.rainin = round(float(device["1hour_rainfall_mm"] * mm_to_inch), 2)
            line.dailyrainin = round(float(device["current_day_rainfall_mm"] * mm_to_inch), 2)

            dew_point_c = dew_point(float(device["since_last_observation_temperature_c"]), device["since_last_observation_humidity"])

            line.dewptf = round(float(celcius_to_fahrenheit(dew_point_c)), 2)
            line.windspeedmph = round(float(device["since_last_observation_wind_speed_avg_ms"]) * ms_to_mph, 2)
            line.windspdmph_avg2m = round(float(device["since_last_observation_wind_speed_avg_ms"]) * ms_to_mph, 2)
            line.winddir = round(float(device["since_last_observation_wind_direction_degrees"]), 2)
            line.winddir_avg2m = round(float(device["since_last_observation_wind_direction_degrees"]), 2)

            send(config["WeatherUnderground"]["ID"], config["WeatherUnderground"]["Key"], config["WeatherUnderground"]["Softwaretype"], line.__dict__)

            weatherunderground_converted_weatherreports += 1
            if weatherunderground_converted_weatherreports % config.getint("WeatherUnderground","NtfyEveryXWeatherreports")  == 0:
                log(f"Weather Underground: Saved '{weatherunderground_converted_weatherreports}' weatherreports to Weather Underground.\nCurrent weather:\n - Temperature: {device['since_last_observation_temperature_c']} C\n - Humidity: {device['since_last_observation_humidity']} %\n - Rainfall: {device['1hour_rainfall_mm']} mm since last hour.", "report")
        
    elif len(weatherreports) > 0 and len(weatherreports) <= 20:
        log(f"Weather Underground: Not enough weatherreports to summarize. There need to be at least 20. Currently there are {len(weatherreports)} weatherreports. Please make sure that the receiving antenna is close enough to the weatherstation.", "info")
    else:
        log(f"Weather Underground: There are no weatherreports to summarize. Please make sure that the weatherdevice has been turned on.", "info")
