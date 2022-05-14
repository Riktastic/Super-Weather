# Info from: https://discourse.nodered.org/t/upload-pws-data-to-pwsweather/47347/3 . This isn't made public.
# SAMPLE STRING TO UPDATE DATA ON PWSWEATHER.com


# https://pwsupdate.pwsweather.com/api/v1/submitwx?ID=STATIONID&PASSWORD=APIkey&dateutc=2000-12-01+15:20:01&winddir=225&windspeedmph=0.0&windgustmph=0.0&tempf=34.88&rainin=0.06&dailyrainin=0.06&monthrainin=1.02&yearrainin=18.26&baromin=29.49&dewptf=30.16&humidity=83&weather=OVC&solarradiation=183&UV=5.28&softwaretype=Examplever1.1&action=updateraw


# All parameters are optional except for the ones marked with *.
# If your software or hardware doesn't support a parameter it can be omitted from the string.


# ID *		Station ID as registered

# PASSWORD *	The API key available on the station's page

# dateutc	*	Date and time in the format of year-mo-da+hour:min:sec

# winddir		Wind direction in degrees

# windspeedmph	Wind speed in miles per hour

# windgustmph	Wind gust in miles per hour

# tempf		Temperature in degrees fahrenheit

# rainin		Hourly rain in inches

# dailyrainin	Daily rain in inches

# monthrainin	Monthly rain in inches

# yearrainin	Seasonal rain in inches (usually local meteorological year)

# baromin		Barometric pressure in inches

# dewptf		Dew point in degrees fahrenheit

# humidity	Humidity in percent

# weather		Current weather or sky conditions using standard METAR abbreviations and intensity (e.g. -RA, +SN, SKC, etc.)

# solarradiation	Solar radiation

# UV		UV

# softwaretype *	Software type


# The string always concludes with action=updateraw to indicate the end of the readings


# For more information contact AerisWeather Support: 

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
        self.windspeedmph = 0.0 # Instant mph V
        self.tempf = 0.0 # Instant fahrenheit V
        self.rainin = 0.0 # 60 min inch V
        self.dailyrainin = 0.0 # 1 day inch V
        self.monthrainin = 0.0 # 1 month inch V
        self.yearrainin = 0.0 # 1 year inch V
        self.dewptf = 0.0 # Instant in fahrenheit V
        self.humidity = 0.0 # Instant V


def send(id, key, softwaretype, data):
    config = parse()

    try:
        url = 'https://pwsupdate.pwsweather.com/api/v1/submitwx'

        params = {
            'ID':               str(id),
            'PASSWORD':         str(key),
            'dateutc':          str(data['dateutc']),
            'winddir':          float(data['winddir']),
            'windspeedmph':     float(data['windspeedmph']),
            'tempf':            float(data['tempf']),
            'rainin':           float(data['rainin']), 
            'dailyrainin':      float(data['dailyrainin']),
            'monthrainin':      float(data['monthrainin']),
            'yearrainin':       float(data['yearrainin']),
            'dewptf':           float(data['dewptf']),
            'humidity':         float(data['humidity']),
            'softwaretype':     str(softwaretype),
            'action':           'updateraw'}

        result = requests.post(url=url, params=params)
        result.raise_for_status()

        if result.content != b'{"error":null,"success":true}\n' or result.status_code != 200:
            raise Exception(f"PWSWeather didn't acknowledge the success of the update. Response of the server:\n - Status-code: {result.status_code}\n - Content: {result.content}")  

        if config.getboolean("PWSWeather","Debug") == True:
            log(f'PWSWeather: Response to "{result.url}": \n- Headers: {result.headers}\n- Content:{result.content}' ,"debug")

    except Exception as error:
        log(f"PWSWeather: Could not send results: {error}", "error")


pwsweather_converted_weathereports = 0
def convertAndSend():
    global pwsweather_converted_weathereports

    config = parse()

    send_pwsweather_cache_connection = cache.create_connection(config["Cache"]["File"]) # Opens a connection to the cache. Every thread needs its own connection.

    weatherreports = cache.get_weather(send_pwsweather_cache_connection)

    if len(weatherreports) > 0:
        weatherreports['timestamp'] = pandas.to_datetime(weatherreports['timestamp'],unit='s')

        weatherreports_1year = weatherreports[weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(days=365*2), datetime.datetime.utcnow())]
        weatherreports_1year = weatherreports_1year.groupby('device_id').agg({'rainfall_mm': 'sum'}) # Get the total rainfall within the last 1 year.
        weatherreports_1year = weatherreports_1year.add_prefix('1year_')

        weatherreports_1month = weatherreports[weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(days=365/12), datetime.datetime.utcnow())]
        weatherreports_1month = weatherreports_1month.groupby('device_id').agg({'rainfall_mm': 'sum'}) # Get the total rainfall within the last 1 month.
        weatherreports_1month = weatherreports_1month.add_prefix('1month_')

        weatherreports_1day = weatherreports[weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(days=1), datetime.datetime.utcnow())]
        weatherreports_1day = weatherreports_1day.groupby('device_id').agg({'rainfall_mm': 'sum'}) # Get the total rainfall within the last 1 day.
        weatherreports_1day = weatherreports_1day.add_prefix('1day_')

        weatherreports_1hour = weatherreports[weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(days=1), datetime.datetime.utcnow())]
        weatherreports_1hour = weatherreports_1hour.groupby('device_id').agg({'rainfall_mm': 'sum'}) # Get the total rainfall within the last 1 hour.
        weatherreports_1hour = weatherreports_1hour.add_prefix('1hour_')

        weatherreports_since_last_observation = weatherreports[weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(minutes=int(config['PWSWeather']['SendIntervalInMinutes'])), datetime.datetime.utcnow())]
        weatherreports_since_last_observation = weatherreports_since_last_observation.groupby('device_id').agg({'temperature_c':'mean', # Get the average temperature, humidity, wind speed and winddirection of the last minute. These values are being used as the instantaneous values. The 1 minute is to make sure that any errors within the reading are being averaged out.
                                                                               'humidity':'mean',
                                                                               'wind_speed_avg_ms':'mean',
                                                                               'wind_direction_degrees': 'mean'})
        weatherreports_since_last_observation = weatherreports_since_last_observation.add_prefix('since_last_observation_')

        weatherreports_concatenated = pandas.merge(weatherreports_1year, weatherreports_1month, left_index=True, right_index=True)
        weatherreports_concatenated = pandas.merge(weatherreports_concatenated, weatherreports_1day, left_index=True, right_index=True)
        weatherreports_concatenated = pandas.merge(weatherreports_concatenated, weatherreports_1hour, left_index=True, right_index=True)
        weatherreports_concatenated = pandas.merge(weatherreports_concatenated, weatherreports_since_last_observation, left_index=True, right_index=True)

        for index, device in weatherreports_concatenated.iterrows():
            line = weatherdata() # Use the weatherdata class of the pwsweather library. This class makes it able to store values and return them in the format required for sending messages to pwsweather.
            line.dateutc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            line.winddir = round(float(device["since_last_observation_wind_direction_degrees"]), 2)
            line.windspeedmph = round(float(device["since_last_observation_wind_speed_avg_ms"]) * ms_to_mph, 2)
            line.tempf = round(float(celcius_to_fahrenheit(device["since_last_observation_temperature_c"])), 2)
            line.rainin = round(float(device["1hour_rainfall_mm"] * mm_to_inch), 2)
            line.dailyrainin = round(float(device["1day_rainfall_mm"] * mm_to_inch), 2)
            line.monthrainin = round(float(device["1month_rainfall_mm"] * mm_to_inch), 2)
            line.yearrainin = round(float(device["1year_rainfall_mm"] * mm_to_inch), 2)
            dew_point_c = dew_point(float(device["since_last_observation_temperature_c"]), device["since_last_observation_humidity"])
            line.dewptf = round(float(celcius_to_fahrenheit(dew_point_c)), 2)
            line.humidity = round(float(device["since_last_observation_humidity"]), 2)

            send(config["PWSWeather"]["ID"], config["PWSWeather"]["Key"], config["PWSWeather"]["Softwaretype"], line.__dict__)

            pwsweather_converted_weathereports += 1
            if pwsweather_converted_weathereports % config.getint("PWSWeather","NtfyEveryXWeatherreports")  == 0:
                log(f"PWSWeather: Saved '{pwsweather_converted_weathereports}' weatherreports to PWSWeather.\nCurrent weather:\n - Temperature: {device['since_last_observation_temperature_c']} C\n - Humidity: {device['since_last_observation_humidity']} %\n - Rainfall: {device['1hour_rainfall_mm']} mm since last hour.", "report")

    elif len(weatherreports) > 0 and len(weatherreports) <= 20:
        log(f"PWSWeather: Not enough weatherreports to summarize. There need to be at least 20. Currently there are {len(weatherreports)} weatherreports. Please make sure that the receiving antenna is close enough to the weatherstation.", "info")
    else:
        log(f"PWSWeather: There are no weatherreports to summarize. Please make sure that the weatherdevice has been turned on.", "info")