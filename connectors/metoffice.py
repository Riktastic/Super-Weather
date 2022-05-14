# Info: https://wow.metoffice.gov.uk/support/dataformats

import requests
import datetime
import pandas

from libs.logger import log
from libs.config import parse
import libs.cache as cache
from libs.calculations import *


class weatherdata:
    def __init__(self):
        self.dateutc = None # YYYY-mm-DD HH:mm:ss
        self.dailyrainin = 0.0 # Since beginning of day. Inches
        self.dewptf = 0.0 # Instant
        self.humidity = 0.0 # Instant
        self.rainin = 0.0 # Instant Inches
        self.tempf = 0.0 # Instant Fahrenheit
        self.winddir = 0.0 # Instant degrees
        self.windspeedmph = 0.0 # Instant MPH


def send(id, key, softwaretype, data):
    config = parse()

    try:
        url = 'http://wow.metoffice.gov.uk/automaticreading?'

        params = {
            'siteid':                       str(id),
            'siteAuthenticationKey':        str(key),
            'dateutc':                      str(data['dateutc']),
            'softwaretype':                 str(softwaretype),
            'dailyrainin':                  float(data['dailyrainin']),
            'dewptf':                       float(data['dewptf']),
            'humidity':                     float(data['humidity']),
            'rainin':                       float(data['rainin']), 
            'tempf':                        float(data['tempf']),
            'winddir':                      float(data['winddir']),
            'windspeedmph':                 float(data['windspeedmph'])}
        
        result = requests.post(url=url, params=params)
        result.raise_for_status()

        if result.content != b'{}' or result.status_code != 200:
            raise Exception(f"Met Office didn't acknowledge the success of the update. Response of the server:\n - Status-code: {result.status_code}\n - Content: {result.content}") 

        if config.getboolean("MetOffice","Debug") == True:
            log(f'MetOffice: Response to "{result.url}": \n- Headers: {result.headers}\n- Content:{result.content}' ,"debug")

    except Exception as error:
        log(f"MetOffice: Could not send results: {error}", "error")


metoffice_converted_weatherreports = 0
def convertAndSend():
    global metoffice_converted_weatherreports
    config = parse()

    send_metoffice_cache_connection = cache.create_connection(config["Cache"]["File"]) # Opens a connection to the cache. Every thread needs its own connection.

    weatherreports = cache.get_weather(send_metoffice_cache_connection)

    if len(weatherreports) > 0:
        weatherreports['timestamp'] = pandas.to_datetime(weatherreports['timestamp'],unit='s')


        today = datetime.datetime.utcnow().date()
        weatherreports_current_day = weatherreports[weatherreports['timestamp'].between(datetime.datetime(today.year, today.month, today.day), datetime.datetime.utcnow())]
        weatherreports_current_day = weatherreports_current_day.groupby('device_id').agg({'rainfall_mm': 'sum'}) # Get the the total rainfall since the beginning of the current day.
        weatherreports_current_day = weatherreports_current_day.add_prefix('current_day_')

        weatherreports_since_last_observation = weatherreports[weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(minutes=int(config['MetOffice']['SendIntervalInMinutes'])), datetime.datetime.utcnow())]
        weatherreports_since_last_observation = weatherreports_since_last_observation.groupby('device_id').agg({'temperature_c':'mean',
                                                                               'humidity':'mean',
                                                                               'rainfall_mm': 'sum',
                                                                               'wind_speed_avg_ms':'mean',
                                                                               'wind_direction_degrees': 'mean'})
        weatherreports_since_last_observation = weatherreports_since_last_observation.add_prefix('since_last_observation_')

        weatherreports_concatenated = pandas.merge(weatherreports_current_day, weatherreports_since_last_observation, left_index=True, right_index=True)


        for index, device in weatherreports_concatenated.iterrows():
            line = weatherdata() # Use the weatherdata class of the weatherunderground library. This class makes it able to store values and return them in the format required for sending messages to weatherunderground.
            line.dateutc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            line.dailyrainin = round(float(device["current_day_rainfall_mm"]) * mm_to_inch, 2)
            dew_point_c = dew_point(float(device["since_last_observation_temperature_c"]), device["since_last_observation_humidity"])
            line.dewptf = round(float(celcius_to_fahrenheit(dew_point_c)), 2)
            line.humidity = round(float(device["since_last_observation_humidity"]), 2)
            line.rainin = round(float(device["since_last_observation_rainfall_mm"]) * mm_to_inch, 2)
            line.tempf = round(float(celcius_to_fahrenheit(device["since_last_observation_temperature_c"])), 2)
            line.winddir = round(float(device["since_last_observation_wind_direction_degrees"]), 2)
            line.windspeedmph = round(float(device["since_last_observation_wind_speed_avg_ms"]) * ms_to_mph, 2)
            
            send(config["MetOffice"]["ID"], config["MetOffice"]["Key"], config["MetOffice"]["Softwaretype"], line.__dict__)

            metoffice_converted_weatherreports += 1
            if metoffice_converted_weatherreports % config.getint("MetOffice","NtfyEveryXWeatherreports") == 0:
                log(f"Met Office: Saved '{metoffice_converted_weatherreports}' weatherreports to the Met Office.\nCurrent weather:\n - Temperature: {device['since_last_observation_temperature_c']} C\n - Humidity: {device['since_last_observation_humidity']} %\n - Rainfall: {device['since_last_observation_rainfall_mm']} mm", "report")

    elif len(weatherreports) > 0 and len(weatherreports) <= 20:
        log(f"Met Office: Not enough weatherreports to summarize. There need to be at least 20. Currently there are {len(weatherreports)} weatherreports. Please make sure that the receiving antenna is close enough to the weatherstation.", "info")
    else:
        log(f"Met Office: There are no weatherreports to summarize. Please make sure that the weatherdevice has been turned on.", "info")


