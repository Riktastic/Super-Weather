# Info: https://community.windy.com/topic/8168/report-your-weather-station-data-to-windy

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
        self.temp = 0.0 # Instant celcius V
        self.wind = 0.0 # Instant windspeedms V
        self.winddir = 0.0 # Instant V
        self.rh = 0.0 # Instant humidity V
        self.dewpoint = 0.0 # Instant in fahrenheit V     
        self.precip = 0.0 # 60 min mm V

def send(id, key, data):
    config = parse()

    try:
        url = f'https://stations.windy.com./pws/update/{str(key)}'
        
        params = {
            'station':          str(id),
            'dateutc':          str(data['dateutc']),
            'temp':             float(data['temp']),
            'wind':             float(data['wind']),
            'winddir':          float(data['winddir']),
            'rh':               float(data['rh']),
            'dewpoint':         float(data['dewpoint']),
            'precip':           float(data['precip'])}

        result = requests.post(url=url, params=params)
        result.raise_for_status()

        if result.content != b'SUCCESS' or result.status_code != 200:
            raise Exception(f"Windy didn't acknowledge the success of the update. Response of the server:\n - Status-code: {result.status_code}\n - Content: {result.content}") 
        if config.getboolean("Windy","Debug") == True:
            log(f'Windy: Response to "{result.url}": \n- Headers: {result.headers}\n- Content:{result.content}' ,"debug")


    except Exception as error:
        log(f"Windy: Could not send results: {error}", "error")

windy_received_weatherreports = 0
def convertAndSend():
    global windy_received_weatherreports
    config = parse()

    send_windy_cache_connection = cache.create_connection(config["Cache"]["File"]) # Opens a connection to the cache. Every thread needs its own connection.

    weatherreports = cache.get_weather(send_windy_cache_connection)

    if len(weatherreports) > 0:
        weatherreports['timestamp'] = pandas.to_datetime(weatherreports['timestamp'],unit='s')

        weatherreports_1hour = weatherreports[weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(hours=1), datetime.datetime.utcnow())]
        weatherreports_1hour = weatherreports_1hour.groupby('device_id').agg({'rainfall_mm': 'sum'}) # Get the total rainfall within the last 1 hour.
        weatherreports_1hour = weatherreports_1hour.add_prefix('1hour_')

        weatherreports_since_last_observation = weatherreports[weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(minutes=int(config['Windy']['SendIntervalInMinutes'])), datetime.datetime.utcnow())]
        weatherreports_since_last_observation = weatherreports_since_last_observation.groupby('device_id').agg({'temperature_c':'mean', # Get the average temperature, humidity, wind speed and winddirection of the last minute. These values are being used as the instantaneous values. The 1 minute is to make sure that any errors within the reading are being averaged out.
                                                                               'humidity':'mean',
                                                                               'wind_speed_avg_ms':'mean',
                                                                               'wind_direction_degrees': 'mean'})
        weatherreports_since_last_observation = weatherreports_since_last_observation.add_prefix('since_last_observation_')

        weatherreports_concatenated = pandas.merge(weatherreports_1hour, weatherreports_since_last_observation, left_index=True, right_index=True)


        for index, device in weatherreports_concatenated.iterrows():
            line = weatherdata() # Use the weatherdata class of the weatherunderground library. This class makes it able to store values and return them in the format required for sending messages to weatherunderground.
            line.dateutc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            line.temp = round(float(device["since_last_observation_temperature_c"]), 2)
            line.wind = round(float(device["since_last_observation_wind_speed_avg_ms"]), 2)
            line.winddir = round(float(device["since_last_observation_wind_direction_degrees"]), 2)
            line.rh = round(float(device["since_last_observation_humidity"]), 2)
            line.dewpoint = dew_point(float(device["since_last_observation_temperature_c"]), device["since_last_observation_humidity"])
            line.precip = round(float(device["1hour_rainfall_mm"]), 2)

            send(config["Windy"]["ID"], config["Windy"]["Key"], line.__dict__)

            windy_received_weatherreports += 1
            if windy_received_weatherreports % config.getint("Windy","NtfyEveryXWeatherreports")  == 0:
                log(f"Windy: Saved '{windy_received_weatherreports}' weatherreports to Windy.\nCurrent weather:\n - Temperature: {device['since_last_observation_temperature_c']} C\n - Humidity: {device['since_last_observation_humidity']} %\n - Rainfall: {device['1hour_rainfall_mm']} mm since last hour.", "report")
        

    elif len(weatherreports) > 0 and len(weatherreports) <= 20:
        log(f"Windy: Not enough weatherreports to summarize. There need to be at least 20. Currently there are {len(weatherreports)} weatherreports. Please make sure that the receiving antenna is close enough to the weatherstation.", "info")
    else:
        log(f"Windy: There are no weatherreports to summarize. Please make sure that the weatherdevice has been turned on.", "info")
