#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
print("""
#####################################
#
#  RTL-433 weatherstation handler
#
#####################################
#
# - Author: https://Github.com/Riktastic
#
##
""")

import threading
import schedule

from libs.logger import log
from libs.config import parse
from libs.calculations import *
import libs.cache as cache
import libs.database as database
import connectors.metoffice as metoffice
import connectors.openweathermap as openweathermap
import connectors.pwsweather as pwsweather
import connectors.weatherunderground as weatherunderground
import connectors.windy as windy

# Required for parsing the output of the RTL-SDR receiver.
import subprocess
import sys
import os
import json
import re
import time
from dateutil import parser
# Required: https://github.com/merbanan/rtl_433

#################################
# Main
##

def getWeatherreports(): # Runs continuesly. Run only once per computer.
    get_count = 0 # Counts the amount of 

    log("Startup: Parsing the config file.", "info")
    get_config = parse() # Parses and recieves the config file.

    log("Startup: Opening the cache.", "info")
    get_cache_connection = cache.create_connection(get_config["Cache"]["File"]) # Create a connection to the config file.

    log("Startup: Cache has been opened.", "info")

    log("Startup: Connecting to the database.", "info")
    get_database_connection = database.create_connection(get_config["Database"]["Host"], get_config["Database"]["Port"], get_config["Database"]["User"], get_config["Database"]["Password"]) # Create a connection to the database.

    log("Startup: Connected to the database.", "info")

    kill_rtl_433_cmd = "pkill -9 rtl_433"
    os.system(kill_rtl_433_cmd) # Make sure to kill any running rtl_433 processes.

    log("Startup: Starting RTL_433", "info")
    rtl_433_cmd = "rtl_433 -f 868M -s 1024k -R 119 -M time:iso -M level -F json" # "-F json": Outputs received data as json. "-f 868M" listens at the 868Mhz bandwidth. "-R 119" uses the 119th algorithm of rtl_433 to decode  received messages.
    rtl433_proc = subprocess.Popen(rtl_433_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)

    rtl_output_found_sdr_re = re.compile("^(Found)")  # Matches whenever a device has been found.

    log("Startup: Ready to start listening for weatherreports.", "info")
    while True: # Runs forever.
        if rtl433_proc.poll() is not None: # Triggers whenever the RTL_433 dongle can't be found.
            log(f"RTL_433: Exited with code, can't continue: {str(rtl433_proc.poll())}", "error")
            sys.exit(rtl433_proc.poll())
            quit()

        global timer, timeout
        if (time.process_time() - timer) >= 60*timeout:
            log(f"Time since last message has passed the {timeout} minutes timeout.")
            quit()

        for line in iter(rtl433_proc.stdout.readline, ''): # run for each line.
            if rtl_output_found_sdr_re.match(line): # Check for certain lines.
                log(f"RTL_433: Found a SDR: {line.lstrip('Found ').strip()}", "info")

            if "time" in line: # If the result contains the word "time", do the following:
                
                timer = time.process_time() # Reset the timer.

                line_dict = json.loads(line) # Convert the line to a dictionary by first reading it as JSON.
                model = line_dict["model"] # Get the model name.
                model_id = line_dict["id"] # Get the model id.

                if model == "Bresser-5in1": # Make sure that we only register Alecto WS-4800 devices. 

                # If the device hasn't been registered in the cache.
                    if not cache.check_device(get_cache_connection, model, model_id):
                        log(f"'{model}' with model ID '{model_id}' could not be found in the cache. Checking if we need to update the cache or need to add the device into the database.", "info")
                        if not database.check_device(get_database_connection, get_config["Database"]["Database"], model, model_id): # Check if the device has been registered in the cache.
                            log(f"'{model}' with model ID '{model_id}' could not be found in the database. Adding it and updating the cache.", "info")
                            database.insert_device(get_database_connection, get_config["Database"]["Database"], [model, model_id, 0, 0, "Unknown"]) # If it hasn't been registered in the cache, then add it.
                        else:
                            log(f"'{model}' with model ID '{model_id}' was found in the database. Updating the cache.", "info")

                        cache.empty_table(get_cache_connection, "device") # Empty the cache table. It will be updated using the list of devices from the database.
                        devices = database.get_devices(get_database_connection, get_config["Database"]["Database"],) # Export the devices table from the database.
                        for device in devices: # For each row within the devices table.
                            cache.insert_device(get_cache_connection, device) # Insert each row into the cache.
                    
                        log(f'Cache: The cache has been updated.', "info")
                    
                    device_id = cache.check_device(get_cache_connection, model, model_id)
                    previous_weatherreport = cache.get_previous_weather(get_cache_connection, device_id)

                    if len(previous_weatherreport > 0):
                        rainfall_accumulated = previous_weatherreport['rainfall_mm_accumulated_since_boot']
                    else:
                        rainfall_accumulated = line_dict["rain_mm"]

                    rainfall_mm = line_dict["rain_mm"] - rainfall_accumulated

                    line_cache = cache.weatherdata() # Creates a class that will be used to store weatherdata.
                    line_cache.device_id = device_id
                    
                    timestamp_datetime = parser.parse(line_dict["time"]) # Convert the timestamp to a ISO 8601 timestamp (without zone).

                    line_cache.timestamp_unix = time.mktime(timestamp_datetime.timetuple())
                    line_cache.temperature_c = round(float(line_dict["temperature_C"]), 2)
                    line_cache.humidity = line_dict["humidity"]
                    line_cache.rainfall_mm = (round(float(rainfall_mm), 2))
                    line_cache.rainfall_mm_accumulated_since_boot = (round(float(line_dict["rain_mm"]), 2))
                    line_cache.wind_speed_max_ms = round(float(line_dict["wind_max_m_s"]), 4)
                    line_cache.wind_speed_avg_ms = round(float(line_dict["wind_avg_m_s"]), 4)
                    line_cache.wind_direction_degrees = round(line_dict["wind_dir_deg"], 2)
                    line_cache.device_battery_status = bool(line_dict["battery_ok"])
                    line_cache.device_noise = round(float(line_dict["noise"]), 2)
                    line_cache.device_rssi = round(float(line_dict["rssi"]), 2)
                    line_cache.device_snr = round(float(line_dict["snr"]), 2)
                    line_cache.device_modulation = line_dict["mod"]
                    line_cache.device_frequency_1 = round(float(line_dict["freq1"]), 2)
                    line_cache.device_frequency_2 = round(float(line_dict["freq2"]), 2)

                    cache.insert_weather(get_cache_connection, line_cache.get_list())

                    get_count+=1 # Count the amount of weathereports that have been saved to the cache.

                    if get_count == 1:
                        log(f"Saved the first weatherreport since starting the handler to the cache.", "info")

                    if get_count == 10:
                        log(f"Saved the first 10 weatherreports since starting the handler to the cache. From now on messages will be summarized per x received weatherreports.", "info")


                    if get_count % 1000 == 0:
                        log(f"Saved '{get_count}' weatherreports to the cache.", "info")


def emptyCache(): # Seperate function for emptying the cache. Has to be seperate to be able to be called from the scheduler.
    log("Cache: It is 24:00. Emptying the cache.", "info")
    emptyCache_config = parse() # Get the configfile.
    cache.empty_table(cache.create_connection(emptyCache_config["Cache"]["File"]), "weather") # Empty the cache using the configfile options.


def startScheduler():
    log("Startup: Started the scheduler.", "info")
    while True:
        schedule.run_pending()
        time.sleep(15) # Suspend for 15 seconds. Before starting the loop again.


config = parse()
timer = time.process_time()
timeout = 15 #minutes

if config.getboolean('MetOffice','Enabled') == True: schedule.every(config.getint('MetOffice','SendIntervalInMinutes')).minutes.do(metoffice.convertAndSend) ; log("Startup: The Met Office connector has been enabled.", "info")
if config.getboolean('OpenWeathermap','Enabled') == True: schedule.every(config.getint('OpenWeathermap','SendIntervalInMinutes')).minutes.do(openweathermap.convertAndSend) ; log("Startup: The OpenWeathermap connector has been enabled.", "info")
if config.getboolean('PWSWeather','Enabled') == True: schedule.every(config.getint('PWSWeather','SendIntervalInMinutes')).minutes.do(pwsweather.convertAndSend) ; log("Startup: The PWSWeather connector has been enabled.", "info")
if config.getboolean('WeatherUnderground','Enabled') == True: schedule.every(config.getint('WeatherUnderground','SendIntervalInMinutes')).minutes.do(weatherunderground.convertAndSend) ; log("Startup: The WeatherUnderground connector has been enabled.", "info")
if config.getboolean('Windy','Enabled') == True: schedule.every(config.getint('Windy','SendIntervalInMinutes')).minutes.do(windy.convertAndSend) ; log("Startup: The Windy connector has been enabled.", "info")
schedule.every(config.getint('Database','SendIntervalInMinutes')).minutes.do(database.convertAndSend)
schedule.every().day.at("00:00").do(emptyCache)

# Start 2 threads. To be able to run 2 continues loops at the same time. The first one is for getting the weatherreports.
thread_getWeatherreports = threading.Thread(target=getWeatherreports)
thread_getWeatherreports.start()

# The second thread is for starting the scheduler.
thread_startScheduler = threading.Thread(target=startScheduler)
thread_startScheduler.start()

# print(openweathermap.create("id", "name", "shortname", "lat", "lon", "height"))
