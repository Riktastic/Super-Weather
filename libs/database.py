from libs.logger import log
import psycopg2
import datetime
import pandas

from libs.config import parse
import libs.cache as cache
from libs.calculations import *

class weatherdata:
    def __init__(self):
        self.device_id = None
        self.timestamp = None
        self.temperature_c = None
        self.temperature_f = None
        self.temperature_k = None
        self.humidity = None
        self.rainfall_mm = None 
        self.rainfall_inch = None
        self.rainfall_mm_accumulated_since_boot = None
        self.rainfall_inch_accumulated_since_boot = None
        self.dew_point_c = None
        self.dew_point_f = None
        self.dew_point_k = None
        self.cloud_base_m = None
        self.cloud_base_f = None
        self.wind_speed_max_ms = None
        self.wind_speed_max_mph = None
        self.wind_speed_max_kmh = None
        self.wind_speed_max_knots = None
        self.wind_speed_max_bft = None
        self.wind_speed_avg_ms = None
        self.wind_speed_avg_mph = None
        self.wind_speed_avg_kmh = None
        self.wind_speed_avg_knots = None
        self.wind_speed_avg_bft = None
        self.wind_chill = None
        self.heat_index = None
        self.wind_direction_degrees = None
        self.wind_direction_cardinal = None
        self.device_battery_status = None
        self.device_noise = None
        self.device_rssi = None
        self.device_snr = None
        self.device_modulation = None
        self.device_frequency_1 = None
        self.device_frequency_2 = None

    def get_list(self):
        return [self.device_id, self.timestamp, self.temperature_c, self.temperature_f, self.temperature_k, self.humidity, self.rainfall_mm, self.rainfall_inch, self.rainfall_mm_accumulated_since_boot, self.rainfall_inch_accumulated_since_boot, self.dew_point_c, self.dew_point_f, self.dew_point_k, self.cloud_base_m, self.cloud_base_f, self.wind_speed_max_ms, self.wind_speed_max_mph, self.wind_speed_max_kmh, self.wind_speed_max_knots, self.wind_speed_max_bft, self.wind_speed_avg_ms, self.wind_speed_avg_mph, self.wind_speed_avg_kmh, self.wind_speed_avg_knots, self.wind_speed_avg_bft, self.wind_chill, self.heat_index, self.wind_direction_degrees, self.wind_direction_cardinal, self.device_battery_status, self.device_noise, self.device_rssi, self.device_snr, self.device_modulation, self.device_frequency_1, self.device_frequency_2]


sql_create_table_weather = """
        CREATE TABLE #database#.weather (
            weatherreport_id SERIAL PRIMARY KEY,
            device_id bigint NOT NULL,
            timestamp timestamp NOT NULL,
            temperature_c real,
            temperature_f real,
            temperature_k real,
            humidity real,
            rainfall_mm real,
            rainfall_inch real,
            rainfall_mm_accumulated_since_boot real,
            rainfall_inch_accumulated_since_boot real,
            dew_point_c real,
            dew_point_f real,
            dew_point_k real,
            cloud_base_m real,
            cloud_base_f real,
            wind_speed_max_ms real,
            wind_speed_max_mph real,
            wind_speed_max_kmh real,
            wind_speed_max_knots real,
            wind_speed_max_bft smallint,
            wind_speed_avg_ms real,
            wind_speed_avg_mph real,
            wind_speed_avg_kmh real,
            wind_speed_avg_knots real,
            wind_speed_avg_bft smallint,
            wind_chill real,
            heat_index real,
            wind_direction_degrees smallint, 
            wind_direction_cardinal character varying(3),
            device_battery_status boolean,
            device_noise real,
            device_rssi real,
            device_snr real,
            device_modulation character varying(6),
            device_frequency_1 real,
            device_frequency_2 real
        )
        """

sql_create_table_device = """
        CREATE TABLE #database#.device (
                device_id SERIAL PRIMARY KEY,
                model character varying(20),
                model_id character varying(20),
                lon real,
                lat real,
                location character varying(100)
        )
        """

def create_connection(host, port, user, password):
    connection = None
    try:
        # connect to the PostgreSQL server
        connection = psycopg2.connect(host=host, port=port, user=user, password=password)
    except (Exception, psycopg2.DatabaseError) as error:
        log(f"Database: Failed to connect to '{host}:{port}'", "error")
        return -1
    else:
        if connection is not None:
            return connection
        else:
            return -1

def create_database(connection, database):
    try:
        cursor = connection.cursor()

        sql = f"""
            CREATE SCHEMA {database};
            """

        cursor.execute(sql)
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        cursor.close()
        log(f"Database: Failed to create database [{database}]: {error}", "error")
        return -1
    else:
        cursor.close()
        log(f"Database: Created database [{database}].", "info")        
        return True

def create_table(connection, sql):
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        cursor.close()
        log(f"Database: Failed to create the weatherreport table: {error}", "error")
        return -1
    else:
        cursor.close()
        log(f"Database: Created table using: {sql}", "info") 
        return True

def check_database(connection, database):
    try:
        cursor = connection.cursor()

        sql = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.schemata
                WHERE  schema_name = '{database}'
            );
            """

        cursor.execute(sql)
        answer = cursor.fetchall()[0][0]
    except (Exception, psycopg2.DatabaseError) as error:
        cursor.close()
        log(f"Database: Failed to check if database [{database}] exists: {error}", "error")
        return -1
    else:
        cursor.close()
        return answer

def get_devices(connection, database):
    table = "device"
    try:
        cursor = connection.cursor()

        sql = f"""
            SELECT * FROM {database}.{table};
            """

        cursor.execute(sql)
        answer = cursor.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        cursor.close()
        log(f"Database: Failed to get devices: {error}", "error")
        return -1
    else:
        cursor.close()
        return answer


def check_table(connection, database, table):
    try:
        cursor = connection.cursor()

        sql = f"""
            SELECT EXISTS (
                SELECT FROM pg_tables
                WHERE  schemaname = '{database}'
                AND    tablename  = '{table}'
            );
            """

        cursor.execute(sql)
        answer = cursor.fetchall()[0][0]
    except (Exception, psycopg2.DatabaseError) as error:
        cursor.close()
        log(f"Database: Failed to check if table [{database}.{table}] exists: {error}", "error")
        return -1
    else:
        cursor.close()
        return answer


def check_device(connection, database, model, model_id):
    table = "device"
    try:
        cursor = connection.cursor()

        sql = f"""
            SELECT EXISTS (
                SELECT FROM {database}.{table}
                WHERE  model = '{model}'
                AND    model_id  = '{model_id}'
            );
            """

        cursor.execute(sql)
        answer = cursor.fetchall()[0][0]
    except (Exception, psycopg2.DatabaseError) as error:
        cursor.close()
        log(f"Database: Failed to check if a device with model '{model}' and a model_id of {model_id} exists: {error}", "error")
        return -1
    else:
        cursor.close()
        return answer


def insert_weather(connection, database, data):
    table = "weather"
    sql = f"INSERT INTO {database}.{table} (device_id, timestamp, temperature_c, temperature_f, temperature_k, humidity, rainfall_mm, rainfall_inch, rainfall_mm_accumulated_since_boot, rainfall_inch_accumulated_since_boot, dew_point_c, dew_point_f, dew_point_k, cloud_base_m, cloud_base_f, wind_speed_max_ms, wind_speed_max_mph, wind_speed_max_kmh, wind_speed_max_knots, wind_speed_max_bft,  wind_speed_avg_ms, wind_speed_avg_mph, wind_speed_avg_kmh, wind_speed_avg_knots, wind_speed_avg_bft, wind_chill, heat_index, wind_direction_degrees, wind_direction_cardinal, device_battery_status, device_noise, device_rssi, device_snr, device_modulation, device_frequency_1, device_frequency_2) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING weatherreport_id"
    try:
        cursor = connection.cursor()
        cursor.execute(sql, data)
        weatherreport_id = cursor.fetchone()[0]
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        cursor.close()
        log(f"Database: Failed to insert new weatherreport: {error}", "error")
        return -1
    else:
        cursor.close()
        return weatherreport_id


def insert_device(connection, database, data):
    table = "device"
    sql = f"INSERT INTO {database}.{table} (model, model_id, lon, lat, location) VALUES(%s, %s, %s, %s, %s) RETURNING device_id"
    device_id = None
    try:
        cursor = connection.cursor()
        cursor.execute(sql, data)
        device_id = cursor.fetchone()[0]
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        cursor.close()
        log(f"Database: Failed to insert new device: {error}", "error")
        return -1
    else:
        cursor.close()
        return device_id


database_converted_weatherreports = 0
def convertAndSend():
    global database_converted_weatherreports # Get a pointer to the count.
    sendWeatherreport_to_database_config = parse()

    send_database_cache_connection = cache.create_connection(sendWeatherreport_to_database_config["Cache"]["File"]) # Opens a connection to the cache. Every thread needs its own connection.
    send_database_connection = create_connection(sendWeatherreport_to_database_config["Database"]["Host"], sendWeatherreport_to_database_config["Database"]["Port"], sendWeatherreport_to_database_config["Database"]["User"], sendWeatherreport_to_database_config["Database"]["Password"]) # Opens a connection to the database. Every thread needs its own connection.

    latest_weatherreports = cache.get_weather(send_database_cache_connection)

    if len(latest_weatherreports) > 0:
        latest_weatherreports['timestamp'] = pandas.to_datetime(latest_weatherreports['timestamp'],unit='s')

        latest_weatherreports_since_last_observation = latest_weatherreports[latest_weatherreports['timestamp'].between(datetime.datetime.utcnow() - datetime.timedelta(minutes=int(sendWeatherreport_to_database_config['Database']['SendIntervalInMinutes'])), datetime.datetime.utcnow())]

        latest_weatherreports_since_last_observation = latest_weatherreports_since_last_observation.groupby('device_id').agg({'temperature_c':'mean', 
                                                            'humidity':'mean',
                                                            'rainfall_mm':'sum',
                                                            'rainfall_mm_accumulated_since_boot':'max',
                                                            'wind_speed_max_ms':'max',
                                                            'wind_speed_avg_ms':'mean',
                                                            'wind_direction_degrees': "mean",                                                  
                                                            'device_battery_status': lambda x: 0 if x.any() == 0 else 1,
                                                            'device_noise':'mean',
                                                            'device_rssi':'mean',
                                                            'device_snr':'mean',
                                                            'device_modulation': pandas.Series.mode,
                                                            'device_frequency_1':'mean',
                                                            'device_frequency_2':'mean'})

        for index, device in latest_weatherreports_since_last_observation.iterrows():
            line = weatherdata()
            line.device_id = int(device.name)
            line.timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')

            line.temperature_c = round(float(device["temperature_c"]), 2)
            line.temperature_f = round(float(celcius_to_fahrenheit(device["temperature_c"])), 2)
            line.temperature_k = round(float(celcius_to_kelvin(device["temperature_c"])), 2)

            line.humidity = float(device["humidity"])

            line.rainfall_mm = round(float(device["rainfall_mm"]), 2)
            line.rainfall_inch = round(float(device["rainfall_mm"] * mm_to_inch), 2)

            line.rainfall_mm_accumulated_since_boot = round(float(device["rainfall_mm_accumulated_since_boot"]), 2)
            line.rainfall_inch_accumulated_since_boot = round(float(device["rainfall_mm_accumulated_since_boot"] * mm_to_inch), 2)

            dew_point_c = dew_point(float(device["temperature_c"]), device["humidity"])

            line.dew_point_c = round(float(dew_point_c), 2)
            line.dew_point_f = round(float(celcius_to_fahrenheit(dew_point_c)), 2)
            line.dew_point_k = round(float(celcius_to_kelvin(dew_point_c)), 2)

            cloud_base_m = cloud_base(round(device["temperature_c"]), dew_point_c)
            line.cloud_base_m = round(float(cloud_base_m), 2)
            line.cloud_base_f = round(float(cloud_base_m)* m_to_feet)

            line.wind_speed_max_ms = round(float(device["wind_speed_max_ms"]), 2)
            line.wind_speed_max_mph = round(float(device["wind_speed_max_ms"]) * ms_to_mph, 2)

            line.wind_speed_max_kmh = round(float(device["wind_speed_max_ms"])* ms_to_kmh, 2)
            line.wind_speed_max_knots = round(float(device["wind_speed_max_ms"])* ms_to_knots, 2)
            line.wind_speed_max_bft = windspeed_to_bft(float(device["wind_speed_max_ms"]))

            line.wind_speed_avg_ms = round(float(device["wind_speed_avg_ms"]), 2)
            line.wind_speed_avg_mph = round(float(device["wind_speed_avg_ms"]) * ms_to_mph, 2)
            line.wind_speed_avg_kmh = round(float(device["wind_speed_avg_ms"]) * ms_to_kmh, 2)
            line.wind_speed_avg_knots = round(float(device["wind_speed_avg_ms"]) * ms_to_knots, 2)
            line.wind_speed_avg_bft = windspeed_to_bft(float(device["wind_speed_avg_ms"]))

            line.wind_chill = round(wind_chill(float(device["wind_speed_avg_ms"]), float(device["temperature_c"])),2)

            line.heat_index = heat_index(device["temperature_c"], device["humidity"])

            line.wind_direction_degrees = round(float(device["wind_direction_degrees"]), 2)

            line.wind_direction_cardinal = str(coordinate_degrees_to_cardinal(device["wind_direction_degrees"]))

            line.device_battery_status = bool(device["device_battery_status"])
            line.device_noise = round(float(device["device_noise"]), 2)
            line.device_rssi = round(float(device["device_rssi"]), 2)
            line.device_snr = round(float(device["device_snr"]), 2)
            line.device_modulation = str(device["device_modulation"])
            line.device_frequency_1 = round(float(device["device_frequency_1"]), 2)
            line.device_frequency_2 = round(float(device["device_frequency_2"]), 2)
            insert_weather(send_database_connection, sendWeatherreport_to_database_config['Database']['Database'], line.get_list())
 
            database_converted_weatherreports += 1
            if database_converted_weatherreports % config.getint("Database","NtfyEveryXWeatherreports")  == 0:
                log(f"Database: Saved '{database_converted_weatherreports}' weatherreports to the database. Current weather:\n - Temperature: {device['temperature_c']} C\n - Humidity: {device['humidity']} %\n - Rainfall: {device['rainfall_mm']} mm.", "report")


    elif len(latest_weatherreports) > 0 and len(latest_weatherreports) <= 20:
        log(f"Database: Not enough weatherreports to summarize. There need to be at least 20. Currently there are {len(latest_weatherreports)} weatherreports. Please make sure that the receiving antenna is close enough to the weatherstation.", "info")
    else:
        log(f"Database: There are no weatherreports to summarize. Please make sure that the weatherdevice has been turned on.", "info")


config = parse()

log(f"Database: Checking if the databaseserver exists and has been configured at: '{config['Database']['Host']}:{config['Database']['Port']}'.", "info")
connection = create_connection(config["Database"]["Host"], config["Database"]["Port"], config["Database"]["User"], config["Database"]["Password"])

if connection != -1:
    if not check_database(connection, config["Database"]["Database"]):
        if create_database(connection, config["Database"]["Database"]) == -1:
            quit()

    if not check_table(connection, config["Database"]["Database"], "weather"):
        sql = sql_create_table_weather.replace("#database#", config["Database"]["Database"])
        if create_table(connection, sql) == -1:
            quit()

    if not check_table(connection, config["Database"]["Database"], "device"):
        sql = sql_create_table_device.replace("#database#", config["Database"]["Database"])
        if create_table(connection, sql) == -1:
            quit()

    log(f"Database: The database is ready.", "info")