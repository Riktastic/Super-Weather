from libs.config import parse
from libs.logger import log
import pandas

# Required for checking if the cache-database exists.
from os.path import exists

# Required for all general SQLite tasks
import sqlite3
import datetime
import time

class weatherdata:
    def __init__(self):
        self.device_id = None
        self.timestamp_unix = None
        self.temperature_c = None
        self.humidity = None
        self.rainfall_mm = None
        self.rainfall_mm_accumulated_since_boot = None
        self.wind_speed_max_ms = None
        self.wind_speed_avg_ms = None
        self.wind_direction_degrees = None
        self.device_battery_status = None
        self.device_noise = None
        self.device_rssi = None
        self.device_snr = None
        self.device_modulation = None
        self.device_frequency_1 = None
        self.device_frequency_2 = None

    def get_list(self):
        return [self.device_id , self.timestamp_unix, self.temperature_c, self.humidity, self.rainfall_mm, self.rainfall_mm_accumulated_since_boot, self.wind_speed_max_ms, self.wind_speed_avg_ms, self.wind_direction_degrees , self.device_battery_status , self.device_noise , self.device_rssi , self.device_snr , self.device_modulation , self.device_frequency_1 , self.device_frequency_2 ]



sql_create_table_weather = """
        CREATE TABLE weather (
            device_id INTEGER,
            timestamp INTEGER,
            temperature_c REAL,
            humidity REAL,
            rainfall_mm REAL,
            rainfall_mm_accumulated_since_boot REAL,
            wind_speed_max_ms REAL,
            wind_speed_avg_ms REAL,
            wind_direction_degrees INTEGER, 
            device_battery_status INTEGER,
            device_noise REAL,
            device_rssi REAL,
            device_snr REAL,
            device_modulation TEXT,
            device_frequency_1 REAL,
            device_frequency_2 REAL
        )
        """


sql_create_table_device = """
        CREATE TABLE device (
                device_id INTEGER PRIMARY KEY,
                model TEXT,
                model_id TEXT,
                lon REAL,
                lat REAL,
                location TEXT
        )
        """


sql_create_table_info = """
        CREATE TABLE info (
                info_id INTEGER PRIMARY KEY,
                timestamp ,
                version REAL,
                version_database REAL,
                operation TEXT,
                info TEXT
        )
        """


def create_database(file):
    connection = None
    try:
        connection = sqlite3.connect(file)
        create_table(connection, sql_create_table_info)

        timestamp = datetime.datetime.now()
        timestamp_unix = time.mktime(timestamp.timetuple())

        config = parse()
        cursor = connection.cursor()

        info = (timestamp_unix, config["General"]["Version"], config["General"]["VersionCache"], "create_cache", "The cachedatabase has been created.")
        sql = f''' INSERT INTO info(timestamp,version,version_database,operation,info) VALUES(?,?,?,?,?) '''
        cursor.execute(sql,info)
        connection.commit()

    except sqlite3.Error as error:
        log(f"Cache: Failed to create a database: {error}", "error")
        connection.rollback()
        quit()

    return connection


def create_connection(file):
    connection = None
    try:
        connection = sqlite3.connect(file)
    except sqlite3.Error as error:
        log(f"Cache: Failed to connect to database: {error}", "error")
        quit()

    return connection


def create_table(connection, create_table_sql):
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_sql)
    except sqlite3.Error as error:
        log(f"Cache: Failed to create a table: {error}", "error")

def check_table(connection,table):
    answer = False
    try:
        cursor = connection.cursor()
        sql = f"""
            SELECT tbl_name FROM sqlite_master
            WHERE type='table' AND tbl_name  = '{table}'
            """
        cursor.execute(sql)
        results = cursor.fetchall()[0]
        if len(results) > 0:
            answer = True
        cursor.close()

    except sqlite3.Error as error:
        log(f"Cache: Failed to check if table [{table}] exists: {error}", "error")
        quit()
    finally:
        return answer


def get_weather(connection):
    answer = pandas.DataFrame()
    try:
        sql = f"SELECT * FROM weather;"
        answer = pandas.read_sql_query(sql, connection)

    except Exception as error:
        log(f"Cache: Failed to get all weatherreports: {error}", "error")
        quit()
    finally:
        return answer

def get_previous_weather(connection,device_id):
    answer = pandas.DataFrame()
    try:
        sql = f"SELECT rainfall_mm_accumulated_since_boot FROM weather WHERE device_id = {device_id} ORDER BY timestamp DESC LIMIT 1;"
        answer = pandas.read_sql_query(sql, connection)
    except Exception as error:
        log(f"Cache: Failed to get the last weatherreport: {error}", "error")
        quit()
    finally:
        return answer

def delete_table(connection, table):
    sql = f"DROP TABLE {table}"
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()


def empty_table(connection, table):
    sql = f"DELETE FROM '{table}';"
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()


def insert_weather(connection, data):
    table = "weather"
    sql = f''' INSERT INTO {table}(device_id,timestamp,temperature_c,humidity,rainfall_mm,rainfall_mm_accumulated_since_boot,wind_speed_max_ms,wind_speed_avg_ms,wind_direction_degrees,device_battery_status,device_noise,device_rssi,device_snr,device_modulation,device_frequency_1,device_frequency_2)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'''
    try:
        cursor = connection.cursor()
        cursor.execute(sql, data)
        connection.commit()
        return cursor.lastrowid
    except Exception as error:
        log(f"Cache: Failed to insert new weatherreport: {error}", "error")



def insert_device(connection, data):
    table = "device"
    sql = f''' INSERT INTO {table}(device_id,model,model_id,lon,lat,location)
              VALUES(?,?,?,?,?,?) '''
    cursor = connection.cursor()
    cursor.execute(sql, data)
    connection.commit()
    return cursor.lastrowid


def insert_info(connection, data):
    table = "info"
    sql = f''' INSERT INTO {table}(info_id,timestamp,version,version_database,operation,info)
              VALUES(?,?,?,?,?,?) '''
    cursor = connection.cursor()
    cursor.execute(sql, data)
    connection.commit()
    return cursor.lastrowid

def check_device(connection, model, model_id):
    table = "device"
    sql = f'''SELECT EXISTS(SELECT 1 FROM {table} WHERE model="{model}" AND model_id="{model_id}");'''
    cursor = connection.cursor()
    cursor.execute(sql)
    return cursor.fetchone()[0]


config = parse()
file = config["Cache"]["File"]

log(f"Cache: Checking if the databaseserver exists and has been configured at: '{file}'.", "info")
if not exists(file):
    log(f"Cache: Could not find '{file}'. Trying to create the cachefile.", "info")
    connection = create_database(file)
    create_table(connection, sql_create_table_device)
    create_table(connection, sql_create_table_weather)
    log(f"Cache: Database '{file}' has been created.", "info")
else:
    connection = create_connection(file)
    if not check_table(connection, "weather"):
        log(f"Cache: could not find the 'weather' table. Trying to create the table.", "info")
        create_table(connection, sql_create_table_weather)

    if not check_table(connection, "device"):
        log(f"Cache: could not find the 'device' table. Trying to create the table.", "info")
        create_table(connection, sql_create_table_device)

    log(f"Cache: The cache is ready.", "info")




