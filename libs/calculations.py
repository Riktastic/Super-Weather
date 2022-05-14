import math

ms_to_mph = 2.2369362920544025
mm_to_inch = 0.0393700787
m_to_feet = 3.281
ms_to_kmh = 3.6
ms_to_knots = 1.943844

def celcius_to_fahrenheit(degrees):
    return 9.0/5.0 * degrees + 32

def fahrenheit_to_celcius(degrees):
    return (degrees - 32) * 5.0/9.0

def celcius_to_kelvin(degrees):
    return degrees + 273.15

bft_scale = (0.5, 1.5, 3.3, 5.5, 7.9, 10.7, 13.8, 17.1, 20.7, 24.4, 28.4, 32.6)
def windspeed_to_bft(windspeed):
    # windspeed in metres per second.
    # Used scale: https://en.wikipedia.org/wiki/Beaufort_scale
    for bft in range(len(bft_scale)):
        if windspeed < bft_scale[bft]:
            return bft

    return len(bft_scale)


def dew_point(temperature, humidity):
    # temperature in celcius.
    # Source of the formula: https://en.wikipedia.org/wiki/Dew_point
    a = 17.27
    b = 237/7
    temp = ((a * temperature) / (b + temperature)) + math.log(humidity/100.0)
    return (b * temp) / (a - temp)


def cloud_base(temperature, dew_point):
    # temperature and dew_point in celcius.
    spread = temperature - dew_point
    return spread * 126 + 4 # 4 is the station height of the device. $todo: Calculate using local cache.


def wind_chill(windspeed, temperature):
    # windspeed in m/s and temperature in celcius.
    windspeed_kmh = windspeed / ms_to_kmh
    return 13.12 + 0.6215*temperature -  11.37*math.pow(windspeed_kmh, 0.16) + 0.3965*temperature*math.pow(windspeed_kmh, 0.16)


def heat_index(temperature, humidity):
    # temperature in celcius.
    temperature_f = celcius_to_fahrenheit(temperature)

    c1 = -42.379
    c2 = 2.04901523
    c3 = 10.14333127
    c4 = -0.22475541
    c5 = -6.83783e-3
    c6 = -5.481717e-2
    c7 = 1.22874e-3
    c8 = 8.5282e-4
    c9 = -1.99e-6

    # try simplified formula first (used for HI < 80)
    hi = 0.5 * (temperature_f + 61. + (temperature_f - 68.) * 1.2 + humidity * 0.094)

    if hi >= 80:
        # use Rothfusz regression
        hi = math.fsum([
            c1,
            c2 * temperature_f,
            c3 * humidity,
            c4 * temperature_f * humidity,
            c5 * temperature_f ** 2,
            c6 * humidity ** 2,
            c7 * temperature_f ** 2 * humidity,
            c8 * temperature_f * humidity ** 2,
            c9 * temperature_f ** 2 * humidity**2,
        ])

    return fahrenheit_to_celcius(hi)

def feels_like(temperature, humidity, windspeed):
    # temperature in celcius.

    temperature_f = celcius_to_fahrenheit(temperature)

    if temperature_f <= 50 and windspeed > 3:
        # Wind Chill for low temp cases (and wind)
        fl = wind_chill(windspeed, temperature)
    elif temperature_f >= 80:
        # Heat Index for High temp cases
        fl = heat_index(temperature, humidity)
    else:
        fl = temperature_f

    return fahrenheit_to_celcius(fl)


def coordinate_degrees_to_cardinal(degrees):
    cardinal_coordinates = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    cardinal_coordinates_position = int((degrees + 11.25)/22.5)
    return cardinal_coordinates[cardinal_coordinates_position % 16]