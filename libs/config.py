from configparser import ConfigParser
file = "config.ini"

def parse(filename=file):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    sections = ["General", "Cache", "Database", "WeatherUnderground"]

    for section in sections:
        if not parser.has_section(section):
            raise Exception(f"Config: Cache: Section '{section}' not found in the '{filename}' file.")
            exit()

    return parser