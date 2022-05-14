import requests
from datetime import datetime
from inspect import stack
from libs.config import parse


def log(message, category):
    log_config = parse()
    caller = stack()[1].function

    # Get the current timestamp.
    timestamp = datetime.now()
    timestamp_iso8601 = timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f%z')

    # Add a "icon" that quickly accustoms the user to a logcategory, using a switch-case style solution.
    category_dict = {
        'warning': '[!?]',
        'error': '[!]',
        'info': '[i]',
        'debug': '[D]',
        'data': '[-]',
        'report': '[R]'
    }
    category_icon = category_dict.get(category, category)

    # Log to ntfy.sh
    if (category == 'error' or category == 'report') and log_config.getboolean('Ntfy.sh','Enabled') == True:
        result = requests.post(f"{log_config['Ntfy.sh']['Url']}",
        data=f"{timestamp} {category_icon} '{caller}': {message}",
            headers={
                "Authorization": f"Basic {log_config['Ntfy.sh']['Key']}"
            }
        )
        if result.status_code != 200:
            raise Exception(f"Ntfy.sh didn't acknowledge the success of the update. Response of the server:\n - Status-code: {result.status_code}\n - Content: {result.content}") 
        
    # Log to file:
    logfile_appender = open(log_config['Log']['Logfile'], "a")
    logfile_appender.write(f"{timestamp_iso8601} {category_icon} '{caller}': {message}\n" )

    # Log to screen:
    print(f"{timestamp} {category_icon} '{caller}': {message}" )