from datetime import datetime
import pytz
import re
import logging
import numpy as np
import json


# This fixes https://stackoverflow.com/questions/50916422/python-typeerror-object-of-type-int64-is-not-json-serializable
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


def get_lifull_url_from_house_id(house_id):
    return f'https://www.homes.co.jp/mansion/b-{house_id}/?iskks=1'


def get_date_str_today():
    return datetime.now(tz=pytz.timezone('America/Los_Angeles')).strftime("%Y-%m-%d")


def get_int_from_text(item):
    if isinstance(item, str):
        parsed_str = ''.join(re.findall(r'\d+', item))
        if parsed_str == '':
            return 0
        return int(parsed_str)
    return None


def get_float_from_text(item):
    if isinstance(item, str):
        parsed_str = ''.join(re.findall(r'\d*[.\d+]', item))
        if parsed_str == '':
            return 0
        return float(parsed_str)
    return None


def get_log_level_from_str(log_level_str):
    if log_level_str == 'debug':
        return logging.DEBUG
    elif log_level_str == 'info':
        return logging.INFO
    elif log_level_str == 'error':
        return logging.ERROR
    elif log_level_str == 'fatal':
        return logging.FATAL
    else:
        return None
