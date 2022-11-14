from datetime import date
import re
import logging


def get_lifull_url_from_house_id(house_id):
    return f'https://www.homes.co.jp/mansion/b-{house_id}/?iskks=1'


def get_date_str_today():
    return date.today().strftime("%Y-%m-%d")


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
