from datetime import datetime, timedelta
import pytz
import re
import logging
from utils import constant


def get_lifull_mansion_url_from_house_id(house_id):
    return f'https://www.homes.co.jp/mansion/b-{house_id}/?iskks=1'


def get_lifull_chintai_url_from_house_id(house_id):
    if house_id.isnumeric():
        return f'https://www.homes.co.jp/chintai/b-{house_id}/?iskks=1'

    return f'https://www.homes.co.jp/chintai/room/{house_id}'


def get_date_str_today():
    return datetime.now(tz=pytz.timezone('America/Los_Angeles')).strftime("%Y-%m-%d")


def get_date_str_yesterday():
    return (datetime.now(tz=pytz.timezone('America/Los_Angeles')) - timedelta(days=1)).strftime("%Y-%m-%d")


def get_int_from_text(item, empty_str_to_none=False):
    if isinstance(item, str):
        parsed_str = ''.join(re.findall(r'\d+', item))
        if parsed_str == '':
            if empty_str_to_none:
                return None
            else:
                return 0
        return int(parsed_str)
    return None


def get_float_from_text(item, empty_str_to_none=False):
    if isinstance(item, str):
        parsed_str = ''.join(re.findall(r'\d*[.\d+]', item))
        if parsed_str == '':
            if empty_str_to_none:
                return None
            else:
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


def get_district_from_name(name):
    valid_district = []
    for district in constant.TOKYO_DISTRICTS:
        if district in name:
            valid_district.append(district)
    return valid_district
