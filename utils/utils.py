from datetime import date
import re


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