from datetime import date


def get_date_str_today():
    return date.today().strftime("%Y-%m-%d")
