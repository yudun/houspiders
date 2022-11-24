"""
python3 ./main.py -i /home/ubuntu/houspiders/house_info_spider/output/raw_html --logfile log/2022-11-15-log.txt
"""
from os import listdir
from os.path import isfile, join, basename
import getopt
import logging
import sys
from scrapy import Selector

from house_info_processor.mansion_info import MansionInfo
from house_info_processor.rent_info import RentInfo

sys.path.append('../')

import db.utils as dbutil
from utils import utils
from utils import constant


def process_unavailable_house(house_id, category, cnx, cur):
    """
     Mark it as unavailable in `xxx_link` table and update the unavailable_date.
    """
    if category == constant.CHINTAI:
        row_count = dbutil.update_table(val_map={
            'is_available': 0,
            'unavailable_date': utils.get_date_str_today()
        },
            where_clause=f'house_id="{house_id}"',
            table_name='lifull_rent_link',
            cur=cur)
    else:
        row_count = dbutil.update_table(val_map={
            'is_available': 0,
            'unavailable_date': utils.get_date_str_today()
        },
            where_clause=f'house_id="{house_id}"',
            table_name='lifull_house_link',
            cur=cur)
    # Commit the changes
    cnx.commit()
    return row_count


def update_mansion_price_if_changed(house_id, house_price, cnx, cur):
    """
    Update price in `house_price_history` table if different from latest or no record.
    """
    cur.execute(
        f"SELECT price, price_date from lifull_house_price_history WHERE house_id={house_id} order by price_date asc;")
    all_rows = cur.fetchall()
    rowcount = 0
    # If current price is different from latest or no record.
    if len(all_rows) == 0 or int(all_rows[-1][0]) != house_price:
        insert_data = {
            'house_id': house_id,
            'price': house_price,
            'price_date': utils.get_date_str_today()
        }
        row_count = dbutil.insert_table(val_map=insert_data,
                                        table_name='lifull_house_price_history',
                                        cur=cur,
                                        on_duplicate_update_val_map=insert_data)
        if row_count <= 0:
            logging.error(f'house_id {house_id}: New price fails to update.')
        elif row_count == 1:
            logging.info(f'house_id {house_id}: New price is inserted.')
        elif row_count == 2:
            logging.info(f'house_id {house_id}: New price is updated.')
        # Commit the changes
        cnx.commit()
    return rowcount


def update_rent_price_if_changed(house_id, rent, manage_fee, cnx, cur):
    """
    Update price in `lifull_rent_price_history` table if different from latest or no record.
    """
    cur.execute(
        f'SELECT rent, manage_fee, price_date from lifull_rent_price_history WHERE '
        f'house_id="{house_id}" order by price_date asc;')
    all_rows = cur.fetchall()
    rowcount = 0
    # If current price is different from latest or no record.
    if len(all_rows) == 0 or int(all_rows[-1][0]) != rent or int(all_rows[-1][1]) != manage_fee:
        insert_data = {
            'house_id': house_id,
            'rent': rent,
            'manage_fee': manage_fee,
            'price_date': utils.get_date_str_today()
        }
        row_count = dbutil.insert_table(val_map=insert_data,
                                        table_name='lifull_rent_price_history',
                                        cur=cur,
                                        on_duplicate_update_val_map=insert_data)
        if row_count <= 0:
            logging.error(f'house_id {house_id}: New price fails to update.')
        elif row_count == 1:
            logging.info(f'house_id {house_id}: New price is inserted.')
        elif row_count == 2:
            logging.info(f'house_id {house_id}: New price is updated.')
        # Commit the changes
        cnx.commit()
    return rowcount


def update_house_info_table(house_info, category, cnx, cur):
    logging.debug(f'Full info for house_id {house_info.house_id}: {house_info}')
    if house_info.num_null_fields > 0:
        logging.error(f'house_id {house_info.house_id}: {house_info.num_null_fields} null fields in House Info.')

    insert_data = house_info.__dict__.copy()
    # We don't need this field in the table.
    del insert_data['num_null_fields']
    # We will store stations in another table.
    stations = insert_data['stations']
    del insert_data['stations']
    # We will store conditions in another table.
    conditions = insert_data['conditions']
    del insert_data['conditions']

    row_count = dbutil.insert_table(
        val_map=insert_data,
        table_name='lifull_rent_info' if category == constant.CHINTAI else 'lifull_house_info',
        cur=cur,
        on_duplicate_update_val_map=insert_data
    )
    cnx.commit()
    if row_count <= 0:
        logging.error(f'house_id {house_info.house_id}: House Info is not inserted.')
    elif row_count == 1:
        logging.info(f'house_id {house_info.house_id}: House Info is inserted.')
    elif row_count == 2:
        logging.info(f'house_id {house_info.house_id}: House Info is updated.')

    # Update stations info in lifull_stations_near_house table.
    num_inserted_station = 0
    for line, station, walk_min in stations:
        insert_data = {
            'house_id': house_info.house_id,
            'line_name': line,
            'station_name': station,
            'walk_distance_in_minute': walk_min,
            'category': category
        }
        row_count = dbutil.insert_table(
            val_map=insert_data,
            table_name='lifull_stations_near_house',
            cur=cur,
            on_duplicate_update_val_map=insert_data
        )
        cnx.commit()
        if row_count == 1:
            num_inserted_station += 1
    if num_inserted_station > 0:
        logging.info(f'house_id {house_info.house_id}: {num_inserted_station} stations are inserted.')

    # Update conditions info in lifull_house_condition table.
    num_inserted_condition = 0
    for condition in conditions:
        if condition == '':
            continue
        insert_data = {
            'house_id': house_info.house_id,
            'house_condition': condition,
            'category': category
        }
        row_count = dbutil.insert_table(
            val_map=insert_data,
            table_name='lifull_house_condition',
            cur=cur,
            on_duplicate_update_val_map=insert_data
        )
        cnx.commit()
        if row_count == 1:
            num_inserted_condition += 1
    if num_inserted_condition > 0:
        logging.info(f'house_id {house_info.house_id}: {num_inserted_condition} conditions are inserted.')


def process_mansion_info(house_id, response, cnx, cur):
    """
    1. Update its price in `house_price_history` table if different from latest or no record;
    2. Inject/merge new data into `house_info` table;
    """
    # Construct the MansionInfo struct
    house_info = MansionInfo(house_id, response)

    updated_row_count = update_mansion_price_if_changed(house_id, house_info.price, cnx, cur)
    if updated_row_count == 1:
        logging.info(f'Insert new price in lifull_house_price_history for {house_id}')
    elif updated_row_count == 2:
        logging.info(f'Update new price in lifull_house_price_history for {house_id}')

    update_house_info_table(house_info, constant.MANSION_CHUKO, cnx, cur)


def process_rent_info(house_id, response, cnx, cur):
    """
    1. Update its price in `house_price_history` table if different from latest or no record;
    2. Inject/merge new data into `house_info` table;
    """
    # Construct the RentInfo struct
    house_info = RentInfo(house_id, response)

    updated_row_count = update_rent_price_if_changed(house_id, house_info.rent, house_info.manage_fee, cnx, cur)
    if updated_row_count == 1:
        logging.info(f'Insert new price in lifull_rent_price_history for {house_id}')
    elif updated_row_count == 2:
        logging.info(f'Update new price in lifull_rent_price_history for {house_id}')

    update_house_info_table(house_info, constant.CHINTAI, cnx, cur)


if __name__ == "__main__":
    usage = 'main.py -i <parent_dir_path> --id <certain_house_id> --logfile <log_file> --loglevel <loglevel>'
    parent_dir_path = ''
    log_file = ''
    certain_house_id = ''
    loglevel = logging.INFO
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:l:", ["dir=", "logfile=", "id=", "loglevel="])
    except getopt.GetoptError:
        print(usage)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(usage)
            sys.exit()
        elif opt in ("-i", "--dir"):
            parent_dir_path = arg
        elif opt in ("--id"):
            certain_house_id = arg
        elif opt in ("--loglevel"):
            loglevel = utils.get_log_level_from_str(arg)
        elif opt in ("-l", "--logfile"):
            log_file = arg
    if parent_dir_path == '' or log_file == '':
        print(usage)
        sys.exit(2)
    print('Input parent dir path is', parent_dir_path)

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=loglevel,
                        filemode='w',
                        filename=log_file)

    # Connect to the database
    cnx = dbutil.get_mysql_cnx()
    cur = cnx.cursor(buffered=True)

    # If we only want to check a certain_house_id
    if certain_house_id != '':
        file_path = join(parent_dir_path, f'{certain_house_id}.html')
        with open(file_path, 'r') as f:
            logging.debug(f'process_mansion_info for {file_path}')
            process_mansion_info(int(certain_house_id), Selector(text=str(f.read())), cnx, cur)
        sys.exit()

    file_paths = [join(parent_dir_path, f) for f in listdir(parent_dir_path)
                  if isfile(join(parent_dir_path, f)) and f.endswith('.html')]
    print(f'{len(file_paths)} html files will be processed.')
    for file_path in file_paths:
        house_id = basename(file_path).replace('.html', '')
        with open(file_path, 'r') as f:
            logging.debug(f'process_mansion_info for {file_path}')
            process_mansion_info(house_id, Selector(text=str(f.read())), cnx, cur)
