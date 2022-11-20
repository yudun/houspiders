"""
python3 ./main.py -i /home/ubuntu/houspiders/house_info_spider/output/raw_html --logfile log/2022-11-15-log.txt
"""
from os import listdir
from os.path import isfile, join, basename
from datetime import date
import json
import getopt
import logging
import re
import sys
from scrapy import Selector

sys.path.append('../')

import db.utils as dbutil
from utils import utils
from utils import constant


def get_district_from_name(name):
    valid_district = []
    for district in constant.TOKYO_DISTRICTS:
        if district in name:
            valid_district.append(district)
    return valid_district


class HouseInfo:
    def safe_strip(self, item, default=None, do_not_count_null=False):
        if isinstance(item, str):
            return item.strip()

        if not do_not_count_null:
            self.num_null_fields += 1

        return default

    def __init__(self, house_id, response):
        # Stats check how many fields are set as null unexpectedly.
        self.num_null_fields = 0

        self.house_id = house_id
        self.name = response.css('.mod-buildingName').css('.bukkenName::text').get()
        self.room = response.css('.mod-buildingName').css('.bukkenRoom::text').get()

        detailTopSale = response.css('.mod-detailTopSale')
        self.price = utils.get_int_from_text(detailTopSale.css('#chk-bkc-moneyroom::text').get())
        self.address = self.safe_strip(detailTopSale.css('#chk-bkc-fulladdress::text').get())

        valid_district = get_district_from_name(self.address)
        if len(valid_district) == 0:
            logging.error(f'{house_id}: error parse district {self.address})')
            self.district = ''
        elif len(valid_district) > 1:
            logging.error(f'{house_id}: error parse district {self.address})')
            self.district = valid_district[0]
        else:
            self.district = valid_district[0]

        self.moneykyoueki = utils.get_int_from_text(
            self.safe_strip(detailTopSale.css('#chk-bkc-moneykyoueki::text').get()))
        self.moneyshuuzen = utils.get_int_from_text(
            self.safe_strip(detailTopSale.css('#chk-bkc-moneyshuuzen::text').get()))

        self.stations = []
        for traffic in detailTopSale.css('#chk-bkc-fulltraffic').css('.traffic::text').getall():
            traffic = re.split(' +', traffic.strip())
            if len(traffic) != 3:
                continue
            if '徒歩' not in traffic[2]:
                continue
            self.stations.append((traffic[0], traffic[1], utils.get_int_from_text(traffic[2])))

        build_date_str = self.safe_strip(detailTopSale.css('#chk-bkc-kenchikudate::text').get(), default='')
        tmp_l = re.findall(r'\d+', ''.join(re.findall(r'\d+年\d+月', build_date_str)))
        if len(tmp_l) != 2:
            self.num_null_fields += 1
            logging.error(f'{house_id}: error parse build_date {build_date_str})')
            self.build_date = None
        else:
            self.build_date = date(int(tmp_l[0]), int(tmp_l[1]), 1).strftime("%Y-%m-%d")
        tmp_l = re.findall(r'築\d+年', build_date_str)
        if len(tmp_l) != 1:
            if len(re.findall(r'新築', build_date_str)) > 0:
                self.age = 0
            else:
                self.num_null_fields += 1
                logging.error(f'{house_id}: error parse build_age {build_date_str})')
                self.age = None
        else:
            self.age = utils.get_int_from_text(tmp_l[0])

        self.window_angle = self.safe_strip(detailTopSale.css('#chk-bkc-windowangle::text').get(),
                                            do_not_count_null=True)
        self.house_area = utils.get_float_from_text(
            self.safe_strip(detailTopSale.css('#chk-bkc-housearea::text').get()))
        self.balcony_area = utils.get_float_from_text(
            self.safe_strip(detailTopSale.css('#chk-bkc-balconyarea::text').get()))
        self.has_balcony = self.balcony_area > 0
        self.floor_plan = self.safe_strip(detailTopSale.css('#chk-bkc-marodi::text').get())
        self.feature_comment = self.safe_strip(detailTopSale.css('#chk-bkp-featurecomment::text').get(),
                                               do_not_count_null=True)
        register_date = detailTopSale.css('#chk-bkh-newdate::text').get()
        self.register_date = None if register_date is None else register_date.replace('/', '-')

        bukkenNotes = response.css('.mod-bukkenNotes')
        self.has_elevator = False
        self.note = None
        self.has_special_note = False
        self.conditions = []

        for tr in bukkenNotes.css('tr'):
            if tr.css('th::text').get() == '設備・サービス':
                equipments = tr.css('ul.normalEquipment').css('li::text').getall()
                equipments = [re.sub('\n.*', '', x.strip()) for x in equipments]
                self.has_elevator = 'エレベーター' in equipments
            elif tr.css('th::text').get() == 'この物件のこだわり':
                tmpl = tr.css('ul.bukkenEquipment').css('li.active').css('span::text').getall()
                self.conditions += [re.sub('\n.*', '', x.strip()) for x in tmpl]
            elif tr.css('th::text').get() == '物件の状況':
                if tr.css('ul.normalEquipment').get() is None:
                    tmpl = tr.css('td::text').getall()
                else:
                    tmpl = tr.css('ul.normalEquipment').css('li::text').getall()
                self.conditions += [re.sub('\n.*', '', x.strip()) for x in tmpl]
            elif tr.css('th::text').get() == '保険・保証':
                if tr.css('ul.normalEquipment').get() is None:
                    tmpl = tr.css('td::text').getall()
                else:
                    tmpl = tr.css('ul.normalEquipment').css('li::text').getall()
                self.conditions += [re.sub('\n.*', '', x.strip()) for x in tmpl]
            elif tr.css('th::text').get() == '備考':
                self.note = ''.join(tr.css('td#chk-bkf-biko::text').getall()).strip()
                self.has_special_note = '告知事項' in self.note
            elif tr.css('th::text').get() == 'その他':
                if tr.css('ul.normalEquipment').get() is None:
                    tmpl = tr.css('td::text').getall()
                else:
                    tmpl = tr.css('ul.normalEquipment').css('li::text').getall()
                self.conditions += [re.sub('\n.*', '', x.strip()) for x in tmpl]
        self.conditions.remove('')

        bukkenSpecDetail = response.css('.mod-bukkenSpecDetail')
        self.unit_num = utils.get_int_from_text(bukkenSpecDetail.css('#chk-bkd-allunit::text').get())

        num_floor_infos = bukkenSpecDetail.css('#chk-bkd-housekai::text').get()
        self.floor_num = None
        self.num_total_floor = None
        if num_floor_infos is None:
            self.num_null_fields += 2
        else:
            floor_num_l = re.findall(r'\d+階 /', num_floor_infos)
            num_total_floor_l = re.findall(r'/ \d+階建', num_floor_infos)
            if len(floor_num_l) != 1:
                self.num_null_fields += 1
            else:
                self.floor_num = utils.get_int_from_text(floor_num_l[0], empty_str_to_none=True)

            if len(num_total_floor_l) != 1:
                self.num_null_fields += 1
            else:
                self.num_total_floor = utils.get_int_from_text(num_total_floor_l[0], empty_str_to_none=True)

        self.structure = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-housekouzou::text').get())
        self.land_usage = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-landyouto::text').get(),
                                          do_not_count_null=True)
        self.land_position = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-landchisei::text').get(),
                                             do_not_count_null=True)
        self.land_right = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-landright::text').get())
        land_moneyshakuchi = bukkenSpecDetail.css('#chk-bkd-moneyshakuchi::text').get()
        self.land_moneyshakuchi = None if land_moneyshakuchi is None else utils.get_int_from_text(
            land_moneyshakuchi.strip())
        land_term = bukkenSpecDetail.css('#chk-bkd-conterm::text').get()
        self.land_term = None if land_term is None else land_term.strip()
        self.land_landkokudoho = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-landkokudoho::text').get())

        self.other_fee_details = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-moneyother::text').get(),
                                                 do_not_count_null=True)

        other_fees = [] if self.other_fee_details is None else re.findall(r'\d*,?\d+円', self.other_fee_details)
        self.total_other_fee = sum(utils.get_int_from_text(x) for x in other_fees)

        self.manage_details = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-management::text').get())
        self.latest_rent_status = self.safe_strip(
            bukkenSpecDetail.css('#chk-bkd-genkyo').css('.genkyoText::text').get())
        self.trade_method = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-taiyou::text').get())

    def __str__(self):
        return json.dumps(self.__dict__, indent=2, ensure_ascii=False)


def process_unavailable_house(house_id, cnx, cur):
    """
     Mark it as unavailable in `house_link` table and update the unavailable_date.
    """
    row_count = dbutil.update_table(val_map={
        'is_available': 0,
        'unavailable_date': utils.get_date_str_today()
    },
        where_clause=f'house_id={house_id}',
        table_name='lifull_house_link',
        cur=cur)
    # Commit the changes
    cnx.commit()
    return row_count


def update_house_price_if_changed(house_id, house_price, cnx, cur):
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


def update_house_info_table(house_info, cnx, cur):
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
        table_name='lifull_house_info',
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
            'walk_distance_in_minute': walk_min
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

    # Update stations info in lifull_house_condition table.
    num_inserted_condition = 0
    for condition in conditions:
        insert_data = {
            'house_id': house_info.house_id,
            'house_condition': condition,
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


def process_house_info(house_id, response, cnx, cur):
    """
    1. Update its price in `house_price_history` table if different from latest or no record;
    2. Inject/merge new data into `house_info` table;
    """
    # Construct the HouseInfo struct
    house_info = HouseInfo(house_id, response)

    updated_row_count = update_house_price_if_changed(house_id, house_info.price, cnx, cur)
    if updated_row_count > 0:
        logging.info(f'Update {updated_row_count} in lifull_house_price_history for {house_id}')

    update_house_info_table(house_info, cnx, cur)


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
            logging.debug(f'process_house_info for {file_path}')
            process_house_info(int(certain_house_id), Selector(text=str(f.read())), cnx, cur)
        sys.exit()

    file_paths = [join(parent_dir_path, f) for f in listdir(parent_dir_path)
                  if isfile(join(parent_dir_path, f)) and f.endswith('.html')]
    for file_path in file_paths:
        house_id = basename(file_path).replace('.html', '')
        with open(file_path, 'r') as f:
            logging.debug(f'process_house_info for {file_path}')
            process_house_info(house_id, Selector(text=str(f.read())), cnx, cur)
