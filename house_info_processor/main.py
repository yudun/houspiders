from os import listdir
from os.path import isfile, join, basename
from datetime import date
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
    def __init__(self, house_id, response):
        self.house_id = house_id
        self.name = response.css('.mod-buildingName').css('.bukkenName::text').get()

        valid_district = get_district_from_name(self.name)
        if len(valid_district) == 0:
            logging.error(f'{house_id}: error parse district {self.name})')
            self.district = ''
        elif len(valid_district) > 1:
            logging.error(f'{house_id}: error parse district {self.name})')
            self.district = valid_district[0]
        else:
            self.district = valid_district[0]

        self.room = response.css('.mod-buildingName').css('.bukkenRoom::text').get()

        detailTopSale = response.css('.mod-detailTopSale')
        self.price = utils.get_int_from_text(detailTopSale.css('#chk-bkc-moneyroom::text').get())
        self.address = detailTopSale.css('#chk-bkc-fulladdress::text').get().strip()
        self.moneykyoueki = utils.get_int_from_text(
            detailTopSale.css('#chk-bkc-moneykyoueki::text').get().strip())
        self.moneyshuuzen = utils.get_int_from_text(
            detailTopSale.css('#chk-bkc-moneyshuuzen::text').get().strip())

        self.stations = []
        for traffic in detailTopSale.css('#chk-bkc-fulltraffic').css('.traffic::text').getall():
            traffic = re.split(' +', traffic.strip())
            if len(traffic) != 3:
                continue
            if '徒歩' not in traffic[2]:
                continue
            self.stations.append((traffic[0], traffic[1], utils.get_int_from_text(traffic[2])))

        build_date_str = detailTopSale.css('#chk-bkc-kenchikudate::text').get().strip()
        l = re.findall(r'\d+', ''.join(re.findall(r'\d+年\d+月', build_date_str)))
        if len(l) != 2:
            logging.error(f'{house_id}: error parse build_date {build_date_str})')
            self.build_date = 'null'
        else:
            self.build_date = date(int(l[0]), int(l[1]), 1).strftime("%Y-%m-%d")
        self.age = utils.get_int_from_text(''.join(re.findall(r'築\d+年', build_date_str)))

        self.window_angle = detailTopSale.css('#chk-bkc-windowangle::text').get().strip()
        self.house_area = utils.get_float_from_text(detailTopSale.css('#chk-bkc-housearea::text').get().strip())
        self.balcony_area = utils.get_float_from_text(detailTopSale.css('#chk-bkc-balconyarea::text').get().strip())
        self.floor_plan = utils.get_float_from_text(detailTopSale.css('#chk-bkc-marodi::text').get().strip())
        self.feature_comment = utils.get_float_from_text(
            detailTopSale.css('#chk-bkp-featurecomment::text').get().strip())
        self.register_date = detailTopSale.css('#chk-bkh-newdate::text').get().strip().replace('/', '-')

        bukkenNotes = response.css('.mod-bukkenNotes')
        equipments = bukkenNotes.css('#chk-bkf-setsubi3>ul.normalEquipment').css('li::text').getall()
        equipments = [re.sub('\n.*', '', x.strip()) for x in equipments]
        self.has_elevator = 'エレベーター' in equipments
        self.has_balcony = self.balcony_area > 0
        self.note = ''.join(bukkenNotes.css('#chk-bkf-biko::text').getall()).strip()
        self.has_special_note = '告知事項' in self.note

        bukkenSpecDetail = response.css('.mod-bukkenSpecDetail')
        self.unit_num = utils.get_int_from_text(bukkenSpecDetail.css('#chk-bkd-allunit::text').get())
        num_floor_infos = bukkenSpecDetail.css('#chk-bkd-housekai::text').get().split('/')
        self.floor_num = utils.get_int_from_text(num_floor_infos[0])
        self.num_total_floor = utils.get_int_from_text(num_floor_infos[1])
        self.structure = bukkenSpecDetail.css('#chk-bkd-housekouzou::text').get().strip()
        self.land_usage = bukkenSpecDetail.css('#chk-bkd-landyouto::text').get().strip()
        self.land_position = bukkenSpecDetail.css('#chk-bkd-landchisei::text').get().strip()
        self.land_right = bukkenSpecDetail.css('#chk-bkd-landright::text').get().strip()
        land_moneyshakuchi = bukkenSpecDetail.css('#chk-bkd-moneyshakuchi::text').get()
        self.land_moneyshakuchi = 'null' if land_moneyshakuchi is None else utils.get_int_from_text(
            land_moneyshakuchi.strip())
        land_term = bukkenSpecDetail.css('#id="chk-bkd-conterm"::text').get()
        self.land_term = 'null' if land_term is None else land_term.strip()
        self.land_landkokudoho = bukkenSpecDetail.css('#chk-bkd-landkokudoho::text').get().strip()

        self.other_fee_details = bukkenSpecDetail.css('#chk-bkd-moneyother::text').get().strip()
        other_fees = self.other_fee_details.split('円')
        self.total_other_fee = sum(utils.get_int_from_text(x) for x in other_fees)

        self.manage_details = bukkenSpecDetail.css('#chk-bkd-management::text').get().strip()
        self.latest_rent_status = bukkenSpecDetail.css('#chk-bkd-genkyo').css('.genkyoText::text').get().strip()
        self.trade_method = bukkenSpecDetail.css('#chk-bkd-taiyou::text').get().strip()

    def __str__(self):
        return self.__dict__


def process_unavailable_house(house_id, cnx, cur):
    """
     Mark it as unavailable in `house_link` table and update the unavailable_date.
    """
    rowcount = dbutil.update_table(val_map={
        'is_available': 0,
        'unavailable_date': f"'{utils.get_date_str_today()}'"
    },
        where_clause=f'house_id={house_id}',
        table_name='lifull_house_link',
        cur=cur)
    # Commit the changes
    cnx.commit()
    return rowcount


def update_house_price_if_changed(house_id, house_price, cnx, cur):
    """
    Update price in `house_price_history` table if different from latest or no record.
    """
    cur.execute(
        f"SELECT price, price_date from lifull_house_price_history WHERE house_id={house_id} order by price_date asc;")
    all_rows = cur.fetchall()
    rowcount = 0
    # If current price is different from latest or no record.
    if len(all_rows) == 0 or all_rows[-1][0] != house_price:
        rowcount = dbutil.insert_table(val_map={
            'house_id': house_id,
            'price': house_price,
            'price_date': f"'{utils.get_date_str_today()}'"
        },
            table_name='lifull_house_price_history',
            cur=cur)
        # Commit the changes
        cnx.commit()
    return rowcount


def update_house_info_table(house_info, cnx, cur):
    print(house_info)
    pass


def process_house_info(house_id, response, cnx, cur):
    """
    1. Update its price in `house_price_history` table if different from latest or no record;
    2. Inject/merge new data into `house_info` table;
    """
    # Construct the HouseInfo struct
    house_info = HouseInfo(house_id, response)

    update_house_price_if_changed(house_id, house_info.price, cnx, cur)

    update_house_info_table(house_info, cnx, cur)


if __name__ == "__main__":
    parent_dir_path = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:", ["dir="])
    except getopt.GetoptError:
        print('main.py -i <parent_dir_path>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('main.py -i <parent_dir_path>')
            sys.exit()
        elif opt in ("-i", "--dir"):
            parent_dir_path = arg
    print('Input parent dir path is', parent_dir_path)

    # Connect to the database
    cnx = dbutil.get_mysql_cnx()
    cur = cnx.cursor(buffered=True)

    file_paths = [join(parent_dir_path, f) for f in listdir(parent_dir_path)
                  if isfile(join(parent_dir_path, f)) and f.endswith('.html')]
    for file_path in file_paths[:1]:
        house_id = basename(file_path).replace('.html', '')
        with open(file_path, 'r') as f:
            process_house_info(house_id, Selector(text=str(f.read())), cnx, cur)
