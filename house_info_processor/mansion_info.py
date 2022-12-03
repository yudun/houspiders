from datetime import date
import json
import logging
import re
import sys

sys.path.append('../')

from utils import utils


class MansionInfo:
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
        self.price = utils.get_float_from_text(detailTopSale.css('#chk-bkc-moneyroom::text').get())
        if self.price == 0:
            # If the price is not available it is possible in inner span
            self.price = utils.get_float_from_text(detailTopSale.css('#chk-bkc-moneyroom').css('.num>span::text').get())
        self.address = self.safe_strip(detailTopSale.css('#chk-bkc-fulladdress::text').get())

        valid_district = utils.get_district_from_name(self.address)
        if len(valid_district) == 0:
            logging.error(f'{house_id}: error parse district {self.address})')
            self.district = ''
        elif len(valid_district) > 1:
            logging.error(f'{house_id}: error parse district {self.address})')
            self.district = valid_district[0]
        else:
            self.district = valid_district[0]

        self.moneykyoueki = utils.get_float_from_text(
            self.safe_strip(detailTopSale.css('#chk-bkc-moneykyoueki::text').get()))
        self.moneyshuuzen = utils.get_float_from_text(
            self.safe_strip(detailTopSale.css('#chk-bkc-moneyshuuzen::text').get()))

        self.stations = []
        station_text_list = detailTopSale.css('#chk-bkc-fulltraffic').css('.traffic::text').getall()
        if len(station_text_list) == 0:
            station_text_list = detailTopSale.css('#chk-bkc-fulltraffic>p::text').getall()
        for traffic in station_text_list:
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
            self.safe_strip(detailTopSale.css('#chk-bkc-balconyarea::text').get(), default='0', do_not_count_null=True))
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
                self.has_elevator = self.has_elevator or 'エレベーター' in equipments
            if tr.css('th::text').get() == '設備・条件':
                equipments = tr.css('ul.normalEquipment').css('li::text').getall()
                equipments = [re.sub('\n.*', '', x.strip()) for x in equipments]
                self.has_elevator = self.has_elevator or 'エレベーター' in equipments
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

        bukkenSpecDetail = response.css('.mod-bukkenSpecDetail')
        self.cash_on_cash_roi_percentage = utils.get_float_from_text(
                self.safe_strip(bukkenSpecDetail.css('#chk-bkd-moneyrimawari::text').get(),
                                do_not_count_null=True))

        # Fallback house_area to new entries in bukkenSpecDetail
        if self.house_area is None or self.house_area == 0:
            self.house_area = utils.get_float_from_text(
                self.safe_strip(bukkenSpecDetail.css('#chk-bkd-houseareaminmax::text').get(), default='0',
                                do_not_count_null=True).split('～')[0])
        if self.house_area is None or self.house_area == 0:
            self.house_area = utils.get_float_from_text(
                self.safe_strip(bukkenSpecDetail.css('#chk-bkd-housearea::text').get(), default='0',
                                do_not_count_null=True))

        self.land_area = utils.get_float_from_text(
                self.safe_strip(bukkenSpecDetail.css('#chk-bkd-landarea::text').get(), do_not_count_null=True))

        # Fallback balcony_area to new entries in bukkenSpecDetail
        if self.balcony_area == 0:
            self.balcony_area = utils.get_float_from_text(
                self.safe_strip(bukkenSpecDetail.css('#chk-bkd-balconyarea::text').get(), default='0',
                                do_not_count_null=True))
            self.has_balcony = self.balcony_area > 0

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

        if bukkenSpecDetail.css('#chk-bkd-housekouzou::text').get() is not None:
            self.structure = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-housekouzou::text').get())
        else:
            self.structure = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-kouzou::text').get())
        self.land_usage = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-landyouto::text').get(),
                                          do_not_count_null=True)
        self.land_position = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-landchisei::text').get(),
                                             do_not_count_null=True)
        self.land_right = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-landright::text').get())
        land_moneyshakuchi = bukkenSpecDetail.css('#chk-bkd-moneyshakuchi::text').get()
        self.land_moneyshakuchi = None if land_moneyshakuchi is None else utils.get_float_from_text(
            land_moneyshakuchi.strip())
        land_term = bukkenSpecDetail.css('#chk-bkd-conterm::text').get()
        self.land_term = None if land_term is None else land_term.strip()
        self.land_landkokudoho = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-landkokudoho::text').get())

        self.other_fee_details = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-moneyother::text').get(),
                                                 do_not_count_null=True)

        other_fees = [] if self.other_fee_details is None else re.findall(r'\d*,?\d+円', self.other_fee_details)
        self.total_other_fee = sum(utils.get_float_from_text(x) for x in other_fees)

        self.manage_details = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-management::text').get())
        self.latest_rent_status = self.safe_strip(
            bukkenSpecDetail.css('#chk-bkd-genkyo').css('.genkyoText::text').get())
        self.trade_method = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-taiyou::text').get())

    def __str__(self):
        return json.dumps(self.__dict__, indent=2, ensure_ascii=False)