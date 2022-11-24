from datetime import date
import json
import logging
import re
import sys

sys.path.append('../')

from utils import utils


class RentInfo:
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

        detailTopRent = response.css('.mod-detailTopRent')
        self.name = detailTopRent.css('.bukkenName::text').get()
        self.room = detailTopRent.css('.bukkenRoom::text').get()
        self.rent = utils.get_float_from_text(
            detailTopRent.css('.price').css('#chk-bkc-moneyroom>.num>span::text').get())
        self.manage_fee = utils.get_float_from_text(detailTopRent.css('.price').css('#chk-bkc-moneyroom::text').get())
        tmpl_l = self.safe_strip(detailTopRent.css('#chk-bkc-moneyshikirei::text').get(), default='').split('/')
        if len(tmpl_l) != 2:
            self.deposit_money_in_month = 0
            self.gift_money_in_month = 0
        else:
            self.deposit_money_in_month = utils.get_float_from_text(tmpl_l[0])
            self.gift_money_in_month = utils.get_float_from_text(tmpl_l[1])

        tmpl_l = self.safe_strip(detailTopRent.css('#chk-bkc-moneyhoshoukyaku::text').get(), default='').split('/')
        if len(tmpl_l) != 2:
            self.guarantee_money_in_month = 0
            self.shokyaku_money_in_month = 0
        else:
            self.guarantee_money_in_month = utils.get_float_from_text(tmpl_l[0])
            self.shokyaku_money_in_month = utils.get_float_from_text(tmpl_l[1])

        self.address = self.safe_strip(detailTopRent.css('#chk-bkc-fulladdress::text').get())

        valid_district = utils.get_district_from_name(self.address)
        if len(valid_district) == 0:
            logging.error(f'{house_id}: error parse district {self.address})')
            self.district = ''
        elif len(valid_district) > 1:
            logging.error(f'{house_id}: error parse district {self.address})')
            self.district = valid_district[0]
        else:
            self.district = valid_district[0]

        self.stations = []
        for traffic in detailTopRent.css('#chk-bkc-fulltraffic>p::text').getall():
            traffic = re.split(' +', traffic.strip())
            if len(traffic) != 3:
                continue
            if '徒歩' not in traffic[2]:
                continue
            self.stations.append((traffic[0], traffic[1], utils.get_int_from_text(traffic[2])))

        build_date_str = self.safe_strip(detailTopRent.css('#chk-bkc-kenchikudate::text').get(), default='')
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

        self.window_angle = self.safe_strip(detailTopRent.css('#chk-bkc-windowangle::text').get(),
                                            do_not_count_null=True)
        self.house_area = utils.get_float_from_text(
            self.safe_strip(detailTopRent.css('#chk-bkc-housearea::text').get()))
        self.balcony_area = utils.get_float_from_text(
            self.safe_strip(detailTopRent.css('#chk-bkc-balconyarea::text').get()))
        self.has_balcony = self.balcony_area > 0
        self.floor_plan = self.safe_strip(detailTopRent.css('#chk-bkc-marodi::text').get())

        bukkenSpecDetail = response.css('.mod-bukkenSpecDetail')
        self.other_fee_details = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-moneyother::text').get(),
                                                 do_not_count_null=True)
        other_fees = [] if self.other_fee_details is None else re.findall(r'\d*,?\d+円', self.other_fee_details)
        self.total_other_fee = sum(utils.get_float_from_text(x) for x in other_fees)
        self.structure = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-housekouzou::text').get())
        self.parking_lot = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-parking::text').get())
        self.unit_num = utils.get_int_from_text(bukkenSpecDetail.css('#chk-bkd-parkunit::text').get(),
                                                empty_str_to_none=True)
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
        self.rent_term = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-conterm::text').get())
        self.rent_refresh_fee = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-moneykoushin::text').get(),
                                                do_not_count_null=True)
        self.guarantee_company = self.safe_strip(''.join(bukkenSpecDetail.css('#chk-bkd-guaranteecom::text').getall()))
        self.insurance = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-insurance::text').get())
        self.current_status = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-genkyo::text').get())
        rent_start_date_str = self.safe_strip(
            bukkenSpecDetail.css('#chk-bkd-usable').css('div.spec::text').get(), default='')
        tmp_l = re.findall(r'\d+', ''.join(re.findall(r'\d+年\d+月', rent_start_date_str)))
        if len(tmp_l) != 2:
            self.rent_start_date = rent_start_date_str
        else:
            if '下旬' in rent_start_date_str:
                self.rent_start_date = date(int(tmp_l[0]), int(tmp_l[1]), 15).strftime("%Y-%m-%d")
            else:
                self.rent_start_date = date(int(tmp_l[0]), int(tmp_l[1]), 1).strftime("%Y-%m-%d")
        self.trade_method = self.safe_strip(bukkenSpecDetail.css('#chk-bkd-taiyou::text').get())
        register_date = bukkenSpecDetail.css('#chk-bkd-newdate::text').get()
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
            elif tr.css('th::text').get() == '入居条件':
                if tr.css('ul.normalEquipment').get() is None:
                    tmpl = tr.css('td::text').getall()
                else:
                    tmpl = tr.css('ul.normalEquipment').css('li::text').getall()
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

    def __str__(self):
        return json.dumps(self.__dict__, indent=2, ensure_ascii=False)
