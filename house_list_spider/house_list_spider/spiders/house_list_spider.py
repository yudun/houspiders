"""
scrapy crawl house_list -O output/2022-11-14/house_links.csv -a error_list_urls_path=output/2022-11-14/error_list_urls.csv \
-a category=chuko \
--logfile log/2022-11-14-log.txt

scrapy crawl house_list -O output/2022-11-14/house_chintai_links.csv -a error_list_urls_path=output/2022-11-14/error_chintai_list_urls.csv \
-a category=chintai \
--logfile log/2022-11-14-chintai-log.txt
"""
import scrapy
from scrapy import signals
import re
import logging
import sys
import os
import csv

sys.path.append('../')

from utils import utils
from utils import constant


class HouseListSpider(scrapy.Spider):
    name = 'house_list'
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'

    def __init__(self, error_list_urls_path, category, **kw):
        self.error_list_urls_path = error_list_urls_path
        self.category = category
        super(HouseListSpider, self).__init__(**kw)
        self.failed_pages_list = []

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(HouseListSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        if len(self.failed_pages_list) > 0:
            logging.error(f'These {len(self.failed_pages_list)} urls are not crawled: {self.failed_pages_list}')
            # Create the parent path if not exist
            os.makedirs(os.path.dirname(self.error_list_urls_path), exist_ok=True)

            # Write failed_pages_list to error_list_urls_path
            with open(self.error_list_urls_path, 'w+') as f:
                # using csv.writer method from CSV package
                write = csv.writer(f)
                write.writerow(['error_list_url'])
                write.writerows([[x] for x in self.failed_pages_list])
                logging.info(f'{len(self.failed_pages_list)} urls written to {self.error_list_urls_path}')

    def start_requests(self):
        if self.category == constant.CHUKO:
            yield scrapy.Request(url=f'https://www.homes.co.jp/mansion/{self.category}/tokyo/list', callback=self.fanout_list_page)
        if self.category == constant.CHINTAI:
            yield scrapy.Request(url=f'https://www.homes.co.jp/chintai/tokyo/list/', callback=self.fanout_list_page)

    # Step 1. Parse the 1st house listing page
    def fanout_list_page(self, response):
        total_num_house = utils.get_int_from_text(response.css('.totalNum::text').get())
        num_pages = utils.get_int_from_text(response.css('.lastPage>span::text').get())
        logging.info(f'Total {total_num_house} houses and {num_pages} pages found.')
        for page_index in range(num_pages):
            if self.category == constant.CHINTAI:
                # We are only interested in the 23 districts.
                chintai_form_data = {
                    **{f'cond[city][{idx}]': str(idx) for idx in range(13101, 13124)},
                    "cond[monthmoneyroom]": "0",
                    "cond[monthmoneyroomh]": "0",
                    "cond[housearea]": "0",
                    "cond[houseareah]": "0",
                    "cond[walkminutesh]": "0",
                    "cond[houseageh]": "0",
                    "bukken_attr[category]": "chintai",
                    "bukken_attr[pref]": "13",
                }
                yield scrapy.FormRequest(url=f'{response.url}?page={page_index + 1}',
                                         callback=self.parse_chintai_list_page,
                                         method="POST", formdata=chintai_form_data,
                                         errback=self.errback_httpbin)
            else:
                yield scrapy.Request(url=f'{response.url}?page={page_index + 1}', callback=self.parse_mansion_list_page,
                                     errback=self.errback_httpbin)

    # Step 2. Parse each house listing page and extract house_id
    def parse_mansion_list_page(self, response):
        request_url = response.url
        logging.info(f'Start crawling {request_url}')
        list_info = request_url.split('/')[3:6]
        sale_category = f'{list_info[0]}_{list_info[1]}'
        city = list_info[2]

        num_house_on_the_page = 0
        for house in response.css('.moduleInner'):
            is_pr_item = len(house.css('.icon:contains(PR)')) > 0
            listing_house_name = house.css('.bukkenName::text').get()
            if listing_house_name is None:
                logging.error(f'listing_house_name parse failure: {listing_house_name}')

            house_link_list = []
            house_listing_price_list = []

            house_item_list = house.css('.raSpecRow.checkSelect')
            # if this house has details list, it contains multiple house_link
            if len(house_item_list) > 0:
                for house_item in house_item_list:
                    house_link_list.append(house_item.css('.detail>a::attr("href")').get())
                    house_listing_price_list.append(
                        utils.get_int_from_text(house_item.css('.priceLabel>span.num::text').get()))
            # Otherwise it only has one house_link -- most likely a PR item
            else:
                if len(house.css('a.detailLink::attr("href")')) > 0:
                    house_link_list.append(house.css('a.detailLink::attr("href")').get())
                else:
                    logging.error(f'house_link can not be found for {listing_house_name}.')
                    continue
                if len(house.css('.price>span.num::text')) > 0:
                    house_listing_price_list.append(
                        utils.get_int_from_text(house.css('.price>span.num::text').get()))
                else:
                    house_listing_price_list.append(None)
                    logging.error(f'house_price can not be found for {listing_house_name}.')

            for idx in range(len(house_link_list)):
                house_link = house_link_list[idx]
                house_listing_price = house_listing_price_list[idx]

                # Parse house_id from house_link
                house_id_parse = re.findall(r'/b-\d+/', house_link)
                if len(house_id_parse) != 1 and len(re.findall(r'\d+', house_id_parse[0])) != 1:
                    logging.error(f'house_id parse failure: {house_link}')
                    continue
                house_id = int(re.findall(r'\d+', house_id_parse[0])[0])

                yield {'house_id': house_id,
                       'is_pr_item': is_pr_item,
                       'listing_house_name': listing_house_name,
                       'listing_house_price': house_listing_price,
                       'sale_category': sale_category,
                       'city': city}

                num_house_on_the_page += 1

        logging.info(f'Finish crawling {request_url} with {num_house_on_the_page} house found.')

    # Step 2. Parse each rent listing page and extract house_id
    def parse_chintai_list_page(self, response):
        request_url = response.url
        logging.info(f'Start crawling {request_url}')

        num_house_on_the_page = 0
        for house in response.css('.moduleInner'):
            is_pr_item = len(house.css('.icon:contains(PR)')) > 0
            listing_house_name = house.css('.bukkenName::text').get()
            if listing_house_name is None:
                logging.error(f'listing_house_name parse failure: {listing_house_name}')

            house_link_list = []
            listing_house_rent_list = []
            listing_house_manage_fee_list = []

            house_item_list = house.css('.prg-room.prg-building.checkSelect')
            # if this house has details list, it contains multiple house_link
            if len(house_item_list) > 0:
                for house_item in house_item_list:
                    house_link_list.append(house_item.css('.detail>a::attr("href")').get())
                    listing_house_rent_list.append(
                        utils.get_int_from_text(house_item.css('.priceLabel>span.num::text').get()))
                    tmp_l = house_item.css('.price::text').getall()
                    if len(tmp_l) != 2:
                        logging.error(f'Manage fee error format for {listing_house_name}.')
                    listing_house_manage_fee_list.append(utils.get_int_from_text(tmp_l[0]))
            # Otherwise it only has one house_link -- most likely a PR item
            else:
                if len(house.css('a.detailLink::attr("href")')) > 0:
                    house_link_list.append(house.css('a.detailLink::attr("href")').get())
                else:
                    logging.error(f'house_link can not be found for {listing_house_name}.')
                    continue
                if len(house.css('.price>span.num::text')) > 0 and len(house.css('td.price::text')) > 0:
                    listing_house_rent_list.append(
                        utils.get_int_from_text(house.css('.price>span.num::text').get()))
                    listing_house_manage_fee_list.append(
                        utils.get_int_from_text(house.css('td.price::text').get()))
                else:
                    listing_house_rent_list.append(None)
                    listing_house_manage_fee_list.append(None)
                    logging.error(f'rent_price or manage fee not found for {listing_house_name}.')

            for idx in range(len(house_link_list)):
                house_link = house_link_list[idx]
                listing_house_rent = listing_house_rent_list[idx]
                listing_house_manage_fee = listing_house_manage_fee_list[idx]

                if is_pr_item:
                    house_id_parse = re.findall(r'/b-\d+/', house_link)
                    if len(house_id_parse) != 1 and len(re.findall(r'\d+', house_id_parse[0])) != 1:
                        logging.error(f'PR house_id parse failure: {house_link}')
                        continue
                    house_id = re.findall(r'\d+', house_id_parse[0])[0]
                else:
                    # Parse house_id from house_link
                    house_id_parse = re.findall(r'room/[0-9a-z]+', house_link)
                    if len(house_id_parse) != 1 or len(house_id_parse[0].split('/')) != 2:
                        logging.error(f'house_id parse failure: {house_link}')
                        continue
                    house_id = house_id_parse[0].split('/')[1]

                yield {'house_id': house_id,
                       'is_pr_item': is_pr_item,
                       'listing_house_name': listing_house_name,
                       'listing_house_rent': listing_house_rent,
                       'listing_house_manage_fee': listing_house_manage_fee,
                       'city': 'tokyo'}

                num_house_on_the_page += 1

        logging.info(f'Finish crawling {request_url} with {num_house_on_the_page} house found.')

    def errback_httpbin(self, failure):
        # log all failures
        logging.error(repr(failure))
        self.failed_pages_list.append(failure.request.url)
