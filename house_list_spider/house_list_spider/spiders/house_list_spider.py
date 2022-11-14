"""
scrapy crawl house_list -o output/house_links.csv --logfile log/log.txt
"""
import scrapy
import re
import logging
import sys

sys.path.append('../')

from utils import utils

HOUSE_LIST_START_URL_LIST = ['https://www.homes.co.jp/mansion/chuko/tokyo/list']


class HouseListSpider(scrapy.Spider):
    name = 'house_list'
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'

    def start_requests(self):
        for url in HOUSE_LIST_START_URL_LIST:
            yield scrapy.Request(url=url, callback=self.fanout_list_page)

    # Step 1. Parse the 1st house listing page
    def fanout_list_page(self, response):
        total_num_house = int(re.sub('[, ]', '', response.css('.totalNum::text').get()))
        num_pages = int(response.css('.lastPage>span::text').get())
        logging.info(f'Total {total_num_house} houses and {num_pages} pages found.')
        for page_index in range(num_pages):
            yield scrapy.Request(url=f'{response.url}?page={page_index + 1}', callback=self.parse_list_page)

    # # Step 2. Parse each house listing page and extract house_id
    def parse_list_page(self, response):
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
                    house_listing_price_list.append(utils.get_int_from_text(house_item.css('.priceLabel>span.num::text').get()))
            # Otherwise it only has one house_link
            else:
                if len(house.css('a.detailLink::attr("href")')) > 0:
                    house_link_list.append(house.css('a.detailLink::attr("href")').get())
                else:
                    logging.error(f'house_link can not be found for {listing_house_name}.')
                    return
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
                    return
                house_id = int(re.findall(r'\d+', house_id_parse[0])[0])

                yield {'house_id': house_id,
                       'is_pr_item': is_pr_item,
                       'listing_house_name': listing_house_name,
                       'listing_house_price': house_listing_price,
                       'sale_category': sale_category,
                       'city': city}

                num_house_on_the_page += 1

        logging.info(f'Finish crawling {request_url} with {num_house_on_the_page} house found.')
