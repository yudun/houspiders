import scrapy
import pandas as pd
import logging


class HouseInfo:
    def __init__(self, response):
        self.is_in_pr_format = len(response.css('.mod-detailTopSale')) > 0


class HouseInfoSpider(scrapy.Spider):
    name = 'house_info'
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'

    def get_url_from_house_id(cls, house_id):
        return f'https://www.homes.co.jp/mansion/b-{house_id}/?iskks=1'

    def start_requests(self):
        df = pd.read_csv('/Users/yudun/Documents/houspiders/house_list_spider/output/house_links.csv')
        # df = pd.DataFrame({
        #     'house_id': [1455630000097],
        # })

        logging.info(f'Total {len(df)} houses will be scrawled.')
        for index, row in df.iterrows():
            yield scrapy.Request(url=self.get_url_from_house_id(row.house_id), callback=self.parse_house_info, cb_kwargs={'house_id': row.house_id})

    def parse_house_info(self, response, house_id):
        if len(response.css('.mod-detailTopSale')) != 1:
            logging.error(f'House is in wrong format: {response.url}')
            return

        with open(f'output/raw_html/{house_id}.html', 'wb') as html_file:
            html_file.write(response.body)
        logging.info(f'{response.url} has been saved locally.')
        # house_info = HouseInfo(response)
        # house_name = response.css('.break-words::text').get()
        # house_price = response.css('p.text-brand.text-xl.font-bold::text').get()
        #
        # addr1 = response.css('.flex.justify-between.flex-start::text').get()
        # addr2 = response.css('.text-base.text-left::text').get()
        # house_rough_address_text = addr1 if addr1 is not None else addr2
        #
        # station_list = [x.get() for x in response.css('.space-x-2.flex.items-center').css('.text-base.leading-7::text')]
        # print(house_name, house_price, house_rough_address_text, station_list, '\n')
