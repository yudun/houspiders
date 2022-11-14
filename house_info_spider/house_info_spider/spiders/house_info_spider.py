"""
scrapy crawl house_info -a i=/home/ubuntu/houspiders/house_list_processor/output/house_id_to_crawl.csv --logfile log/log.txt
"""
import scrapy
from scrapy import signals
import pandas as pd
import logging
import sys

sys.path.append('../')

from house_info_processor.main import process_house_info, process_unavailable_house
import db.utils as dbutil
from utils import utils


class HouseInfoSpider(scrapy.Spider):
    name = 'house_info'
    # Allow handling 404 requests
    handle_httpstatus_list = [404]
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'

    def __init__(self, i, **kw):
        self.house_link_file_path = i
        super(HouseInfoSpider, self).__init__(**kw)
        # Init database connection
        self.cnx = None
        self.cur = None

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(HouseInfoSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        # Connect to the database
        self.cnx = dbutil.get_mysql_cnx()
        self.cur = self.cnx.cursor(buffered=True)

    def spider_closed(self, spider):
        self.cnx.close()

    def start_requests(self):
        # df = pd.read_csv(self.house_link_file_path)
        df = pd.DataFrame({
            'house_id': [1234],
        })

        logging.info(f'Total {len(df)} houses will be scrawled.')
        for index, row in df.iterrows():
            yield scrapy.Request(url=utils.get_lifull_url_from_house_id(row.house_id), callback=self.parse_house_info, cb_kwargs={'house_id': row.house_id})

    def parse_house_info(self, response, house_id):
        if response.status == 404 or response.css('.mod-expiredInformation').get() is not None:
            process_unavailable_house(house_id, self.cnx, self.cur)
            return
        elif len(response.css('.mod-detailTopSale')) != 1:
            logging.error(f'House is in wrong format: {response.url}')
            return

        with open(f'output/raw_html/{house_id}.html', 'wb') as html_file:
            html_file.write(response.body)
        logging.info(f'{response.url} has been saved locally.')

        # Mark it as available in `house_link` table;
        dbutil.update_table(val_map={
            'is_available': 1,
            'unavailable_date': 'null'
        },
            where_clause=f'house_id={house_id}',
            table_name='lifull_house_link',
            cur=self.cur)
        self.cnx.commit()

        process_house_info(house_id, response, self.cnx, self.cur)
