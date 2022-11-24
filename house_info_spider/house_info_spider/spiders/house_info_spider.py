"""
scrapy crawl house_info -O output/2022-11-15/error_house_id1.csv \
-a i=/home/ubuntu/houspiders/house_list_processor/output/2022-11-15/house_id_to_crawl.csv -a m=original \
-a crawl_date=2022-11-15 -a category=mansion_chuko -a city=tokyo \
--logfile log/2022-11-15-log1.txt

scrapy crawl house_info -O output/2022-11-15/error_house_id2.csv \
-a i=output/2022-11-15/error_house_id1.csv -a m=error \
-a crawl_date=2022-11-15 -a category=mansion_chuko -a city=tokyo \
--logfile log/2022-11-15-log2.txt


scrapy crawl house_info -O output/2022-11-15/error_house_chintai_id1.csv \
-a i=/home/ubuntu/houspiders/house_list_processor/output/2022-11-15/house_chintai_id_to_crawl.csv -a m=original \
-a crawl_date=2022-11-15 -a category=chintai -a city=tokyo \
--logfile log/2022-11-15-chintai-log1.txt

scrapy crawl house_info -O output/2022-11-15/error_house_chintai_id2.csv \
-a i=output/2022-11-15/error_house_chintai_id1.csv -a m=error \
-a crawl_date=2022-11-15 -a category=chintai -a city=tokyo \
--logfile log/2022-11-15-chintai-log2.txt
"""
import scrapy
from scrapy import signals
import pandas as pd
import logging
import sys

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

sys.path.append('../')

from house_info_processor.main import process_mansion_info, process_rent_info, process_unavailable_house
import db.utils as dbutil
from utils import utils
from utils import constant


class HouseInfoSpider(scrapy.Spider):
    name = 'house_info'
    # Allow handling 404 requests
    handle_httpstatus_list = [404]
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'

    def __init__(self, i, m, crawl_date, category, city, **kw):
        self.house_link_file_path = i
        self.mode = m
        self.crawl_date = crawl_date
        self.category = category
        self.city = city
        super(HouseInfoSpider, self).__init__(**kw)
        # Init database connection
        self.cnx = None
        self.cur = None
        self.new_unavailable_house_num = 0

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
        logging.info(f'Total {self.new_unavailable_house_num} houses become unavailable today.')
        stats_df = pd.read_sql(f"""SELECT new_unavailable_house_num 
                    FROM lifull_crawler_stats 
                    WHERE crawl_date = '{self.crawl_date}'
                    AND category = '{self.category}'
                    AND city = '{self.city}'
                    """, self.cnx)
        if len(stats_df) != 1:
            logging.error(f'lifull_crawler_stats fail to get old old_new_unavailable_house_num for {self.crawl_date} {self.category} {self.city}')
            old_new_unavailable_house_num = 0
        else:
            old_new_unavailable_house_num = int(stats_df.loc[0].new_unavailable_house_num)

        new_unavailable_house_num = old_new_unavailable_house_num + self.new_unavailable_house_num
        insert_data = {
            'crawl_date': self.crawl_date,
            'category': self.category,
            'city': self.city,
            'new_unavailable_house_num': new_unavailable_house_num
        }
        dbutil.insert_table(val_map=insert_data,
                            table_name='lifull_crawler_stats',
                            cur=self.cur,
                            on_duplicate_update_val_map=insert_data)
        self.cnx.commit()
        logging.info(f'{new_unavailable_house_num} new_unavailable_house_num updated in lifull_crawler_stats for {self.crawl_date} {self.category} {self.city}')

        self.cnx.close()

    def start_requests(self):
        try:
            df = pd.read_csv(self.house_link_file_path)
        except pd.errors.EmptyDataError:
            df = pd.DataFrame()

        if self.mode == 'error':
            # The error_house_id csv may contain duplicates
            df = df.drop_duplicates(subset=['house_id'])
        logging.info(f'Total {len(df)} houses will be scrawled.')

        for index, row in df.iterrows():
            if self.category == constant.CHINTAI:
                yield scrapy.Request(url=utils.get_lifull_chintai_url_from_house_id(row.house_id), callback=self.parse_house_info,
                                     errback=self.errback_httpbin,
                                     cb_kwargs={'house_id': str(row.house_id)})
            else:
                yield scrapy.Request(url=utils.get_lifull_mansion_url_from_house_id(row.house_id), callback=self.parse_house_info,
                                     errback=self.errback_httpbin,
                                     cb_kwargs={'house_id': str(row.house_id)})

    def parse_house_info(self, response, house_id):
        logging.info(f'Start crawling {house_id}')
        if response.status == 404 or response.css('.mod-expiredInformation').get() is not None or response.css('.mod-bukkenNotFound').get() is not None:
            row_count = process_unavailable_house(house_id, self.category, self.cnx, self.cur)
            if row_count > 0:
                self.new_unavailable_house_num += 1
                logging.info(f'{house_id} has been marked as unavailable.')
            else:
                logging.error(f'{house_id} fail to be marked as unavailable.')
            return

        if self.category == constant.CHINTAI:
            if len(response.css('.mod-detailTopRent')) != 1:
                logging.error(f'House is in wrong format: {response.url}')
                return

            # Mark it as available in `house_link` table;
            dbutil.update_table(val_map={
                'is_available': 1,
                'unavailable_date': None
            },
                where_clause=f'house_id="{house_id}"',
                table_name='lifull_rent_link',
                cur=self.cur)
            self.cnx.commit()

            process_rent_info(house_id, response, self.cnx, self.cur)
        else:
            if len(response.css('.mod-detailTopSale')) != 1:
                logging.error(f'House is in wrong format: {response.url}')
                return

            with open(f'output/raw_html/{house_id}.html', 'wb') as html_file:
                html_file.write(response.body)
            logging.info(f'{house_id} has been saved locally.')

            # Mark it as available in `house_link` table;
            dbutil.update_table(val_map={
                'is_available': 1,
                'unavailable_date': None
            },
                where_clause=f'house_id={house_id}',
                table_name='lifull_house_link',
                cur=self.cur)
            self.cnx.commit()

            process_mansion_info(house_id, response, self.cnx, self.cur)
        logging.info(f'Finish processing {house_id}')
    
    def errback_httpbin(self, failure):
        # log all failures
        logging.error(repr(failure))
        house_id = failure.request.cb_kwargs['house_id']

        # In case you want to do something special for some errors,
        # you may need the failure's type:
        if failure.check(HttpError):
            # these exceptions come from HttpError spider middleware
            # you can get the non-200 response
            response = failure.value.response
            logging.error('HttpError on %s', response.url)
            fail_reason = 'HttpError'

        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            logging.error('DNSLookupError on %s', request.url)
            fail_reason = 'DNSLookupError'

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            logging.error('TimeoutError on %s', request.url)
            fail_reason = 'TimeoutError'
        else:
            fail_reason = 'Other'

        yield {'house_id': house_id,
               'fail_reason': fail_reason}
        