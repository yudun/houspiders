import scrapy
import re


class HouseListSpider(scrapy.Spider):
    name = 'house_list'
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'

    def start_requests(self):
        urls = [
            'https://www.homes.co.jp/mansion/chuko/tokyo/list/?page=271',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        total_num_house = re.sub('[, ]', '', response.css('.totalNum')[0].css('::text').get())
        print(f'Total {total_num_house} houses found:')
        for house in response.css('.moduleInner'):
            house_link = house.css('a::attr("href")').get()
            house_name = house.css('.bukkenName::text').get()
            print(house_link)
