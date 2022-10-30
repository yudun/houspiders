import scrapy
import re


class HouseInfoSpider(scrapy.Spider):
    name = 'house_info'
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'

    def start_requests(self):
        urls = [
            'https://www.homes.co.jp/mansion/b-1455630000097/',
            'https://www.homes.co.jp/mansion/b-1418210001548/',
            'https://www.homes.co.jp/mansion/b-1417040002965/',
            'https://www.homes.co.jp/mansion/b-1227550000787/',
            'https://www.homes.co.jp/mansion/b-1244810005865/',
            'https://www.homes.co.jp/mansion/b-1434000031135/',
            'https://www.homes.co.jp/mansion/b-1375750000844/',
            'https://www.homes.co.jp/mansion/b-60000330221911/',
            'https://www.homes.co.jp/mansion/b-1393080000405/',
            'https://www.homes.co.jp/mansion/b-60000330221558/',
            'https://www.homes.co.jp/mansion/b-1227550000790/',
            'https://www.homes.co.jp/mansion/b-60000330221016/',
            'https://www.homes.co.jp/mansion/b-60000330221468/',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
            house_name = response.css('.break-words::text').get()
            house_price = response.css('p.text-brand.text-xl.font-bold::text').get()

            addr1 = response.css('.flex.justify-between.flex-start::text').get()
            addr2 = response.css('.text-base.text-left::text').get()
            house_rough_address_text = addr1 if addr1 is not None else addr2

            station_list = [x.get() for x in response.css('.space-x-2.flex.items-center').css('.text-base.leading-7::text')]
            print(house_name, house_price, house_rough_address_text, station_list, '\n')
