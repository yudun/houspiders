#!/usr/bin/bash
today=$(TZ=America/Los_Angeles date '+%Y-%m-%d')

# Run house_list_spider
cd /home/ubuntu/houspiders/house_list_spider
scrapy crawl house_list -O output/${today}/house_links.csv --logfile log/${today}-log.txt

# Process the new house list
cd /home/ubuntu/houspiders/house_list_processor
python3 ./main.py -i /home/ubuntu/houspiders/house_list_spider/output/${today}/house_links.csv \
-o output/${today}/house_id_to_crawl.csv --logfile log/${today}-log.txt \
--crawl_date ${today} --category mansion_chuko --city tokyo

# Run house_info_spider based on house_id_to_crawl.csv output from processor
cd /home/ubuntu/houspiders/house_info_spider
scrapy crawl house_info -O output/${today}/error_house_id1.csv \
-a i=/home/ubuntu/houspiders/house_list_processor/output/${today}/house_id_to_crawl.csv -a m=original \
-a crawl_date=${today} -a category=mansion_chuko -a city=tokyo \
--logfile log/${today}-log1.txt

# Run house_info_spider for 2nd time to crawl error_house_id1.csv
scrapy crawl house_info -O output/${today}/error_house_id2.csv \
-a i=output/${today}/error_house_id1.csv -a m=error \
-a crawl_date=${today} -a category=mansion_chuko -a city=tokyo \
--logfile log/${today}-log2.txt

# Send summary email

