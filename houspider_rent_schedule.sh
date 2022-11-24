#!/usr/bin/bash
export PATH="/home/ubuntu/.autojump/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/home/ubuntu/.local/bin"

today=$(TZ=America/Los_Angeles date '+%Y-%m-%d')

# Run house_list_spider
# cd /home/ubuntu/houspiders/house_list_spider
# scrapy crawl house_list -O output/${today}/house_chintai_links.csv \
# -a error_list_urls_path=output/${today}/error_chintai_list_urls.csv \
# -a category=chintai \
# --logfile log/${today}-chintai-log.txt

# # Process the new house list
# cd /home/ubuntu/houspiders/house_list_processor
# python3 ./main.py -i /home/ubuntu/houspiders/house_list_spider/output/${today}/house_chintai_links.csv \
# -o output/${today}/house_chintai_id_to_crawl.csv --logfile log/${today}-chintai-log.txt \
# --crawl_date ${today} --category chintai --city tokyo

# Run house_info_spider based on house_id_to_crawl.csv output from processor
cd /home/ubuntu/houspiders/house_info_spider
scrapy crawl house_info -O output/${today}/error_house_chintai_id1.csv \
-a i=/home/ubuntu/houspiders/house_list_processor/output/${today}/house_chintai_id_to_crawl.csv -a m=original \
-a crawl_date=${today} -a category=chintai -a city=tokyo \
--logfile log/${today}-chintai-log1.txt

# Run house_info_spider for 2nd time to crawl error_house_id1.csv
scrapy crawl house_info -O output/${today}/error_house_chintai_id2.csv \
-a i=output/${today}/error_house_chintai_id1.csv -a m=error \
-a crawl_date=${today} -a category=chintai -a city=tokyo \
--logfile log/${today}-chintai-log2.txt

# Send summary email
# cd /home/ubuntu/houspiders/email_monitoring
# python3 ./send_email.py -m summary --crawl_date ${today}
# python3 ./send_email.py -m alert --crawl_date ${today}
