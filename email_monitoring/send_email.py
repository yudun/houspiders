"""
python3 ./send_email.py -m summary --crawl_date 2022-11-17
"""
import getopt
import os
import sys
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json

sys.path.append('../')

import db.utils as dbutil


def send_email(subject, mail_content, to_emails=None, sender_email='housetech.alert@gmail.com'):
    # The mail addresses and password
    if to_emails is None:
        to_emails = ['wyudun@gmail.com']
    f = open('/home/ubuntu/houspiders/email_monitoring/key.json')
    keys = json.load(f)

    # Set up the MIME
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = ','.join(to_emails)
    message['Subject'] = subject  # The subject line

    # The body and the attachments for the mail
    message.attach(MIMEText(mail_content, 'plain'))
    # Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587)  # Use gmail with port
    session.starttls()  # enable security
    session.login(sender_email, keys[sender_email])  # Login with mail_id and password
    text = message.as_string()
    session.sendmail(sender_email, to_emails, text)
    session.quit()


def send_summary_email(crawl_date):
    cnx = dbutil.get_mysql_cnx()
    stats_df = pd.read_sql(f"""SELECT category, city, new_added_house_num, 
                            new_unavailable_become_available_house_num, 
                            updated_house_num,
                            new_unavailable_house_num 
                        FROM lifull_crawler_stats 
                        WHERE crawl_date = '{crawl_date}'
                        ORDER BY city, category
                        """, cnx)

    subject = f"{stats_df['new_added_house_num'].sum()} New, {stats_df['new_unavailable_house_num'].sum()} Removed, {stats_df['updated_house_num'].sum()} Updated, {stats_df['new_unavailable_become_available_house_num'].sum()} Reopen"

    result_content = '\n'.join([
        f'{x.city} {x.category}: {x.new_added_house_num} New, {x.new_unavailable_house_num} Removed, {x.updated_house_num} Updated, {x.new_unavailable_become_available_house_num} Reopen'
        for _, x in stats_df.iterrows()])

    send_email(subject=f'[Houspider Update][{crawl_date}] {subject}',
               mail_content=f"""Houspider Results for {crawl_date}:
{result_content}
""")
    print('Summary Email Sent successfully.')


def send_alert_email(crawl_date):
    subject = ''

    has_error_list_urls_alert = False
    error_list_urls_error_content = ''
    error_list_urls_paths = [f'/home/ubuntu/houspiders/house_list_spider/output/{crawl_date}/error_list_urls.csv',
                             f'/home/ubuntu/houspiders/house_list_spider/output/{crawl_date}/error_chintai_list_urls.csv']
    error_list_urls_df = None
    for error_list_urls_path in error_list_urls_paths:
        if os.path.exists(error_list_urls_path):
            try:
                error_list_urls_df = pd.read_csv(error_list_urls_path)
            except pd.errors.EmptyDataError:
                pass
            except:
                has_error_list_urls_alert = True
                error_list_urls_error_content += f'\nError reading {error_list_urls_path}'

            if error_list_urls_df is not None and len(error_list_urls_df) > 0:
                has_error_list_urls_alert = True
                error_list_urls_first_3_str = '\n'.join(list(error_list_urls_df[:3]['error_list_url']) + ['...'])
                error_list_urls_error_content += f"""
{len(error_list_urls_df)} house list page urls have errors:
{error_list_urls_first_3_str}
"""
        else:
            has_error_list_urls_alert = True
            error_list_urls_error_content += f'\nPath not exists: {error_list_urls_path}'

    if has_error_list_urls_alert:
        subject += ' error_house_list_url'

    has_error_house_info_urls_alert = False
    error_house_info_urls_error_content = ''
    error_house_info_url_df = None
    error_house_info_url_paths = [f'/home/ubuntu/houspiders/house_info_spider/output/{crawl_date}/error_house_id2.csv',
                                  f'/home/ubuntu/houspiders/house_info_spider/output/{crawl_date}/error_house_chintai_id2.csv']
    for error_house_info_url_path in error_house_info_url_paths:
        try:
            error_house_info_url_df = pd.read_csv(error_house_info_url_path)
        except pd.errors.EmptyDataError:
            pass
        except:
            has_error_house_info_urls_alert = True
            error_house_info_urls_error_content += f'\nError reading {error_house_info_url_path}'

        if error_house_info_url_df is not None and len(error_house_info_url_df) > 0:
            has_error_house_info_urls_alert = True
            error_house_info_url_str = '\n'.join(
                [f'{x.house_id}: {x.fail_reason}' for _, x in error_house_info_url_df.iterrows()])
            error_house_info_urls_error_content += f"""
{len(error_house_info_url_df)} house info urls have errors:
{error_house_info_url_str}
"""
    if has_error_house_info_urls_alert:
            subject += ' error_house_info_url'

    if has_error_list_urls_alert or has_error_house_info_urls_alert:
        send_email(subject=f'[Houspider Error][{crawl_date}]{subject}',
                   mail_content=f"""We found following errors during crawling data for {crawl_date}:
{error_list_urls_error_content}{error_house_info_urls_error_content}
""")
    else:
        print('No alerts today.')


def main(mode, crawl_date):
    if mode == 'summary':
        send_summary_email(crawl_date)
    elif mode == 'alert':
        send_alert_email(crawl_date)


if __name__ == "__main__":
    usage = 'main.py -m <mode> --crawl_date <crawl_date>'
    mode = 'summary'
    crawl_date = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hm:", ["mode=", "crawl_date="])
    except getopt.GetoptError:
        print(usage)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(usage)
            sys.exit()
        elif opt in ("-m", "--mode"):
            mode = arg
        elif opt in ("--crawl_date"):
            crawl_date = arg
    assert mode in ('summary', 'alert')

    print('Email Mode Used:', mode)
    print('Crawl_date:', crawl_date)

    main(mode, crawl_date)
