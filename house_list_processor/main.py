"""
python3 ./main.py -i /home/ubuntu/houspiders/house_list_spider/output/2022-11-14/house_links.csv \
-o output/2022-11-14/house_id_to_crawl.csv --logfile log/2022-11-14-log.txt \
--crawl_date 2022-11-14 --category mansion_chuko --city tokyo
--
"""
import csv
import getopt
import sys
import pandas as pd
import logging

sys.path.append('../')

import db.utils as dbutil
from utils import utils


def handle_possible_new_house_df(df, cnx):
    """
    If the house exists, update it as is_available;
    If not, add it to `house_link` table.
    """
    cur = cnx.cursor(buffered=True)
    date_today_str = utils.get_date_str_today()
    success_added_rowcount = 0
    success_updated_rowcount = 0
    for index, row in df.iterrows():
        update_data = {
            'is_pr_item': row.is_pr_item_new,
            'listing_house_name': row.listing_house_name,
            'listing_house_price': row.listing_house_price_new,
            'sale_category': row.sale_category,
            'city': row.city,
            'is_available': 1,
            'unavailable_date': None
        }
        rowcount = dbutil.insert_table(val_map={
            'house_id': row.house_id,
            'first_available_date': date_today_str,
            **update_data
        },
            table_name='lifull_house_link',
            cur=cur,
            on_duplicate_update_val_map=update_data)
        if rowcount == 1:
            success_added_rowcount += 1
        elif rowcount == 2:
            success_updated_rowcount += 1

        logging.info(f'handle_possible_new_house_df: rowcount={rowcount}, house_id:{row.house_id}')
        # Commit the changes
        cnx.commit()

    return success_added_rowcount, success_updated_rowcount


def handle_updated_house_df(df, cnx):
    """
    Update the house in `house_link` table.
    """
    cur = cnx.cursor(buffered=True)
    success_updated_rowcount = 0
    for index, row in df.iterrows():
        rowcount = dbutil.update_table(val_map={
            'is_pr_item': row.is_pr_item_new,
            'listing_house_name': row.listing_house_name,
            'listing_house_price': row.listing_house_price_new,
            'sale_category': row.sale_category,
            'city': row.city,
            'is_available': 1,
            'unavailable_date': None
        },
            where_clause=f'house_id={row.house_id}',
            table_name='lifull_house_link',
            cur=cur)
        success_updated_rowcount += rowcount
        logging.info(f'handle_updated_house_df: rowcount={rowcount}, house_id:{row.house_id}')

        # Commit the changes
        cnx.commit()

    return success_updated_rowcount


def main(house_links_file_path, output_file_path, strategy, crawl_date, category, city):
    """
     1. Add new listed houses to `house_link` table and put them into feed
     2. For those houses with updated price or was unavailable,
        we merge its info to `house_link` and `house_price_history` table and put it into feed;
     3. For remaining house, they are existing house w/o updated price, we do nothing
     4. For those available house not showing up in latest house_list, put them into feed;

    :param house_links_file_path: The new house links crawled by house_list_spider
    :param output_file_path: A csv file path to be consumed by downstream house_info_spider
    """
    output_house_ids = []
    success_added_rowcount = 0
    success_updated_available_rowcount = 0
    success_updated_rowcount = 0

    # Read and drop duplicated house_id
    new_house_link_df = pd.read_csv(house_links_file_path)
    num_duplicated_new_houses = len(
        new_house_link_df[new_house_link_df.duplicated(['house_id'], keep=False)]['house_id'].unique())

    new_house_link_df.sort_values(by=['house_id', 'is_pr_item'],
                                  ascending=[True, False],
                                  na_position='last',
                                  inplace=True)
    new_house_link_df.drop_duplicates('house_id', inplace=True)
    logging.info(
        f'{num_duplicated_new_houses} duplicated houses and {len(new_house_link_df)} unique houses found from {house_links_file_path}.')

    # Connect to the database
    cnx = dbutil.get_mysql_cnx()

    # Read in existing available house
    available_house_df = pd.read_sql(
        'SELECT house_id, is_pr_item, listing_house_price FROM lifull_house_link WHERE is_available', cnx)
    if len(available_house_df) > 0:
        logging.info(f'{len(available_house_df)} houses read from database.')
        available_house_df['house_id'] = available_house_df['house_id'].astype(int)
        available_house_df['is_pr_item'] = available_house_df['is_pr_item'].astype(bool)

        # Get 3 different groups of houses: new, removed, updated.
        merged_df = new_house_link_df.merge(available_house_df, how='outer', on=['house_id'], suffixes=['_new', '_old'])

        newly_unavailable_house_df = merged_df[merged_df['is_pr_item_new'].isnull()]
        possible_new_house_df = merged_df[merged_df['is_pr_item_old'].isnull()]
        updated_house_df = merged_df.dropna()
        updated_house_df = updated_house_df[
            (updated_house_df['is_pr_item_new'] != updated_house_df['is_pr_item_old']) | (
                    updated_house_df['listing_house_price_new'] != updated_house_df['listing_house_price_old'])]
    else:
        logging.info('No available houses read from database.')
        newly_unavailable_house_df = None
        possible_new_house_df = new_house_link_df
        possible_new_house_df['is_pr_item_new'] = possible_new_house_df['is_pr_item']
        possible_new_house_df['listing_house_price_new'] = possible_new_house_df['listing_house_price']
        updated_house_df = None

    # 1. Handle newly_unavailable_house_df: simply put them into feed;
    if newly_unavailable_house_df is not None:
        output_house_ids += list(newly_unavailable_house_df['house_id'])

    # 2. Handle possible_new_house_df
    if possible_new_house_df is not None:
        success_added_rowcount, success_updated_available_rowcount = handle_possible_new_house_df(possible_new_house_df,
                                                                                                  cnx)
        output_house_ids += list(possible_new_house_df['house_id'])

    # 2. Handle updated_house_df
    if updated_house_df is not None:
        success_updated_rowcount = handle_updated_house_df(updated_house_df, cnx)
        output_house_ids += list(updated_house_df['house_id'])

    if strategy == 'all':
        output_house_ids = list(new_house_link_df['house_id'])

    logging.info(f'success_added_rowcount:{success_added_rowcount}, '
                 f'success_updated_available_rowcount: {success_updated_available_rowcount}, '
                 f'success_updated_rowcount: {success_updated_rowcount}')

    cur = cnx.cursor(buffered=True)
    insert_data = {
        'crawl_date': crawl_date,
        'category': category,
        'city': city,
        'new_added_house_num': success_added_rowcount,
        'new_unavailable_become_available_house_num': success_updated_available_rowcount,
        'updated_house_num': success_updated_rowcount
    }
    dbutil.insert_table(val_map=insert_data,
                        table_name='lifull_crawler_stats',
                        cur=cur,
                        on_duplicate_update_val_map=insert_data)
    cnx.commit()

    # Close the database connection
    cnx.close()

    # Write output_house_ids to output_file_path
    with open(output_file_path, 'w+') as f:
        # using csv.writer method from CSV package
        write = csv.writer(f)
        write.writerow(['house_id'])
        write.writerows([[x] for x in output_house_ids])
        logging.info(f'{len(output_house_ids)} houses written to {output_file_path}')


if __name__ == "__main__":
    usage = 'main.py -i <parent_dir_path> -o <output_file_path> -s <strategy> --logfile <log_file> --loglevel <loglevel>'
    house_links_file_path = ''
    output_file_path = ''
    strategy = 'update_only'
    log_file = ''
    loglevel = logging.INFO
    crawl_date = ''
    category = ''
    city = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:o:s:l:",
                                   ["ifile=", "ofile=", "strategy=", "logfile=", "loglevel="])
    except getopt.GetoptError:
        print(usage)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(usage)
            sys.exit()
        elif opt in ("-i", "--ifile"):
            house_links_file_path = arg
        elif opt in ("-o", "--ofile"):
            output_file_path = arg
        elif opt in ("-s", "--strategy"):
            strategy = arg
        elif opt in ("-l", "--logfile"):
            log_file = arg
        elif opt in ("--loglevel"):
            loglevel = utils.get_log_level_from_str(arg)
        elif opt in ("--crawl_date"):
            crawl_date = arg
        elif opt in ("--category"):
            category = arg
        elif opt in ("--city"):
            city = arg
    assert strategy in ('update_only', 'all')
    if house_links_file_path == '' or output_file_path == '' or log_file == '' or crawl_date == '' or category == '' or city == '':
        print(usage)
        sys.exit(2)

    print('Input file is', house_links_file_path)
    print('Output file is', output_file_path)
    print('Strategy used:', strategy)
    print('Log to file:', log_file)

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=loglevel,
                        filemode='w',
                        filename=log_file)

    main(house_links_file_path, output_file_path, strategy, crawl_date, category, city)
