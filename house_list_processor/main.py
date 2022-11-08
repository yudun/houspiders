import getopt
import sys
import pandas as pd

from mysql.util import get_mysql_cnx


def main(house_links_file_path, output_file_path):
    """
     1. Add new listed houses to `house_link` table and put them into feed
     2. For those houses with updated price or was unavailable,
        we merge its info to `house_link` and `house_price_history` table and put it into feed;
     3. For remaining house, they are existing house w/o updated price, we do nothing
     4. For those available house not showing up in latest house_list, put them into feed;

    :param house_links_file_path: The new house links crawled by house_list_spider
    :param output_file_path: A csv file path to be consumed by downstream house_info_spider
    """
    new_house_link_df = pd.read_csv(house_links_file_path)
    cnx = get_mysql_cnx()
    available_house_df = pd.read_sql('SELECT house_id, listing_house_price FROM lifull_house_link WHERE is_available', cnx)

    cnx.close()


if __name__ == "__main__":
    house_links_file_path = ''
    output_file_path = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:o:", ["ifile=", "ofile="])
    except getopt.GetoptError:
        print('main.py -i <house_links_file_path> -o <output_file_path>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('main.py -i <house_links_file_path> -o <output_file_path>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            house_links_file_path = arg
        elif opt in ("-o", "--ofile"):
            output_file_path = arg
    print('Input file is', house_links_file_path)
    print('Output file is', output_file_path)
    main(house_links_file_path, output_file_path)
