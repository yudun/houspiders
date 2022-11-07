import getopt
import sys


def main(argv):
    house_links_file_path = ''
    output_file_path = ''
    try:
        opts, args = getopt.getopt(argv, "hi:o:", ["ifile=", "ofile="])
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


if __name__ == "__main__":
    main(sys.argv[1:])
