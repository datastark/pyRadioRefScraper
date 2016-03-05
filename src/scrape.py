import csv
import argparse
from RadioRefScraper import RadioRefScraper


def parse_arguments():
    parser = argparse.ArgumentParser(description='Scrape RadioReference.com for Spectrum Usage Information')

    parser.add_argument('table_url', help='The URL of the first page of the table to be scraped.')
    parser.add_argument('output_file', help='The path/name of the output CSV file')

    return parser.parse_args()


def write_data(output_data, output_file):
    with open(output_file, 'wb') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(output_data)


if __name__ == '__main__':
    args = parse_arguments()
    scraper = RadioRefScraper(args.table_url)
    data = scraper.scrape()
    write_data(data, args.output_file)
