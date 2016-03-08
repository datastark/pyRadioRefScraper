import csv
import argparse
from DatabaseConnection import DatabaseConnection


def parse_args():
    parser = argparse.ArgumentParser(description='Store Radio Reference CSV into a PostGreSQL Database')

    parser.add_argument('input_file', help='The path to the Radio Reference CSV file')
    parser.add_argument('db', help='The name of the Database to create')

    parser.add_argument('-host', default='localhost', help='The hostname of the Database Instance')
    parser.add_argument('-port', type=int, default=5432, help='The port of the Database Instance')
    parser.add_argument('-username', default=None, help='The username for the database')
    parser.add_argument('-password', default=None, help='The password for the database')

    return parser.parse_args()


def parse_file(input_file):
    output_data = list()

    with open(input_file) as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            output_data.append(row)

    return output_data


if __name__ == '__main__':
    args = parse_args()
    data = parse_file(args.input_file)

    database = DatabaseConnection(args.db, args.host, args.port, args.username, args.password)

    database.connect()
    database.create(data)
    database.disconnect()
    print '[+] Database Creation Complete'
