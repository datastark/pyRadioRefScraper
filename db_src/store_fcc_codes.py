import csv
import os.path
import argparse
from DatabaseConnection import DatabaseConnection


def parse_args():
    parser = argparse.ArgumentParser(description='Store Radio Reference CSV into a PostGreSQL Database')

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
    service_code_file = '../resources/fcc_radio_services_map.csv'
    station_class_file = '../resources/fcc_station_class_map.csv'

    print '[+] Parsing Data Files'
    service_code_data = parse_file(service_code_file)
    station_class_data = parse_file(station_class_file)
    print '[+] Data Parsed Successfully...'

    print '[+] Connecting To Database'
    database = DatabaseConnection(args.db, args.host, args.port, args.username, args.password)
    database.connect()
    print '[+] Database Connection Established...'

    print '[+] Inserting Data'
    database.insert_services(service_code_data)
    database.insert_station_classes(station_class_data)
    print '[+] Data Insertion Complete...'

    print '[+] Committing Changes and Disconnecting'
    database.disconnect()
    print '[+] All Changes Persisted, Database Connection Terminated...'
    print '[+] Complete'
