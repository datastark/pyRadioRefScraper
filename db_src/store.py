import csv
import os.path
import argparse
from DatabaseConnection import DatabaseConnection


def parse_args():
    parser = argparse.ArgumentParser(description='Store Radio Reference CSV into a PostGreSQL Database')

    parser.add_argument('input_file', help="The path to the Radio Reference CSV file, name should be SS_County.csv, where SS is the state's abbreviation")
    parser.add_argument('db', help='The name of the Database to create')

    parser.add_argument('-host', default='localhost', help='The hostname of the Database Instance')
    parser.add_argument('-port', type=int, default=5432, help='The port of the Database Instance')
    parser.add_argument('-username', default=None, help='The username for the database')
    parser.add_argument('-password', default=None, help='The password for the database')

    return parser.parse_args()


def parse_file_name(input_file):
    base_name = os.path.basename(input_file)
    state = base_name.split('_')[0].upper()
    county = base_name.split('_')[1].split('.')[0].upper()
    file_type = 'signal'
    if 'Licenses' in base_name:
        file_type = 'license'
    return state, county, file_type


def parse_file(input_file):
    output_data = list()

    with open(input_file) as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            output_data.append(row)

    return output_data


if __name__ == '__main__':
    args = parse_args()
    state, county, file_type = parse_file_name(args.input_file)

    print '[+] Parsing Data File'
    data = parse_file(args.input_file)
    print '[+] Data Parsed Successfully...'

    print '[+] Connecting To Database'
    database = DatabaseConnection(args.db, args.host, args.port, args.username, args.password)
    database.connect()
    print '[+] Database Connection Established...'

    print '[+] Inserting Data For {0}, {1}'.format(county, state)
    if file_type == 'signal':
        database.insert_signals(state, county, data)
    else:
        database.insert_licenses(state, county, data)
    print '[+] Data Insertion Complete...'

    print '[+] Committing Changes and Disconnecting'
    database.disconnect()
    print '[+] All Changes Persisted, Database Connection Terminated...'
    print '[+] Complete'
