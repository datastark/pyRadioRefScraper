import psycopg2
import datetime


class DatabaseConnection:

    def __init__(self, db_name, host, port, username, password):
        self.db_name = db_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            if self.username:
                self.connection = psycopg2.connect(host=self.host, port=self.port, database=self.db_name, user=self.username, password=self.password)
            else:
                self.connection = psycopg2.connect(host=self.host, port=self.port, database=self.db_name)
            self.cursor = self.connection.cursor()
        except psycopg2.Error:
            print '[-] Cannot Establish Database Connection...'

        self._init_schema()

    def disconnect(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

    def insert_services(self, service_list):
        if self.connection is None:
            print '[-] Connection Not Established, Cannot Insert Service Information'

        self._insert_services(service_list)

    def insert_station_classes(self, class_list):
        if self.connection is None:
            print '[-] Connection Not Established, Cannot Insert Service Information'

        self._insert_station_classes(class_list)

    def insert_signals(self, state, county, data):
        if self.connection is None:
            print '[-] Connection Not Established, Cannot Insert Signal Information'
            return

        self._insert_signal_data(state, county, data)

    def insert_licenses(self, state, county, data):
        if self.connection is None:
            print '[-] Connection Not Established, Cannot Insert License Information'
            return

        self._insert_license_data(state, county, data)

    def _init_schema(self):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS FCC_SERVICES(CODE CHAR(2) PRIMARY KEY, DESCRIPTION VARCHAR);')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS FCC_STATION_CLASSES(CODE CHAR(5) PRIMARY KEY, DESCRIPTION VARCHAR);')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS SIGNAL_TAGS(ID SERIAL PRIMARY KEY, TAG VARCHAR);')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS LOCATIONS(ID SERIAL PRIMARY KEY, STATE CHAR(2), COUNTY VARCHAR);')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS SIGNALS(ID SERIAL PRIMARY KEY, FREQUENCY DOUBLE PRECISION, DESCRIPTION VARCHAR, SYSTEM VARCHAR, TAG INT REFERENCES SIGNAL_TAGS(ID), LOCATION INT REFERENCES LOCATIONS(ID), UPDATED DATE);')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS LICENSES(ID SERIAL PRIMARY KEY, ENTITY VARCHAR, FREQUENCY DOUBLE PRECISION, GRANTED DATE, NUM_DEVICES INTEGER, STATION_CLASS CHAR(5) REFERENCES FCC_STATION_CLASSES(CODE), SERVICE CHAR(2) REFERENCES FCC_SERVICES(CODE), CITY VARCHAR, LOCATION INT REFERENCES LOCATIONS(ID))')

    def _insert_license_data(self, state, county, data):
        self._insert_location(state, county)
        self._insert_licences(state, county, data)

    def _insert_signal_data(self, state, county, data):
        self._insert_location(state, county)
        self._insert_signal_tags(data)
        self._insert_signals(state, county, data)

    def _insert_services(self, service_list):
        for service in service_list:
            description = service['Description'].replace("'", "''")
            self.cursor.execute("INSERT INTO FCC_SERVICES(CODE, DESCRIPTION) VALUES('{0}', '{1}')".format(service['Code'], description))

    def _insert_station_classes(self, class_list):
        for station_class in class_list:
            description = station_class['Description'].replace("'", "''")
            self.cursor.execute("INSERT INTO FCC_STATION_CLASSES(CODE, DESCRIPTION) VALUES('{0}', '{1}')".format(station_class['Code'], description))

    def _insert_location(self, state, county):
        existing_locations = self._get_existing_locations()

        if state in existing_locations and county in existing_locations[state]:
            return

        self.cursor.execute("INSERT INTO LOCATIONS(STATE, COUNTY) VALUES('{0}', '{1}')".format(state, county))

    def _insert_signal_tags(self, data):
        existing_tags = self._get_existing_tags()
        new_tags = list()
        for row in data:
            if row['Tag'] not in existing_tags:
                new_tags.append(row['Tag'])

        unique_new_tags = list(set(new_tags))
        unique_new_tags = sorted(unique_new_tags)

        for tag in unique_new_tags:
            self.cursor.execute("INSERT INTO SIGNAL_TAGS(TAG) VALUES('{0}')".format(tag))

    def _insert_signals(self, state, county, data):
        tags = self._get_existing_tags()
        locations = self._get_existing_locations()
        for row in data:
            split_date = row['Updated'].split(':')
            updated = datetime.date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
            description = row['Description'].replace("'", "''")
            system = row['System/Category'].replace("'", "''")
            self.cursor.execute("INSERT INTO SIGNALS(FREQUENCY, DESCRIPTION, SYSTEM, TAG, LOCATION, UPDATED) VALUES({0}, '{1}', '{2}', {3}, {4}, '{5}')".format(row['Frequency'], description, system, tags[row['Tag']], locations[state][county], updated))

    def _insert_licences(self, state, county, data):
        locations = self._get_existing_locations()
        for row in data:
            split_date = row['Granted'].split('-')
            granted = datetime.date(int(split_date[0]), int(split_date[1]), int(split_date[2]))

            entity = row['Entity'].replace("'", "''")

            station_class = "'{0}'".format(row['Code'].upper())
            if station_class == "''":
                station_class = 'NULL'

            service = "'{0}'".format(row['Svc'].upper())
            if service == "''":
                service = 'NULL'

            self.cursor.execute("INSERT INTO LICENSES(ENTITY, FREQUENCY, GRANTED, NUM_DEVICES, STATION_CLASS, SERVICE, CITY, LOCATION) VALUES('{0}', {1}, '{2}', {3}, {4}, {5}, '{6}', {7})".format(entity, row['Frequency'], granted, row['Units'], station_class, service, row['City'].upper(), locations[state][county]))

    def _get_existing_tags(self):
        existing_tags = dict()

        self.cursor.execute("SELECT ID, TAG FROM SIGNAL_TAGS")
        query_results = self.cursor.fetchall()
        for tag_row in query_results:
            existing_tags[tag_row[1]] = tag_row[0]

        return existing_tags

    def _get_existing_locations(self):
        existing_locations = dict()
        self.cursor.execute("SELECT ID, STATE, COUNTY FROM LOCATIONS")
        query_results = self.cursor.fetchall()
        for location_row in query_results:
            if location_row[1] not in existing_locations:
                existing_locations[location_row[1]] = dict()

            if location_row[2] not in existing_locations[location_row[1]]:
                existing_locations[location_row[1]][location_row[2]] = location_row[0]

        return existing_locations
