import psycopg2
import datetime


class DatabaseConnection:

    def __init__(self, state, county, db_name, host, port, username, password):
        self.state = state
        self.county = county
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

    def disconnect(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

    def create(self, data):
        if self.connection is None:
            print '[-] Connection Not Established, Cannot Create'
            return

        self._init_schema()
        self._insert_data(data)

    def _init_schema(self):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS TAGS(ID SERIAL PRIMARY KEY, TAG VARCHAR);')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS LOCATIONS(ID SERIAL PRIMARY KEY, STATE CHAR(2), COUNTY VARCHAR);')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS SIGNALS(ID SERIAL PRIMARY KEY, FREQUENCY DOUBLE PRECISION, DESCRIPTION VARCHAR, SYSTEM VARCHAR, TAG INT REFERENCES TAGS(ID), LOCATION INT REFERENCES LOCATIONS(ID), UPDATED DATE);')

    def _insert_data(self, data):
        self._insert_location()
        self._insert_tags(data)
        self._insert_signals(data)

    def _insert_location(self):
        existing_locations = self._get_existing_locations()

        if self.state in existing_locations and self.county in existing_locations[self.state]:
            raise Exception('[-] Data For {0}, {1} Already Inserted...'.format(self.county, self.state))

        self.cursor.execute("INSERT INTO LOCATIONS(STATE, COUNTY) VALUES('{0}', '{1}')".format(self.state, self.county))

    def _insert_tags(self, data):
        existing_tags = self._get_existing_tags()
        new_tags = list()
        for row in data:
            if row['Tag'] not in existing_tags:
                new_tags.append(row['Tag'])

        unique_new_tags = list(set(new_tags))
        unique_new_tags = sorted(unique_new_tags)

        for tag in unique_new_tags:
            self.cursor.execute("INSERT INTO TAGS(TAG) VALUES('{0}')".format(tag))

    def _insert_signals(self, data):
        tags = self._get_existing_tags()
        locations = self._get_existing_locations()
        for index, row in enumerate(data):
            split_date = row['Updated'].split(':')
            updated = datetime.date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
            description = row['Description'].replace("'", "''")
            system = row['System/Category'].replace("'", "''")
            self.cursor.execute("INSERT INTO SIGNALS(FREQUENCY, DESCRIPTION, SYSTEM, TAG, LOCATION, UPDATED) VALUES({0}, '{1}', '{2}', {3}, {4}, '{5}')".format(row['Frequency'], description, system, tags[row['Tag']], locations[self.state][self.county], updated))

    def _get_existing_tags(self):
        existing_tags = dict()

        self.cursor.execute("SELECT ID, TAG FROM TAGS")
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
