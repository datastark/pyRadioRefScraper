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
        self.cursor.execute('''CREATE TABLE TAGS(ID INT PRIMARY KEY, TAG VARCHAR);''')
        self.cursor.execute('''CREATE TABLE SIGNALS(ID INT PRIMARY KEY, FREQUENCY DOUBLE PRECISION, DESCRIPTION VARCHAR, SYSTEM VARCHAR, TAG INT REFERENCES TAGS(ID), UPDATED DATE);''')

    def _insert_data(self, data):
        tags = self._insert_tags(data)
        self._insert_signals(data, tags)

    def _insert_tags(self, data):
        all_tags = list()
        for row in data:
            all_tags.append(row['Tag'])

        unique_tags = list(set(all_tags))
        unique_tags = sorted(unique_tags)

        for index, tag in enumerate(unique_tags):
            self.cursor.execute("INSERT INTO TAGS VALUES({0}, '{1}')".format(index+1, tag))

        return unique_tags

    def _insert_signals(self, data, tags):
        for index, row in enumerate(data):
            split_date = row['Updated'].split(':')
            updated = datetime.date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
            description = row['Description'].replace("'", "''")
            system = row['System/Category'].replace("'", "''")
            self.cursor.execute("INSERT INTO SIGNALS VALUES({0}, {1}, '{2}', '{3}', {4}, '{5}')".format(index+1, row['Frequency'], description, system, tags.index(row['Tag'])+1, updated))
