import urllib2
import unicodedata
from urlparse import urlparse
from bs4 import BeautifulSoup


class RadioRefScraper:

    def __init__(self, base_table_url):
        self.base_url = urlparse(base_table_url).netloc
        self.page_url = base_table_url

    def scrape(self):
        data = list()
        self.append_page_data(self.page_url, data, first_page=True)
        return self.format_data(data)

    def append_page_data(self, url, data, first_page=False):
        page = urllib2.urlopen(url).read()
        soup = BeautifulSoup(page)

        table = soup.find('table', {'class': 'rrtable w1p'})

        if first_page:
            table_rows = table.find_all('tr')[1:]   # Skip only the header
        else:
            table_rows = table.find_all('tr')[2:]   # Skip the header and the first entry (repeated from last page)

        for table_row in table_rows:
            row = list()
            for entry in table_row.find_all('td'):
                row.append(entry.string)
            data.append(row)

        next_page = soup.find_all(lambda tag: (tag.name == 'a' and tag.text == 'Next Page >>'), href=True)
        if len(next_page) > 0:
            self.append_page_data('http://' + self.base_url + next_page[0]['href'], data)

    @staticmethod
    def format_data(data):
        headers = [['Frequency', 'Input', 'Callsign', 'Description', 'System/Category', 'Tag', 'Updated']]

        for index, entry in enumerate(data):
            entry = map(lambda x: unicodedata.normalize('NFKD', x).encode('ascii', 'ignore') if x else 'N/A', entry)
            data[index] = map(str.strip, entry)

        return headers + data
