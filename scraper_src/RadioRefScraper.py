import time
import urllib2
import unicodedata
from urlparse import urlparse
from bs4 import BeautifulSoup


def _append_page_data(base_url, page_url, data, repeated_entry=False, count=1):
        print '[+] Scraping Page {0}'.format(count)
        page = urllib2.urlopen(page_url).read()
        soup = BeautifulSoup(page)

        table = soup.find('table', {'class': 'rrtable w1p'})

        if repeated_entry and count > 1:
            table_rows = table.find_all('tr')[2:]   # Skip the header and the first entry (repeated from last page)
        else:
            table_rows = table.find_all('tr')[1:]   # Skip only the header

        print '\t[+] Parsing {0} Rows'.format(len(table_rows))
        for table_row in table_rows:
            row = list()
            for entry in table_row.find_all('td'):
                if entry.string is None:
                    row.append(entry.text)
                else:
                    row.append(entry.string)
            data.append(row)

        next_page = soup.find_all(lambda tag: (tag.name == 'a' and tag.text == 'Next Page >>'), href=True)
        if len(next_page) > 0:
            print '\t[+] Sleeping Before Moving To Next Page...'
            time.sleep(30)
            print '\t[+] Moving On...'
            _append_page_data(base_url, 'http://' + base_url + next_page[0]['href'], data, count=count+1)


class RadioRefScraper:

    def __init__(self, base_table_url):
        self.base_url = urlparse(base_table_url).netloc
        self.page_url = base_table_url

    def scrape(self):
        data = list()
        print '[+] Scraping Data...'
        _append_page_data(self.base_url, self.page_url, data, repeated_entry=True)
        print '[+] Scraping Complete! Formatting Data...'
        return self.format_data(data)

    @staticmethod
    def format_data(data):
        headers = [['Frequency', 'Input', 'Callsign', 'Description', 'System/Category', 'Tag', 'Updated']]

        for index, entry in enumerate(data):
            entry = map(lambda x: unicodedata.normalize('NFKD', x).encode('ascii', 'ignore') if x else 'N/A', entry)
            data[index] = map(str.strip, entry)

        return headers + data


class LicenseScraper:

    def __init__(self, base_table_url):
        self.base_url = urlparse(base_table_url).netloc
        self.page_url = base_table_url

    def scrape(self):
        data = list()
        print '[+] Scraping Data...'
        _append_page_data(self.base_url, self.page_url, data)
        print '[+] Scraping Complete! Formatting Data...'
        return self.format_data(data)

    @staticmethod
    def format_data(data):
        headers = [['Entity', 'Callsign', 'Frequency', 'Granted', 'Stat', 'Units', 'Pag', 'Code', 'Svc', 'City']]

        for index, entry in enumerate(data):
            entry = map(lambda x: unicodedata.normalize('NFKD', x).encode('ascii', 'ignore') if x else 'N/A', entry)
            data[index] = map(str.strip, entry)

        return headers + data
