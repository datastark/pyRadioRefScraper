import urllib2
from bs4 import BeautifulSoup


def scrape(url):
    print "[+] Scraping Page '{0}'".format(url)
    class_map = ['Code,Description']
    page = urllib2.urlopen(url).read()
    soup = BeautifulSoup(page, 'lxml')

    table = soup.find('table', {'class': 'rrtable sortable'})

    rows = table.find_all('tr')
    for row in rows[1:]:
        entries = row.find_all('td')
        code = entries[0].text.strip()
        description = entries[1].text.strip()
        class_map.append('{0},"{1}"\n'.format(code, description))

    print '[+] Scraping Complete, All Station Class Codes Mapped'
    return class_map


def output(class_map):
    file_name = '../resources/fcc_station_class_map.csv'
    print "[+] Outputting Station Class Code Map To '{0}'".format(file_name)
    with open(file_name, 'wb') as csv_file:
        for row in class_map:
            csv_file.write(row)
    print '[+] File Created Successfully'


if __name__ == '__main__':
    print '[+] Scraping Station Class Codes From RadioReference Table'
    fcc_advanced_search_url = 'http://wiki.radioreference.com/index.php/FCC_Station_Class_Codes'
    service_map = scrape(fcc_advanced_search_url)
    output(service_map)
    print '[+] Complete, Exiting...'
