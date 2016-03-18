import urllib2
from bs4 import BeautifulSoup


def scrape(url):
    print "[+] Scraping Page '{0}'".format(url)
    service_map = ['Code,Description']
    page = urllib2.urlopen(url).read()
    soup = BeautifulSoup(page, 'lxml')

    table = soup.find('select', {'name': 'radioservicecode'})

    entries = table.find_all('option')
    for option in entries:
        text = option.text.strip()
        code = text[:2]
        description = text[5:]
        service_map.append('{0},"{1}"\n'.format(code, description))

    print '[+] Scraping Complete, All Service Codes Mapped'
    return service_map


def output(service_map):
    print "[+] Outputting Service Code Map To '../resources/fcc_radio_services_map.csv'"
    with open('../resources/fcc_radio_services_map.csv', 'wb') as csv_file:
        for row in service_map:
            csv_file.write(row)
    print '[+] File Created Successfully'


if __name__ == '__main__':
    print '[+] Scraping Radio Service Codes From FCC Database'
    fcc_advanced_search_url = 'http://wireless2.fcc.gov/UlsApp/UlsSearch/searchAdvanced.jsp'
    service_map = scrape(fcc_advanced_search_url)
    output(service_map)
    print '[+] Complete, Exiting...'
