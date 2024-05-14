import requests
from bs4 import BeautifulSoup
from src.utils.file_utils import write_json_file


def crawl_timezone():
    url = 'https://timezonedb.com/time-zones'
    response = requests.get(url)
    res = []
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        for row in soup.find_all('tr'):
            columns = row.find_all('td')
            if columns:
                country_code = columns[0].text.strip()
                country_name = columns[1].text.strip()
                time_zone = columns[2].text.strip()
                utc_offset = columns[3].text.strip()
                data = {"Country Code": country_code, "Country Name": country_name, "Time Zone": time_zone,
                        "UTC Offset": utc_offset}
                res.append(data)

        file_path = '/home/hieunguyen/DHBK/DATN/artifact/city_timezone.json'
        write_json_file(file_path=file_path, list_dict=res)
    else:
        print('Failed to retrieve data from the website.')
