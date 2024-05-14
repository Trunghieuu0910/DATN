from geopy.geocoders import Nominatim
import requests
import ast
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

from src.utils.file_utils import write_error_file
from src.utils.logger_utils import get_logger

logger = get_logger('Detect Country')


def get_country_from_city(city_name):
    geolocator = Nominatim(user_agent="city_to_location")
    location = geolocator.geocode(city_name, language='en')
    if location:
        country_name = location.address.split(',')[-1].strip()
        city_name = location.address.split(',')[0].strip()
        latitude = location.latitude
        longitude = location.longitude
        if country_name:
            return {"city_name": city_name, "country": country_name,
                    'coordinates': {'latitude': latitude, 'longitude': longitude}}
    else:
        return None


def get_country_from_city_v2(city_name):
    print(city_name)
    url = f'https://nominatim.openstreetmap.org/search.php?q={city_name}&format=jsonv2&accept-language=en'
    driver = get_driver()
    driver.get(url)
    try:
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        content = soup.select_one('pre').get_text()
        my_list = ast.literal_eval(content)
        location = my_list[0]
        if location:
            country_name = location.get('display_name').split(',')[-1].strip()
            city_name = location.get('display_name').split(',')[0].strip()
            return {"city_name": city_name, "country": country_name}
    except Exception as e:
        logger.exception(e)
        error_path = '/home/hieunguyen/DHBK/DATN/artifact/error_city.txt'
        write_error_file(error_path, city_name)

def get_driver():
    chrome_options = Options()
    # chrome_options.binary_location = '/usr/local/bin/chromedriver'
    chrome_options.add_argument('--headless')
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
    chrome_options.add_argument(f'user-agent={user_agent}')
    chrome_options.add_argument("window-size=1920,1080")
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    return driver