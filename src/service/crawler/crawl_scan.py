import time
import json

from bs4 import BeautifulSoup
from bs4 import BeautifulSoup as soup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

from src.service.crawler.base_crawler import BaseCrawler
from src.database.mongodb.mongodb import MongoDB
from src.utils.file_utils import write_error_file
from src.utils.logger_utils import get_logger

logger = get_logger('Scan Crawler')


class ScanCrawler:
    def __init__(self):
        self.db = MongoDB()

    def crawl_balance_usd(self, cursor=None, start=0, end=0):
        if not cursor:
            cursor = self.db.get_social_users_by_filter(filter_={'flag': {'$in': [0, 1]}}, projection=['address', 'addresses', 'balanceUSD'])

        driver = self.get_driver()
        count = 0
        for doc in cursor:
            count += 1
            address = doc.get('address', None)
            if not address:
                address = doc.get('addresses').get('ethereum')

            logger.info(f"Execute address {address} {count}")
            url = f"https://etherscan.io/address/{address}"
            if count < 0:
                continue
            else:
                try:
                    driver.get(url)
                    html_source = driver.page_source
                    soup = BeautifulSoup(html_source, 'html.parser')
                    balance_token = soup.select_one('#dropdownMenuBalance').get_text()
                    balance_token = balance_token.split('(')[0]
                    balance_token = balance_token.strip()
                    start = balance_token.find('$')
                    balance_token = balance_token[start + 1:]
                    balance_token = balance_token.replace(",", "")
                    balance_token = float(balance_token)
                    print(f'Native balance is {doc.get("balanceUSD")}')
                    balance_token = balance_token + doc.get('balanceUSD', 0)
                except Exception as e:
                    logger.exception(e)
                    balance_token = doc.get('balanceUSD', 0)

            user = {"_id": "0x1_" + address, 'balanceUSD': balance_token}
            print(user)
            self.db.update_social_user(user)

    def get_driver(self):
        chrome_options = Options()
        # chrome_options.binary_location = '/usr/local/bin/chromedriver'
        chrome_options.add_argument('--headless')
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
        chrome_options.add_argument(f'user-agent={user_agent}')
        chrome_options.add_argument("window-size=1920,1080")
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
        return driver
