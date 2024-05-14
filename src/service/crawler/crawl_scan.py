import time
import json

from bs4 import BeautifulSoup

from src.service.crawler.base_crawler import BaseCrawler
from src.database.mongodb.mongodb import MongoDB
from src.utils.file_utils import write_error_file
from src.utils.logger_utils import get_logger

logger = get_logger('Scan Crawler')


class ScanCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()
        self.db = MongoDB()

    def crawl_balance_usd(self, cursor=None, start=0, end=0):
        if not cursor:
            cursor = self.db.get_social_users_by_filter(filter_={'flag': {'$in': [0, 1]}}, projection=['address', 'addresses', 'balanceUSD'])

        driver = self.get_driver()

        count = start
        if end == 0 or end > len(cursor):
            end = len(cursor)
        cursor = cursor[start: end]
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
                    balance_token = balance_token + doc.get('balanceUSD', 0)
                    write_error_file('last_count.txt', str(count))
                except Exception as e:
                    logger.exception(e)
                    balance_token = doc.get('balanceUSD', 0)

            user = {"_id": "0x1_" + address, 'balanceUSD': balance_token}
            print(user)
            # self.db.update_social_user(user)
