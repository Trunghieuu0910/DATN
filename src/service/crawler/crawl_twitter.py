import time
import json

from bs4 import BeautifulSoup

from src.service.crawler.base_crawler import BaseCrawler
from src.database.mongodb.mongodb import MongoDB
from src.utils.logger_utils import get_logger

logger = get_logger('Twitter Crawler')


class TwitterCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()
        self.db = MongoDB()

    def crawl_twitter_users(self, skip):
        cursor, len_cursor = self.db.get_users_with_twitter(skip=skip, projection=['twitter'])
        count = skip
        driver = self.get_driver()
        for user in cursor:
            id_ = user.get('twitter')
            twitter_url = 'https://twitter.com/'
            twitter_user = twitter_url + id_

            driver.get(twitter_user)
            time.sleep(0.85)
            html = driver.page_source

            if 'Account suspended' in html or 'X suspends accounts' in html or "This account doesn’t exist" in html:
                print(user.get("_id"))
                logger.info(f"Execute at {count}/{len_cursor}")
                count += 1
                user['information'] = ""
                self.db.update_user(user)
                continue
            if "Something went wrong" in html:
                print(id_)
                driver.quit()
                time.sleep(5)
                driver = self.get_driver()
                driver.get(twitter_user)
                time.sleep(0.85)
                html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')

            script_tag = soup.find('script', {'type': 'application/ld+json', 'data-testid': 'UserProfileSchema-test'})

            logger.info(f"Execute at {count}/{len_cursor}")
            count += 1
            print(script_tag)

            if script_tag:
                json_data = script_tag.string
                json_data = json.loads(json_data)
                if json_data:
                    try:
                        author = json_data.get("author")
                        infor = {"additionalName": author.get("additionalName"),
                                 "description": author.get("description"),
                                 "givenName": author.get("givenName"),
                                 "homeLocation": author.get("homeLocation")}

                        user['information'] = infor
                        self.db.update_user(user)
                    except Exception as e:
                        logger.exception(e)
                        with open("/home/hieunguyen/test/test/error.txt", "a") as error:
                            error.write(twitter_user)
                            error.write("\n")
