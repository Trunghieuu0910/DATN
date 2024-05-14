from src.service.country.detect_country_by_city import get_country_from_city, get_country_from_city_v2
import pycountry
import re
import time
from src.service.country.detect_country_by_language import detect_language, detect_language_v2
from src.database.mongodb.mongodb import MongoDB
from src.utils.file_utils import open_json_file_to_dict, write_error_file
from src.utils.logger_utils import get_logger

ignore = ['la']

logger = get_logger('Country Detector')


class DetectCountry:
    def __init__(self):
        self.db = MongoDB()
        self.countries = open_json_file_to_dict('/home/hieunguyen/DHBK/DATN/artifact/countries-mapping-language.json')

    def detect_country(self, cursor):
        count = 0
        len_doc = len(cursor)
        for user in cursor:
            logger.info(f"Execute user {count}/{len_doc}")
            count += 1
            information = user.get('information')
            description = information.get('description')
            home_location = information.get('homeLocation')
            city = home_location.get('name')
            if city:
                flag = True
                while flag:
                    try:
                        country = get_country_from_city_v2(city)
                        flag = False
                        if country:
                            user['country'] = country
                            print(country)
                            user['flag'] = 1
                            self.db.update_user(user)
                            # self.db.update_social_user(user)
                    except Exception as e:
                        file_path = '/home/hieunguyen/DHBK/DATN/artifact/error_count.txt'
                        write_error_file(file_path, str(count))
                        logger.info(city)
                        logger.exception(e)
                        time.sleep(60)
            if description and not user.get('country', None):
                try:
                    description = self.remove_emojis(description)
                    lan, confidence = detect_language(description)
                    if confidence < -100 and lan not in ignore:
                        country = self.get_country_by_language(alpha_2=lan)
                        if country:
                            res = {'country': country, 'confidence': confidence, 'lan': lan}
                            user['country'] = res
                            user['flag'] = 1
                            print(res)
                            self.db.update_user(user)
                            # self.db.update_social_user(user)
                except Exception as e:
                    logger.exception(e)
                    exit()

    def get_country_by_language(self, alpha_2):
        alpha_2 = alpha_2.lower()
        for country in self.countries:
            if country.get('languages') == alpha_2:
                return country.get('name')

        return ''

    def remove_emojis(cls, text):
        # Biểu thức chính quy để lọc các emoji
        emoji_pattern = re.compile("["
                                   u"\U0001F600-\U0001F64F"  # emoticons
                                   u"\U0001F300-\U0001F5FF"  # ký hiệu & biểu tượng
                                   u"\U0001F680-\U0001F6FF"  # biểu tượng transport & màn hình
                                   u"\U0001F1E0-\U0001F1FF"  # ký hiệu quốc gia (flag)
                                   u"\U00002500-\U00002BEF"  # hình dáng bổ trợ
                                   u"\U00002702-\U000027B0"
                                   u"\U00002702-\U000027B0"
                                   u"\U0001f926-\U0001f937"
                                   u"\U00010000-\U0010ffff"
                                   "]+", flags=re.UNICODE)
        return emoji_pattern.sub(r'', text)
