from src.utils.file_utils import open_json_file_to_dict, write_error_file
from src.database.mongodb.mongodb import MongoDB
from src.utils.logger_utils import get_logger
import pycountry
from timezonefinder import TimezoneFinder

# Tạo một bản đồ từ tên quốc gia đến mã alpha-2 của quốc gia
logger = get_logger('MongoDB')

mapping = {"MX": "UTC -06:00", "BR": "UTC -03:00", "CA": "UTC -03:00", "RU": "UTC +03:00", "US": "UTC -05:00",
           "ID": "UTC +07:00", 'AU': "UTC +11:00"}


class DectectTimezone:
    def __init__(self):
        self._db = MongoDB()
        self.time_zones = open_json_file_to_dict('/home/hieunguyen/DHBK/DATN/artifact/city_timezone.json')
        self.countries = {country.name: country.alpha_2 for country in pycountry.countries}
        self.time_zones = open_json_file_to_dict('/home/hieunguyen/DHBK/DATN/text.json')
        self.city_zone = open_json_file_to_dict('/home/hieunguyen/DHBK/DATN/artifact/timezone1.json')

    def preprocessing_json_timezone(self):
        result = {}
        for country in self.time_zones:
            country_code = country.get('Country Code')
            country_name = country.get('Country Name')
            time_zone = country.get('Time Zone')
            offset = country.get('UTC Offset')
            if country_code not in result:
                result[country_code] = {'code': country_code,
                                        'country_name': country_name,
                                        'time_zone': {offset: time_zone}
                                        }
            else:
                old_time_zone = result[country_code].get('time_zone')
                if offset not in old_time_zone:
                    old_time_zone.update({offset: time_zone})
                    result[country_code]['time_zone'] = old_time_zone

        return result

    def write_alpha_2_code(self):
        self.countries['Vietnam'] = self.countries.get('Viet Nam')
        self.countries['Venezuela'] = 'VE'
        self.countries['Russia'] = 'RU'
        self.countries['South Korea'] = 'KR'
        self.countries['Turkey'] = 'TR'
        self.countries['Taiwan'] = 'TW'
        self.countries['Iran'] = 'IR'
        self.countries['Laos'] = 'LA'
        self.countries['Palestinian Territories'] = 'PS'
        self.countries['Tanzania'] = 'TZ'
        self.countries['Moldova'] = 'MD'
        self.countries['Macedonia'] = 'MK'
        self.countries['Syria'] = 'SY'
        self.countries['Dominican'] = 'DM'
        self.countries['Bolivia'] = 'BO'
        self.countries['Myanmar (Burma)'] = 'MM'
        self.countries['England'] = 'GB'
        self.countries['East Timor'] = 'TL'
        self.countries['The United Kingdom'] = "GB"
        cursor = self._db.get_social_users_with_country(projection=['country'])
        count = 0
        for doc in cursor:
            logger.info(f"Execute doc with id {doc.get('_id')} and count {count}")
            country_name = doc.get('country', {}).get('country', None)
            if not country_name:
                _id = doc.get('country').get('country')
                write_error_file('eror.txt', _id)
                logger.error(f"_id {_id} error")
                continue

            flag = True
            if country_name in self.countries:
                flag = False
                doc['country']['alpha_2'] = self.countries.get(country_name)
                self._db.update_social_user(user=doc)

            if flag:
                _id = doc.get('country').get('country')
                write_error_file('eror.txt', _id)
                logger.error(f"_id {_id} error")

            count += 1

    def write_timezone(self):
        cursor = self._db.get_social_users_with_country(projection=['country'])
        count = 0
        rest = {}
        for doc in cursor:
            logger.info(f"Execute doc with id {doc.get('_id')} and count {count}")
            country_alpha_2 = doc.get('country').get('alpha_2')

            value = self.time_zones.get(country_alpha_2)
            if not value:
                print(doc.get("_id"))
                break
            if len(value.get('time_zone', {})) == 1:
                dict_timezone = value.get('time_zone', {})
                timezone = list(dict_timezone.keys())[0]
                doc['timezone'] = timezone
                self._db.update_social_user(doc)
            else:
                country_name = doc.get('country').get('country')
                city_name = doc.get('country').get('city_name')
                if country_name == city_name:
                    doc['timezone'] = mapping.get(country_alpha_2)
                    self._db.update_social_user(doc)
                else:
                    coordinate = doc.get('country').get('coordinates', {})
                    latitude = coordinate.get('latitude', None)
                    longitude = coordinate.get('longitude', None)

                    if latitude and longitude:
                        try:
                            if isinstance(latitude, list):
                                la = latitude[1]
                                long = latitude[0]
                                timezone_str = get_timezone(la, long)
                                offset = self.city_zone.get(timezone_str, None)

                            else:
                                timezone_str = get_timezone(latitude, longitude)
                                offset = self.city_zone.get(timezone_str, None)

                            if offset:
                                doc['timezone'] = offset
                                self._db.update_social_user(doc)
                            else:
                                write_error_file('error.txt', timezone_str)
                        except:
                            print(doc.get('_id'))

            count += 1
        print("DONE")

    def retry_write_timezone(self):
        cursor = self._db.get_social_users_without_timezone(projection=['country'])

        for doc in cursor:
            doc_country = doc.get('country')
            country_name = doc_country.get('country')
            city_name = doc_country.get('city_name')
            alpha_2 = doc_country.get('alpha_2')
            if country_name == city_name:
                offset = mapping.get(alpha_2, None)
                if offset:
                    doc['timezone'] = mapping.get(alpha_2)
                    self._db.update_social_user(doc)
                else:
                    print(doc_country)
            else:
                try:
                    coordinates = doc_country.get("coordinates")
                    coor = coordinates.get('latitude')
                    la = coor[1]
                    long = coor[0]

                    print(get_timezone(la, long))
                except:
                    pass

    def timezone_to_offset(self):
        res = {}
        for value in self.time_zones.values():
            timezone = value.get('time_zone')
            for k, v in timezone.items():
                if v not in res:
                    res[v] = k

        return res

    def test(self):
        for key in self.time_zones.keys():
            print(key)


def convert_timezone_to_int(timezone):
    offset_parts = timezone.split()
    hours = int(offset_parts[1][1:3])
    minutes = int(offset_parts[1][4:])
    offset_float = hours + minutes / 60
    if timezone.startswith("UTC -"):
        offset_float = -offset_float

    return offset_float


def get_timezone(latitude, longitude):
    tf = TimezoneFinder()
    timezone_str = tf.timezone_at(lng=longitude, lat=latitude)
    return timezone_str
