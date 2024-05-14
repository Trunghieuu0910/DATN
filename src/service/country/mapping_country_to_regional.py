from src.database.mongodb.mongodb import MongoDB
from src.constants.country_regional_constants import CountryConstant
from src.utils.file_utils import write_error_file


class MappingCountryToRegional:
    def __init__(self):
        self.db = MongoDB()
        self.regionals = CountryConstant()

    def mapping_country_to_regional(self):
        cursor = self.db.get_social_users_by_filter(projection=['country'])
        res = {}
        for c in cursor:
            country_name = c.get('country').get('country')
            for regional in self.regionals.countries:
                for k, v in regional.items():
                    if country_name in v:
                        if k not in res:
                            res[k] = 1
                        else:
                            res[k] += 1

        return res

    def country_in_regional(self, regional):
        cursor = self.db.get_social_users_by_filter(projection=['country'])
        res = {}
        regional = self.regionals.get_regional(regional=regional)
        regional = list(regional.values())[0]
        print(regional)
        for c in cursor:
            country_name = c.get('country').get('country')
            if country_name in regional:
                if country_name not in res:
                    res[country_name] = 1
                else:
                    res[country_name] += 1

        return res

    def write_regional(self):
        cursor = self.db.get_social_users_by_filter(filter_={'flag': 3}, projection=['country'])

        for doc in cursor:
            country_name = doc.get('country').get('country')
            for region in self.regionals.get_all_regional():
                regional = self.regionals.get_regional(region)
                for k, v in regional.items():
                    if country_name in v:
                        doc['regional'] = k
                        print(doc)
                        self.db.update_social_user(doc)

    def merge_regional(self):
        cursor = self.db.get_social_users_by_filter({"flag": {"$in": [3]}}, projection=['regional'])

        for doc in cursor:
            reg = doc.get('regional')
            if reg in ['japan_korea', 'china', 'japan', 'south_korea']:
                doc['regional'] = 'jp_kr_cn'
            elif reg in ['northern_europe', 'southern_europe', 'western_europe', 'eastern_europe', 'russia']:
                doc['regional'] = 'europe'
            elif reg in ['indochina', 'malay']:
                doc['regional'] = 'southeast_asia'
            elif reg in ['western_africa']:
                doc['regional'] = 'africa'
            elif reg in ['caribbean_and_central_america']:
                doc['regional'] = 'south_america'
            elif reg in ['india']:
                doc['regional'] = 'southern_asia'
            elif reg in ['canada', 'united_states']:
                doc['regional'] = 'northern_america'

            print(doc)
            self.db.update_social_user(doc)