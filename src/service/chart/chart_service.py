from src.database.mongodb.mongodb import MongoDB
from src.constants.country_regional_constants import CountryConstant
from src.service.country.analysis_service import AnalysisService
import matplotlib.pyplot as plt
from src.utils.time_utils import convert_timestamp, map_epoch_time_to_day


class ChartService:
    def __init__(self):
        self._db = MongoDB()
        self.regional = CountryConstant()
        self.analysis_service = AnalysisService()

    def chart_by_city_of_a_country(self, country_name, top=10):
        cursor = self._db.get_social_users_by_country(country=country_name, projection=['country'])

        res = {}
        for c in cursor:
            city_name = c.get('country').get('city_name')
            if city_name not in res:
                res[city_name] = 1
            else:
                res[city_name] += 1

        data = dict(sorted(res.items(), key=lambda item: item[1], reverse=True)[:top])
        print(data)
        plt.figure(figsize=(20, 12))
        plt.bar(data.keys(), data.values())

        plt.xlabel('Keys')
        plt.ylabel('Values')
        plt.title('Biểu đồ')

        plt.show()

    def chart_by_country(self, start_top=0, end_top=30):
        cursor = self._db.get_social_users_with_country(projection=['country'])

        res = {}
        for c in cursor:
            country_name = c.get('country').get('country')
            if country_name not in res:
                res[country_name] = 1
            else:
                res[country_name] += 1

        data1 = dict(sorted(res.items(), key=lambda item: item[1], reverse=True))
        print(data1)
        data = dict(sorted(res.items(), key=lambda item: item[1], reverse=True)[start_top:end_top])
        plt.bar(data.keys(), data.values())

        plt.xlabel('Country')
        plt.ylabel('Amount')
        plt.title('Top Country')

        plt.show()

    def chart_by_regional(self):
        cursor = self._db.get_social_users_by_filter(filter_={'flag': {"$in": [0, 1, 3]}}, projection=['regional'])

        res = {}
        for doc in cursor:
            if doc.get('regional') not in res:
                res[doc.get('regional')] = 1
            else:
                res[doc.get('regional')] += 1

        data = dict(sorted(res.items(), key=lambda item: item[1], reverse=True))
        plt.barh(list(data.keys()), list(data.values()))

        plt.xlabel('Amount')
        plt.ylabel('Country')
        plt.title('Top Country')

        plt.show()

    def top_country_in_regional(self, regional):
        cursor = self._db.get_social_users_by_regional(regional=regional, projection=['regional', 'country'])
        res = {}

        for c in cursor:
            country_name = c.get('country').get('country')
            if country_name not in res:
                res[country_name] = 1
            else:
                res[country_name] += 1

        country_name = list(res.keys())
        quantities = list(res.values())

        plt.figure(figsize=(8, 8))
        plt.pie(quantities, labels=country_name, autopct='%1.1f%%', startangle=140)
        plt.axis('equal')
        plt.title(f'Distribution of Country in {regional}')
        plt.show()

    def time_tx_in_regional(self, regional, limit=10000):
        cursor = self._db.get_social_users_by_regional(regional=regional, projection=['regional', '0x1', '0x89', '0x38', 'chainId'])
        res = {}
        test = {}
        ccount = 0
        for c in cursor:
            count = 0
            chain_id = c.get('chainId')
            print(chain_id)
            txs = c.get(chain_id)
            for tx, v in txs.items():
                if count > limit:
                    break
                timestamp = v.get('timestamp')
                num = convert_timestamp(int(timestamp), 0)
                if num not in res:
                    res[num] = 1
                else:
                    res[num] += 1
                if 6 <= num <= 14:
                    if c.get("_id") not in test:
                        test[c.get("_id")] = 1
                    else:
                        test[c.get("_id")] += 1
                count += 1
            if c.get('_id') in test:
                test[c.get('_id')] = test[c.get('_id')] / len(txs)

        plt.bar(res.keys(), res.values())
        plt.xlabel('Keys')
        plt.ylabel('Values')
        plt.title(f'Time Transactions {regional}')

        # test = dict(sorted(test.items(), key=lambda x: x[1], reverse=False))
        # for k, v in test.items():
        #     if v >= 0.4:
        #         user = {"_id": k, 'flag': -4}
        #         print(f"{k} {v}")
        #         ccount += 1
        #         # self._db.update_social_user(user)
        print(ccount)

        plt.show()

    def week_tx_in_regional(self, regional, chain_id='0x38', limit=10000):
        cursor = self._db.get_social_users_by_regional(regional=regional, projection=['regional', chain_id])
        res = {}

        for c in cursor:
            count = 0
            txs = c.get(chain_id)
            for tx, v in txs.items():
                if count > limit:
                    break

                timestamp = v.get('timestamp')
                weekday = map_epoch_time_to_day(timestamp)
                if weekday not in res:
                    res[weekday] = 1
                else:
                    res[weekday] += 1
                count += 1

        plt.bar(res.keys(), res.values())
        plt.xlabel('Keys')
        plt.ylabel('Values')
        plt.title(f'Weekdays Transactions {regional}')

        plt.show()

    def timezone_in_regional(self, regional):
        cursor = self._db.get_social_users_by_regional(regional=regional, projection=['timezone'])
        res = {}

        for c in cursor:
            timezone = c.get('timezone')
            if timezone not in res:
                res[timezone] = 1
            else:
                res[timezone] += 1

        plt.bar(res.keys(), res.values())
        plt.xlabel('Keys')
        plt.ylabel('Values')
        plt.title(f'Timezone distribute of {regional}')

        plt.show()

    def transaction_value_of_regional(self, regionals=None, chain_id='0x38'):
        if not regionals:
            regionals = self.regional.get_all_regional()

        res = {}
        for regional in regionals:
            value_tx = self.analysis_service.value_tx_of_users(regional=regional, chain_id=chain_id)
            median = []
            for v in value_tx.values():
                median.append(v.get('median'))

            res[regional] = median

        data = {}
        for k, v in res.items():
            key = k.replace("_", " ")
            data[key] = v
        plt.boxplot(data.values(), labels=data.keys())
        plt.title('Transactions value of regional')
        plt.xlabel('Group')
        plt.xticks(rotation=90)
        plt.ylabel('Value')
        plt.grid(True)
        plt.show()

    def total_txs_during_time(self, regionals=None, period=None, chain_id='0x38'):
        if not regionals:
            regionals = self.regional.get_all_regional()

        res = {}
        for regional in regionals:
            total_time_tx = self.analysis_service.total_txs_during_time(regional=regional,
                                                                        period=period,
                                                                        chain_id=chain_id)

            total = []
            for v in total_time_tx.values():
                total.append(v)

            res[regional] = total

        data = {}
        for k, v in res.items():
            key = k.replace("_", " ")
            data[key] = v
        plt.boxplot(data.values(), labels=data.keys())
        plt.title('Total transaction distribute of regionals')
        plt.xlabel('Group')
        plt.xticks(rotation=90)
        plt.ylabel('Value')
        plt.grid(True)
        plt.show()

    def balance_of_wallets_in_regional(self, regionals=None):
        if not regionals:
            regionals = self.regional.get_all_regional()

        res = {}
        for regional in regionals:
            balances = []
            cursor = self.analysis_service.balance_of_wallets_in_regional(regional=regional)

            for doc in cursor:
                balance_in_usd = doc.get('balanceInUSD', 0)
                balances.append(balance_in_usd)
            res[regional] = balances

        data = {}
        for k, v in res.items():
            key = k.replace("_", " ")
            data[key] = v
        plt.boxplot(data.values(), labels=data.keys())
        plt.title('Balance of wallets in all regionals')
        plt.xlabel('Group')
        plt.xticks(rotation=90)
        plt.ylabel('Value')
        plt.grid(True)
        plt.show()

    def token_of_regional(self, regional, chain_id='0x38', top=10, ignore=True):
        res = self.analysis_service.token_hold_of_users(regional=regional, chain_id=chain_id, ignore=ignore)

        top_items = sorted(res.items(), key=lambda x: x[1], reverse=True)[:top]

        keys = [item[0] for item in top_items]
        values = [item[1] for item in top_items]

        plt.figure(figsize=(8, 8))
        plt.pie(values, labels=keys, autopct='%1.1f%%', startangle=140)
        plt.axis('equal')  # Đảm bảo biểu đồ tròn
        plt.title(f'Top {top} Token of {regional}')
        plt.show()

    def top_time_zone(self):
        cursor = self._db.get_social_users_with_country(projection=['country', 'timezone'])

        res = {}
        for doc in cursor:
            if doc.get('timezone') not in res:
                res[doc.get('timezone')] = 1
            else:
                res[doc.get('timezone')] += 1

        data = dict(sorted(res.items(), key=lambda item: item[1], reverse=True))
        print(data)
        plt.barh(list(data.keys()), list(data.values()))

        plt.xlabel('Timezone')
        plt.ylabel('Amount')
        plt.title('Top Timezone')

        plt.show()

    def chart_by_time_zone(self, timezone, chain_id='0x38', limit=1000):
        cursor = self._db.get_social_users_by_timezone(timezone=timezone, projection=[chain_id, 'timezone', 'country'])
        res = {}
        offset = convert_timezone_to_int(timezone)
        _ids = {}
        for c in cursor:
            count = 0
            txs = c.get(chain_id)
            for tx, v in txs.items():
                if count > limit:
                    break
                timestamp = v.get('timestamp')
                num = convert_timestamp(timestamp, offset)
                if 2 <= num <= 5:
                    if c.get('_id') not in _ids:
                        _ids[c.get("_id")] = 1/len(txs)
                    else:
                        _ids[c.get("_id")] += 1/len(txs)
                if num not in res:
                    res[num] = 1
                else:
                    res[num] += 1
                count += 1

        data = dict(sorted(_ids.items(), key=lambda item: item[1]))
        ids = []
        for k, v in data.items():
            if v > 0.5:
                ids.append(k)
        #
        # for id in ids:
        #     self._db.social_users_col.delete_one({'_id': id})

        cursor = self._db.get_social_users_by_ids(ids=ids, projection=['information'])
        for doc in cursor:
            print(doc.get('_id'))
            print(doc.get('information', {}).get('homeLocation', {}).get('name', {}))

        plt.bar(res.keys(), res.values())
        plt.xlabel('Keys')
        plt.ylabel('Values')
        plt.title('Biểu đồ cột của')

        plt.show()


def convert_timezone_to_int(timezone):
    if timezone == "UTC":
        return 0
    offset_parts = timezone.split()
    hours = int(offset_parts[1][1:3])
    minutes = int(offset_parts[1][4:])
    offset_float = hours + minutes / 60
    if timezone.startswith("UTC -"):
        offset_float = -offset_float

    return offset_float
