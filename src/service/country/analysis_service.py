import time
from src.database.mongodb.mongodb import MongoDB
from src.database.mongodb.klg import MongoDBKLG
from src.constants.time_constants import TimeConstants
from src.constants.token_ignore_constants import ignore_token


class AnalysisService:
    def __init__(self):
        self.db = MongoDB()
        self.klg = MongoDBKLG()

    def token_hold_of_users(self, regional=None, country=None, chain_id='0x38', ignore=True):
        trava_tokens = ['0x044ede67afdb0f56d7451bb3aaccaeab3f772fad', '0x0391be54e72f7e001f6bbc331777710b4f2999ef', '0x21d5fa5ecf2605c0e835ae054af9bba0468e5951', '0x63367300b8f364a437ce440b58d057c0da138a5d', '0x170772a06affc0d375ce90ef59c8ec04c7ebf5d2']
        # Return a sorted dict with format {address: quantity}
        if country:
            cursor_country = self.db.get_social_users_by_country(country=country, projection=['addresses', 'address'])
        elif isinstance(regional, list):
            cursor_country = self.db.get_social_users_by_regionals(regionals=regional,
                                                                   projection=['addresses', 'address'])
        else:
            cursor_country = self.db.get_social_users_by_regional(regional=regional,
                                                                  projection=['addresses', 'address'])
        wallets = []

        for doc in cursor_country:
            address = doc.get('addresses', {}).get("ethereum", None)

            if not address:
                address = doc.get('address')

            if not address:
                continue
            wallets.append(address)

        wallets = [chain_id + '_' + wallet for wallet in wallets]
        cursor = self.klg.get_wallets(keys=wallets)

        tokens = {}
        for c in cursor:
            token_hold = c.get('tokens', {})
            for t in token_hold.keys():
                if t in trava_tokens:
                    continue
                if t not in tokens:
                    tokens[t] = 1
                else:
                    tokens[t] += 1
        tokens = dict(sorted(tokens.items(), key=lambda x: x[1], reverse=True))
        if ignore:
            for token in ignore_token:
                try:
                    tokens.pop(token)
                except:
                    pass

        list_addresses = [chain_id + "_" + address for address in tokens.keys()]

        contracts = self.klg.get_contracts_by_keys(keys=list_addresses, projection=['name', 'idCoingecko', 'address'])
        tokens_name = {doc.get('address'): doc.get('name') for doc in contracts}
        res = {}
        for k, v in tokens.items():
            token_name = tokens_name.get(k)
            res[token_name] = v
        return res

    def value_tx_of_users(self, regional=None, country=None, chain_id='0x38'):

        if country:
            cursor_country = self.db.get_social_users_by_country(country=country)
        else:
            cursor_country = self.db.get_social_users_by_regional(regional=regional)
        res = {}
        for doc in cursor_country:
            personal_total = 0
            count_tx = 0
            list_txs = doc.get(chain_id)
            for tx_hash, tx in list_txs.items():
                if int(tx.get('value', 0)) != 0:
                    personal_total += int(tx.get('value'))
                    count_tx += 1

            if personal_total > 0:
                personal_total = personal_total / 10 ** 18
                median = personal_total / count_tx
                if median < 1:
                    res[doc['_id']] = {'total_value': personal_total, 'quantity': count_tx,
                                       'median': personal_total / count_tx}

        res = dict(sorted(res.items(), key=lambda x: x[1]['median'], reverse=True))
        return res

    def total_txs_during_time(self, regional=None, country=None, period=None, chain_id="0x38"):
        if not period:
            period = TimeConstants.A_YEAR

        timestamp = int(time.time()) - period
        if country:
            cursor_country = self.db.get_social_users_by_country(country=country)
        else:
            cursor_country = self.db.get_social_users_by_regional(regional=regional)

        res = {}

        for doc in cursor_country:
            count = 0
            list_txs = doc.get(chain_id)
            if len(list_txs) >= 5000:
                print("'" + doc.get('_id') + "',")
            for tx_hash, tx in list_txs.items():
                time_tx = tx.get('timestamp')
                if time_tx > timestamp:
                    count += 1

            if 0 < count < 3000:
                res[doc.get('_id')] = count

        return res

    def balance_of_wallets_in_regional(self, regional=None, country=None):
        if country:
            cursor_country = self.db.get_social_users_by_country(country=country, projection=['addresses', 'address'])
        else:
            cursor_country = self.db.get_social_users_by_regional(regional=regional,
                                                                  projection=['addresses', 'address'])

        wallets = []

        for doc in cursor_country:
            address = doc.get('addresses', {}).get("ethereum", None)

            if not address:
                address = doc.get('address')

            if not address:
                continue
            wallets.append(address)

        cursor = self.klg.get_multichain_wallets(addresses=wallets, projection=['balanceInUSD'])
        res = []
        for doc in cursor:
            if 0 < doc.get('balanceInUSD', 0) < 1e6:
                res.append(doc)
        return res

    def get_tags_of_regional(self, regional=None, country=None, chain_id='0x38'):
        if country:
            cursor_country = self.db.get_social_users_by_country(country=country, projection=['addresses', 'address'])
        else:
            cursor_country = self.db.get_social_users_by_regional(regional=regional,
                                                                  projection=['addresses', 'address'])

        wallets = []
        for doc in cursor_country:
            address = doc.get('addresses', {}).get("ethereum", None)

            if not address:
                address = doc.get('address')

            if not address:
                continue
            wallets.append(address)

        res = {}
        wallets = [chain_id + '_' + wallet for wallet in wallets]
        cursor = self.klg.get_wallets(keys=wallets)

        for doc in cursor:
            tags = doc.get('tags', [])

            for tag in tags:
                if tag not in res:
                    res[tag] = 1
                else:
                    res[tag] += 1

        res = dict(sorted(res.items(), key=lambda x: x[1], reverse=True))
        return res
