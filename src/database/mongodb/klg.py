import sys
import time

import pymongo
from pymongo import MongoClient, UpdateOne
from pymongo.cursor import Cursor

from src.constants.mongodb_constants import MongoKLGCollections
from src.constants.network_constants import DEFAULT_CREDIT_SCORE, Chain
from src.constants.search_constants import SearchConstants
from src.constants.time_constants import TimeConstants
from src.decorators.time_exe import sync_log_time_exe, TimeExeTag
from src.utils.list_dict_utils import sort_log
from src.utils.logger_utils import get_logger
from config import MongoDBKLGConfig

logger = get_logger('MongoDB')


class MongoDBKLG:
    def __init__(self, connection_url=None, database=MongoDBKLGConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBKLGConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        try:
            self.connection = MongoClient(connection_url)
            self.mongo_db = self.connection[database]
        except Exception as e:
            logger.exception(f"Failed to connect to ArangoDB: {connection_url}: {e}")
            sys.exit(1)

        self._wallets_col = self.mongo_db[MongoKLGCollections.wallets]
        self._multichain_wallets_col = self.mongo_db[MongoKLGCollections.multichain_wallets]
        self._projects_col = self.mongo_db[MongoKLGCollections.projects]
        self._smart_contracts_col = self.mongo_db[MongoKLGCollections.smart_contracts]
        self._profiles_col = self.mongo_db[MongoKLGCollections.profiles]
        self._users_col = self.mongo_db[MongoKLGCollections.users]
        self._notifications_col = self.mongo_db[MongoKLGCollections.notifications]

        self._multichain_wallets_credit_scores_col = self.mongo_db[MongoKLGCollections.multichain_wallets_credit_scores]

        self._liquidates_col = self.mongo_db[MongoKLGCollections.liquidates]

        self._abi_col = self.mongo_db[MongoKLGCollections.abi]
        self._configs_col = self.mongo_db[MongoKLGCollections.configs]
        self._is_part_ofs_col = self.mongo_db[MongoKLGCollections.is_part_ofs]
        self._followers_col = self.mongo_db[MongoKLGCollections.followers]

    #######################
    #       Index         #
    #######################

    def _create_index(self):
        ...

    #######################
    #      Project        #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_projects_by_type(self, type_=None, sort_by=None, reverse=False, skip=None, limit=None, chain=None,
                             category=None, last_updated_at=None, projection=None, ignore_ids=None):
        filter_statement = {}
        if type_ is not None:
            filter_statement.update({'sources': type_})
        if chain is not None:
            filter_statement.update({'deployedChains': chain})
        if category is not None:
            filter_statement.update({'category': category})
        if ignore_ids:
            filter_statement.update({'_id': {'$nin': ignore_ids}})
        if last_updated_at is not None:
            if type_ is not None:
                filter_statement.update({f'lastUpdated.{type_}': {'$gt': last_updated_at}})
            else:
                filter_statement.update({'lastUpdatedAt': {'$gt': last_updated_at}})
        projection_statement = self.get_projection_statement(projection)

        cursor = self._projects_col.find(filter_statement, projection=projection_statement, batch_size=1000)
        cursor = self.get_pagination_statement(cursor, sort_by, reverse, skip, limit)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def count_projects_by_type(self, type_=None, chain=None, category=None, last_updated_at=None):
        filter_statement = {}
        if type_ is not None:
            filter_statement.update({'sources': type_})
        if chain is not None:
            filter_statement.update({'deployedChains': chain})
        if category is not None:
            filter_statement.update({'category': category})
        if last_updated_at is not None:
            if type_ is not None:
                filter_statement.update({f'lastUpdated.{type_}': {'$gt': last_updated_at}})
            else:
                filter_statement.update({'lastUpdatedAt': {'$gt': last_updated_at}})

        return self._projects_col.count_documents(filter_statement)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_project(self, project_id, projection=None):
        projection_statement = self.get_projection_statement(projection)
        return self._projects_col.find_one({'_id': project_id}, projection=projection_statement)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_projects(self, project_ids, projection=None):
        projection_statement = self.get_projection_statement(projection)
        return self._projects_col.find({'_id': {'$in': project_ids}}, projection=projection_statement)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_project_by_address(self, address):
        aggregate_statement = [
            {
                '$match': {'address': address}
            },
            {
                "$lookup": {
                    'from': 'projects',
                    'localField': 'project',
                    'foreignField': '_id',
                    'as': 'projects'
                }
            }
        ]
        cursor = self._smart_contracts_col.aggregate(aggregate_statement)
        return list(cursor)

    def get_top_nfts(self, chain_id=None, projection=None, limit=None, last_updated_at=None):
        filter_statement = {'sources': 'nft', 'nftAddresses': {'$exists': True}}
        if chain_id is not None:
            filter_statement.update({'deployedChains': chain_id})

        if last_updated_at is not None:
            filter_statement.update({'lastUpdated.nft': {'$gt': last_updated_at}})

        cursor = self._projects_col.find(filter_statement, projection=projection)
        cursor = cursor.sort('volume', pymongo.DESCENDING)
        if limit:
            cursor = cursor.limit(limit)

        return cursor

    def update_projects(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._projects_col.bulk_write(bulk_operations)

    #######################
    #      Contract       #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_contracts_by_type(self, type_=None, chain_id=None, sort_by=None, reverse=False, skip=None, limit=None,
                              projection=None, last_updated_at=None, batch_size=10000):
        filter_statement = {}
        if type_ is not None:
            if type_ == 'token':
                filter_statement.update({'idCoingecko': {'$exists': True}})
            else:
                filter_statement.update({'tags': type_})
        if chain_id is not None:
            filter_statement.update({'chainId': chain_id})
        if last_updated_at is not None:
            if type_ is not None:
                filter_statement.update({f'lastUpdated.{type_}': {'$gt': last_updated_at}})
            else:
                filter_statement.update({'lastUpdatedAt': {'$gt': last_updated_at}})

        projection_statement = self.get_projection_statement(projection)

        cursor = self._smart_contracts_col.find(filter_statement, projection=projection_statement, batch_size=batch_size)
        cursor = self.get_pagination_statement(cursor, sort_by, reverse, skip, limit)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def count_contracts_by_type(self, type_=None, last_updated_at=None):
        filter_statement = {}
        if type_ is not None:
            if type_ == 'token':
                filter_statement.update({'idCoingecko': {'$exists': True}})
            else:
                filter_statement.update({'tags': type_})
        if last_updated_at is not None:
            if type_ is not None:
                filter_statement.update({f'lastUpdated.{type_}': {'$gt': last_updated_at}})
            else:
                filter_statement.update({'lastUpdatedAt': {'$gt': last_updated_at}})

        return self._smart_contracts_col.count_documents(filter_statement)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_contracts_by_keys(self, keys, projection=None):
        filter_statement = {'_id': {'$in': keys}}
        projection_statement = self.get_projection_statement(projection)
        cursor = self._smart_contracts_col.find(filter_statement, projection=projection_statement, batch_size=1000)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_contract_by_key(self, key, projection=None):
        projection_statement = self.get_projection_statement(projection)
        return self._smart_contracts_col.find_one({'_id': key}, projection=projection_statement)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_protocols(self, projection=None):
        filter_statement = {'lendingInfo': {'$exists': True}}
        projection_statement = self.get_projection_statement(projection)
        cursor = self._smart_contracts_col.find(filter_statement, projection=projection_statement, batch_size=1000)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_contracts_by_address(self, address, chains=None, projection=None):
        if chains is not None:
            keys = [f'{chain}_{address}' for chain in chains]
            filter_statement = {'_id': {'$in': keys}}
        else:
            filter_statement = {'address': address}

        projection_statement = self.get_projection_statement(projection)
        cursor = self._smart_contracts_col.find(filter_statement, projection=projection_statement)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_contracts_by_tags(self, tags: list, chain_id: str = None, projection=None):
        try:
            filter_statement = {'tags': {'$in': tags}}
            if chain_id:
                filter_statement['chainId'] = chain_id
            cursor = self._smart_contracts_col.find(filter=filter_statement, projection=projection, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    #######################
    #       Token         #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_tokens(
            self, chain_id=None, category=None, sort_by=None, reverse=False, skip=None, limit=None,
            projection=None, last_updated_at=None, batch_size=10000
    ):
        filter_statement = {'idCoingecko': {'$exists': True}}
        if chain_id is not None:
            filter_statement.update({'chainId': chain_id})
        if last_updated_at is not None:
            filter_statement.update({'lastUpdated.token': {'$gt': last_updated_at}})
        if category is not None:
            filter_statement.update({'categories': category})

        projection_statement = self.get_projection_statement(projection)

        cursor = self._smart_contracts_col.find(
            filter_statement, projection=projection_statement, batch_size=batch_size)
        cursor = self.get_pagination_statement(cursor, sort_by, reverse, skip, limit)
        return cursor

    def get_token_by_id_coingecko(self, coin_id, chain_id=None, projection=None):
        filter_statement = {'idCoingecko': coin_id}
        if chain_id is not None:
            filter_statement.update({'chainId': chain_id})
        projection_statement = self.get_projection_statement(projection)
        cursor = self._smart_contracts_col.find(filter_statement, projection=projection_statement)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_tokens_by_ids(self, token_ids, projection=None):
        filter_statement = {'idCoingecko': {'$in': token_ids}}
        projection_statement = self.get_projection_statement(projection)
        cursor = self._smart_contracts_col.find(filter_statement, projection=projection_statement)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_tokens_by_list_ids(self, token_ids, projection=None):
        filter_statement = {'idCoingecko': {'$in': token_ids}}
        projection_statement = self.get_projection_statement(projection)
        cursor = self._smart_contracts_col.find(filter_statement, projection=projection_statement).sort([("tokenHealth", -1), ("marketCap", -1)])
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_address_token_links(self, chain_id, address):
        filter_statement = {'_id': f'{chain_id}_{address}'}
        doc = self._smart_contracts_col.find_one(filter_statement, projection=['idCoingecko'])
        if doc and doc.get('idCoingecko'):
            return {'coingecko': f'https://www.coingecko.com/en/coins/{doc["idCoingecko"]}'}
        return {}

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_tokens_by_address(self, address, projection=None):
        projection_statement = self.get_projection_statement(projection)
        filter_statement = {'address': address}
        cursor = self._smart_contracts_col.find(filter_statement, projection=projection_statement)
        return list(cursor)

    def get_price(self, token, chain_id):
        key = f"{chain_id}_{token}"
        filter_statement = {
            "_id": key
        }
        projection_statement = {"price": 1}
        cursor = self._smart_contracts_col.find_one(
            filter=filter_statement, projection=projection_statement, batch_size=1000)
        if cursor:
            return cursor.get("price") or 0

        return 0

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_tokens_by_keys(self, token_keys, projection=None):
        filter_statement = {'_id': {'$in': token_keys}}
        projection_statement = self.get_projection_statement(projection)
        cursor = self._smart_contracts_col.find(filter_statement, projection=projection_statement)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_tokens_by_symbols(self, symbols, projection=None):
        filter_statement = {
            'idCoingecko': {'$exists': True},
            'symbol': {'$in': symbols}
        }
        projection_statement = self.get_projection_statement(projection)
        cursor = self._smart_contracts_col.find(filter_statement, projection=projection_statement)
        return cursor

    #######################
    #      Wallet         #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallet_by_address(self, address, chain_id=None, projection=None):
        filter_statement = {}
        if chain_id:
            filter_statement.update({'_id': f'{chain_id}_{address}'})
        else:
            filter_statement.update({'address': address})
        projection_statement = self.get_projection_statement(projection)
        cursor = self._wallets_col.find(filter_statement, projection=projection_statement)
        return list(cursor)

    def get_elite_wallets(self):
        filter_statement = {'elite': True}
        cursor = self._wallets_col.find(filter_statement, projection=['address'], batch_size=1000)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets(self, keys: list, projection=None):
        filter_statement = {'_id': {'$in': keys}}
        projection_statement = self.get_projection_statement(projection)
        cursor = self._wallets_col.find(filter_statement, projection=projection_statement, batch_size=1000)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallet_by_key(self, key, projection=None):
        filter_statement = {'_id': key}
        projection_statement = self.get_projection_statement(projection)
        cursor = self._wallets_col.find_one(filter_statement, projection=projection_statement)
        return cursor

    def get_wallet_by_filter(self, filter_, projection=None, batch_size=10000):
        cursor = self._wallets_col.find(filter_, projection=projection)
        return cursor.batch_size(batch_size=batch_size)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def count_wallets_by_tags(self, tags, chain=None, last_updated_at=None):
        if len(tags) == 1:
            filter_statement = {'tags': tags[0]}
        else:
            filter_statement = {'tags': {'$in': tags}}

        if last_updated_at is not None:
            filter_statement.update({'lastUpdatedAt': {'$gt': last_updated_at}})

        if not chain:
            counter = self._multichain_wallets_col.count_documents(filter_statement)
        else:
            filter_statement.update({'chainId': chain})
            counter = self._wallets_col.count_documents(filter_statement)

        return counter

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets_with_tags(self, tags, sort_by=None, reverse=False, skip=0, limit=None, chain=None, last_updated_at=None, projection=None):
        if len(tags) == 1:
            filter_statement = {'tags': tags[0]}
        else:
            filter_statement = {'tags': {'$in': tags}}

        if last_updated_at is not None:
            filter_statement.update({'lastUpdatedAt': {'$gt': last_updated_at}})
        projection_statement = self.get_projection_statement(projection)

        if not chain:
            cursor = self._multichain_wallets_col.find(filter_statement, projection=projection_statement, batch_size=1000)
        else:
            filter_statement.update({'chainId': chain})
            cursor = self._wallets_col.find(filter_statement, projection=projection_statement, batch_size=1000)

        cursor = self.get_pagination_statement(cursor, sort_by, reverse, skip, limit)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def update_wallet(self, data, tags=None):
        key = f'{data["chainId"]}_{data["address"]}'

        operator = {'$set': data}
        if tags is not None:
            operator['$addToSet'] = {'tags': {'$each': tags}}

        self._wallets_col.update_one({'_id': key, 'address': data['address']}, operator, upsert=True)

    #######################
    #  Multichain Wallet  #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_multichain_wallet(self, address, projection=None):
        filter_statement = {'_id': address}
        projection_statement = self.get_projection_statement(projection)
        cursor = self._multichain_wallets_col.find_one(filter_statement, projection=projection_statement)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_multichain_wallets(self, addresses, projection=None):
        filter_statement = {'_id': {'$in': addresses}}
        projection_statement = self.get_projection_statement(projection)
        cursor = self._multichain_wallets_col.find(filter_statement, projection=projection_statement, batch_size=1000)
        return cursor

    def count_wallet_by_filter(self, filter_):
        return self._wallets_col.count_documents(filter_)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def update_multichain_wallet(self, data, tags=None):
        key = data["address"]

        operator = {'$set': data}
        if tags is not None:
            operator['$addToSet'] = {'tags': {'$each': tags}}

        self._multichain_wallets_col.update_one({'_id': key, 'address': data['address']}, operator, upsert=True)

    #######################
    #      Profile        #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_profile_by_wallet(self, address, projection=None):
        filter_statement = {f'addresses.{address.lower()}.verified': True}
        projection_statement = self.get_projection_statement(projection)
        cursor = self._profiles_col.find_one(filter_statement, projection=projection_statement)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_profile_by_id(self, profile_id, projection=None):
        filter_statement = {'_id': profile_id}
        projection_statement = self.get_projection_statement(projection)
        cursor = self._profiles_col.find_one(filter_statement, projection=projection_statement)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def update_profile(self, data):
        data['_id'] = data['profileId']
        data['lastUpdatedAt'] = int(time.time())
        self._profiles_col.update_one({'_id': data['profileId']}, {'$set': data}, upsert=True)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def update_profiles(self, data):
        bulk_operations = [UpdateOne({"_id": item["profileId"]}, {"$set": item}, upsert=True) for item in data]
        self._profiles_col.bulk_write(bulk_operations)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def remove_profile(self, profile_id):
        self._profiles_col.delete_one({'_id': profile_id})

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_profiles_by_wallets(self, addresses, projection=None):
        try:
            filter_statement = {'$or': [{f'addresses.{address.lower()}.verified': True} for address in addresses]}
            projection_statement = self.get_projection_statement(projection)
            cursor = self._profiles_col.find(filter_statement, projection=projection_statement)
            return cursor
        except Exception as ex:
            logger.exception(ex)

    #######################
    #       Users         #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_users(self, last_login_at=None):
        try:
            filter_ = {}
            if last_login_at is not None:
                filter_ = {'lastLoginAt': {'$gt': last_login_at}}
            cursor = self._users_col.find(filter_)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def save_user_login(self, address):
        timestamp = int(time.time())
        filter_statement = {'_id': address}
        operator = {
            '$set': {'lastLoginAt': timestamp},
            '$setOnInsert': {'_id': address, 'createdAt': timestamp},
            '$inc': {'numberOfLogins': 1}
        }
        self._users_col.update_one(filter_statement, operator, upsert=True)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def save_user_tweet(self, address, score):
        timestamp = int(time.time())
        filter_statement = {'_id': address}
        operator = {
            '$set': {'lastTweetAt': timestamp, f'tweets.{timestamp}': score},
            '$setOnInsert': {'_id': address, 'createdAt': timestamp},
            '$inc': {'numberOfTweets': 1}
        }
        self._users_col.update_one(filter_statement, operator, upsert=True)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def update_notifications(self, data):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._notifications_col.bulk_write(bulk_operations)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def update_exchange_notifications(self, data):
        bulk_operations = []
        notification_ids = list(data.keys())
        cursor = self.get_notifications_by_ids(ids=notification_ids, projection=['txLarger'])
        list_notifications = list(cursor)
        timestamp = int(time.time() - TimeConstants.A_DAY)
        for notification in list_notifications:
            new_tx = []
            for tx in notification.get('txLarger'):
                if tx.get('timestamp') >= timestamp:
                    new_tx.append(tx)
            if notification.get('_id') in data:
                data[notification.get('_id')]['txLarger'] = data[notification.get('_id')]['txLarger'] + new_tx
        for k, v in data.items():
            bulk_operations.append(UpdateOne({'_id': k},{"$set": v}, upsert=True))

        self._notifications_col.bulk_write(bulk_operations)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_notification(self, notification_id):
        try:
            filter_ = {'_id': notification_id}
            notify = self._notifications_col.find_one(filter_)
            return notify
        except Exception as ex:
            logger.exception(ex)
        return None

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_notifications(self, ids, projection=None, timestamp_threshold=None):
        try:
            filter_ = {'id': {'$in': ids}}
            if timestamp_threshold is not None:
                filter_.update({'timestamp': {'$gte': timestamp_threshold}})
            cursor = self._notifications_col.find(filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_notifications_by_ids(self, ids, projection=None, timestamp_threshold=None):
        try:
            filter_ = {'_id': {'$in': ids}}
            if timestamp_threshold is not None:
                filter_.update({'timestamp': {'$gte': timestamp_threshold}})
            cursor = self._notifications_col.find(filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    #######################
    #       Scores        #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_score_change_logs(self, address: str):
        filter_statement = {'_id': address}
        projection_statement = self.get_projection_statement(['creditScore', 'creditScoreChangeLogs'])
        doc = self._multichain_wallets_credit_scores_col.find_one(filter_statement, projection=projection_statement)
        if not doc:
            return DEFAULT_CREDIT_SCORE, {}

        credit_score = doc.get('creditScore') or DEFAULT_CREDIT_SCORE
        score_logs = sort_log(doc.get('creditScoreChangeLogs'))
        return credit_score, score_logs


    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets_score_change_logs(self, addresses: list):
        filter_statement = {'_id': {'$in': addresses}}
        projection_statement = self.get_projection_statement(['creditScore', 'creditScoreChangeLogs'])
        cursor = self._multichain_wallets_credit_scores_col.find(filter_statement, projection=projection_statement)

        data = []
        for doc in cursor:
            data.append({
                'address': doc['_id'],
                'creditScore': doc.get('creditScore') or DEFAULT_CREDIT_SCORE,
                'creditScoreChangeLogs': sort_log(doc.get('creditScoreChangeLogs'))
            })
        return data

    #######################
    #     Liquidates      #
    #######################

    def get_liquidations_by_address(self, address):
        filter_statement = {
            '$or': [
                {'debtBuyerWallet': address},
                {'liquidatedWallet': address}
            ]
        }
        cursor = self._liquidates_col.find(filter_statement)

        results = {}
        for doc in cursor:
            _id = doc['_id']
            chain_id, *_ = _id.split('_')
            doc['chainId'] = chain_id
            results[_id] = doc
        return results

    def get_liquidations_multiple_addresses(self, address_keys):
        addresses = set()
        for key in address_keys:
            _, address = key.split('_')
            addresses.add(address)
        addresses = list(addresses)

        filter_statement = {
            '$or': [
                {'debtBuyerWallet': {'$in': addresses}},
                {'liquidatedWallet': {'$in': addresses}}
            ]
        }
        cursor = self._liquidates_col.find(filter_statement)
        return {doc['_id']: doc for doc in cursor}

    #######################
    #        ABI          #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_abi(self, abi_names):
        filter_statement = {'_id': {'$in': abi_names}}
        cursor = self._abi_col.find(filter_statement, batch_size=1000)
        return cursor

    #######################
    #       Configs       #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_config(self, key):
        config = self._configs_col.find_one({'_id': key})
        return config

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_configs(self, keys, projection=None, filter_=None):
        cursor = self._configs_col.find({}, projection=projection)
        if keys:
            cursor = self._configs_col.find({'_id': {'$in': keys}}, projection=projection)

        if filter_:
            cursor = self._configs_col.find(filter_, projection=projection)
        return cursor

    def get_score_histogram(self):
        config = self.get_config(key='multichain_wallets_scores')
        if not config:
            return {}
        return config['histogram']

    @sync_log_time_exe(tag=TimeExeTag.database)
    def update_config(self, config):
        try:
            self._configs_col.update_one({'_id': config.pop('id')}, {'$set': config}, upsert=True)
        except Exception as ex:
            logger.exception(ex)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def update_configs(self, configs):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in configs]
        self._configs_col.bulk_write(bulk_operations)

    #######################
    #       Common        #
    #######################

    @sync_log_time_exe(TimeExeTag.database)
    def count_documents(self, collection_name):
        return self.mongo_db[collection_name].count_documents(filter={})

    @sync_log_time_exe(TimeExeTag.database)
    def count_documents_of_collection(self, collection_name):
        coll_stats = self.mongo_db.command('collStats', collection_name)
        return coll_stats['count']

    @staticmethod
    def get_projection_statement(projection: list = None):
        if projection is None:
            return None

        projection_statements = {}
        for field in projection:
            projection_statements[field] = True

        return projection_statements

    @staticmethod
    def get_pagination_statement(cursor: Cursor, sort_by=None, reverse: bool = False, skip: int = 0, limit: int = None):
        if sort_by is not None:
            cursor = cursor.sort(sort_by, -1 if reverse else 1)
        if skip is not None:
            cursor = cursor.skip(skip)
        if limit is not None:
            cursor = cursor.limit(limit)
        return cursor

    def get_docs(self, collection, keys: list = None, filter_: dict = None, batch_size=1000,
                 projection=None):  # change filter_ to obj
        projection_statement = self.get_projection_statement(projection)

        filter_statement = {}
        if keys:
            filter_statement["_id"] = {"$in": keys}
        if filter_ is not None:
            filter_statement.update(filter_)

        cursor = self.mongo_db[collection].find(
            filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        return cursor

    def get_docs_with_db(self, db, collection, keys: list = None, filter_: dict = None, batch_size=1000,
                         projection=None):  # change filter_ to obj
        projection_statement = self.get_projection_statement(projection)

        filter_statement = {}
        if keys:
            filter_statement["_id"] = {"$in": keys}
        if filter_ is not None:
            filter_statement.update(filter_)

        cursor = self.connection[db][collection].find(
            filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        return cursor

    #######################
    #      Followers      #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_entity_follower(self, key, projection=None):
        filter_statement = {'_id': key}
        projection_statement = self.get_projection_statement(projection)
        cursor = self._followers_col.find_one(filter_statement, projection=projection_statement)
        return cursor

    def get_entities_follower(self, keys: list, projection=None):
        filter_statement = {'_id': {'$in': keys}}
        projection_statement = self.get_projection_statement(projection)
        cursor = self._followers_col.find(filter_statement, projection=projection_statement, batch_size=1000)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def update_follower(self, data):
        self._followers_col.update_one({'_id': data['_id']}, {'$set': data}, upsert=True)
