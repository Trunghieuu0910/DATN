import sys

from pymongo import MongoClient

from src.constants.mongodb_constants import MongoKLGCollections
from src.decorators.time_exe import sync_log_time_exe, TimeExeTag
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

    @classmethod
    def get_projection_statement(cls, projection):
        if projection is None:
            return None

        projection_statements = {}
        for field in projection:
            projection_statements[field] = True

        return projection_statements

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
