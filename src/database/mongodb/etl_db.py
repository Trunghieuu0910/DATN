import sys
from typing import List, Optional

import pymongo
from pymongo import MongoClient
import time
from src.constants.mongodb_constants import MongoEventsCollections
from src.constants.network_constants import Chain, BNB
from src.constants.mongodb_events_constants import TxConstants, BlockConstants
from src.decorators.time_exe import sync_log_time_exe, TimeExeTag
from src.models.blocks_mapping_timestamp import Blocks
from src.utils.logger_utils import get_logger
from config import BlockchainETLConfig
from src.utils.time_utils import round_timestamp
from src.constants.time_constants import TimeConstants

logger = get_logger('Blockchain ETL')


class BlockchainETL:
    def __init__(self, connection_url=None):
        if connection_url is None:
            connection_url = BlockchainETLConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        try:
            self.client = MongoClient(connection_url)
        except Exception as ex:
            logger.warning("Failed connecting to MongoDB Main")
            logger.exception(ex)
            sys.exit(1)

        self.bnb_db = self.client[BlockchainETLConfig.BNB_DATABASE]
        self.ethereum_db = self.client[BlockchainETLConfig.ETHEREUM_DATABASE]
        self.fantom_db = self.client[BlockchainETLConfig.FANTOM_DATABASE]
        self.polygon_db = self.client[BlockchainETLConfig.POLYGON_DATABASE]
        self.arbitrum_db = self.client[BlockchainETLConfig.ARBITRUM_DATABASE]
        self.optimism_db = self.client[BlockchainETLConfig.OPTIMISM_DATABASE]
        self.avalanche_db = self.client[BlockchainETLConfig.AVALANCHE_DATABASE]
        self.tron_db = self.client[BlockchainETLConfig.TRON_DATABASE]

    def _get_collection(self, chain_id, collection_name=MongoEventsCollections.TRANSACTIONS):
        if chain_id == Chain.BSC:
            collection = self.bnb_db[collection_name]
        elif chain_id == Chain.ETH:
            collection = self.ethereum_db[collection_name]
        elif chain_id == Chain.FTM:
            collection = self.fantom_db[collection_name]
        elif chain_id == Chain.POLYGON:
            collection = self.polygon_db[collection_name]
        elif chain_id == Chain.ARBITRUM:
            collection = self.arbitrum_db[collection_name]
        elif chain_id == Chain.OPTIMISM:
            collection = self.optimism_db[collection_name]
        elif chain_id == Chain.AVALANCHE:
            collection = self.avalanche_db[collection_name]
        elif chain_id == Chain.TRON:
            collection = self.tron_db[collection_name]
        else:
            raise ValueError(f'Chain {chain_id} is not supported')
        return collection

    @classmethod
    def get_projection_statement(cls, projection: list = None):
        if projection is None:
            return None

        projection_statements = {}
        for field in projection:
            projection_statements[field] = True

        return projection_statements

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_transaction(self, chain_id, transaction_hash):
        _filter = {
            TxConstants.id: f"transaction_{transaction_hash.lower()}"
        }
        _return = {
            TxConstants.id: False,
        }
        for key in TxConstants.data:
            _return[key] = True

        collection = self._get_collection(chain_id)
        return collection.find_one(_filter, _return)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_transactions_by_block(self, chain_id, block_number):
        tx_collection = self._get_collection(chain_id)
        _filter = {
            "block_number": block_number
        }
        _return = {
            TxConstants.id: False,
        }
        for key in TxConstants.data:
            _return[key] = True

        tx_data = tx_collection.find(_filter)
        if tx_data:
            return tx_data

        return None

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_block(self, chain_id, block):
        if not block.isnumeric():
            _filter = {
                "_id": f"block_{block.lower()}"
            }
        else:
            _filter = {
                "number": int(block)
            }

        _return = {
            BlockConstants.id: False
        }
        for key in BlockConstants.data:
            _return[key] = True

        b_collection = self._get_collection(chain_id, MongoEventsCollections.BLOCKS)
        block_data = b_collection.find_one(_filter, _return)
        return block_data

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_blocks_timestamp(self, chain_id, block_numbers):
        b_collection = self._get_collection(chain_id, MongoEventsCollections.BLOCKS)
        block_data = b_collection.find({'number': {'$in': block_numbers}}, projection=['number', 'timestamp'])

        blocks = {}
        for doc in block_data:
            blocks[doc['number']] = doc['timestamp']
        return blocks

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_block_by_timestamp(self, chain_id, timestamp):
        b_collection = self._get_collection(chain_id, MongoEventsCollections.BLOCKS)
        block_data = b_collection.find({'timestamp': {'$lte': timestamp}}, projection=['number', 'timestamp']).sort(
            'number', -1).limit(1)

        blocks = list(block_data)
        if not blocks:
            return None

        return blocks[0]

    def get_transactions_by_hashes(self, chain_id, hashes: list, projection=TxConstants.data):
        _filter = {"_id": {'$in': [f'transaction_{tx_hash}' for tx_hash in hashes]}}
        _return = {
            TxConstants.id: False,
        }
        for key in projection:
            _return[key] = True

        collection = self._get_collection(chain_id)
        cursor = collection.find(_filter, _return)
        return list(cursor)

    def get_transactions_by_pair_address(self, chain_id, from_, to, limit=10):
        _filter = {
            "from_address": from_.lower(),
            "to_address": to.lower(),
        }
        collection = self._get_collection(chain_id)

        _return = {
            TxConstants.id: False,
        }
        for key in TxConstants.data:
            _return[key] = True

        return list(collection.find(_filter, _return).sort(TxConstants.block_number, pymongo.DESCENDING).limit(limit))

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_transactions_by_address(self, chain_id, address, is_contract=False, only_from=False,
                                    projection=TxConstants.data,
                                    to_addresses: List[str] = None, start_block=None, start_block_timestamp=None,
                                    sort_by=TxConstants.block_number, reverse=True, skip=0, limit=None):
        if is_contract:
            _filter = {'to_address': address}
        elif only_from:
            _filter = {'from_address': address}
        else:
            _filter = {'$or': [{'from_address': address}, {'to_address': address}]}

        if to_addresses is not None:
            _filter['to_address'] = {'$in': to_addresses}
        if start_block:
            _filter['block_number'] = {'$gte': start_block}

        if start_block_timestamp:
            _filter['block_timestamp'] = {'$gte': start_block_timestamp}

        collection = self._get_collection(chain_id, MongoEventsCollections.TRANSACTIONS)

        _return = {
            TxConstants.id: False,
        }
        for key in projection:
            _return[key] = True

        direction = pymongo.DESCENDING if reverse else pymongo.ASCENDING
        cursor = collection.find(_filter, _return).sort(sort_by, direction).skip(skip)
        if limit is not None:
            cursor = cursor.limit(limit)
        return list(cursor.batch_size(100))

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_transactions_by_addresses(
            self, chain_id, addresses, projection=TxConstants.data,
            skip=0, limit=None,
            from_address=None, to_address=None,
            receive=True, send=True, success=True
    ):
        _filter = get_transaction_queries(
            addresses, from_address=from_address, to_address=to_address, receive=receive, send=send)
        if _filter is None:
            return []

        if success is True:
            _filter.update({'receipt_status': 1})

        _return = {TxConstants.id: False}
        for key in projection:
            _return[key] = True

        collection = self._get_collection(chain_id, MongoEventsCollections.TRANSACTIONS)
        cursor = collection.find(_filter, _return).sort(TxConstants.block_number, pymongo.DESCENDING).skip(skip)
        if limit is not None:
            cursor = cursor.limit(limit)
        return list(cursor.batch_size(100))

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_transactions_by_native(
            self, chain_id, projection=TxConstants.data,
            skip=0, limit=None,
            from_address=None, to_address=None, success=True
    ):
        if from_address and to_address:
            _filter = {'from_address': from_address, 'to_address': to_address}
        elif from_address is not None:
            _filter = {'from_address': from_address}
        elif to_address is not None:
            _filter = {'to_address': to_address}
        else:
            _filter = {}

        if success is True:
            _filter.update({'receipt_status': 1})

        _return = {TxConstants.id: False}
        for key in projection:
            _return[key] = True

        collection = self._get_collection(chain_id, MongoEventsCollections.TRANSACTIONS)
        cursor = collection.find(_filter, _return).sort(TxConstants.block_number, pymongo.DESCENDING).skip(skip)
        if limit is not None:
            cursor = cursor.limit(limit)
        return list(cursor.batch_size(100))

    @sync_log_time_exe(tag=TimeExeTag.database)
    def count_documents_by_address(self, chain_id, address, is_contract=False, start_block=None):
        if is_contract:
            _filter = {'to_address': address}
        else:
            _filter = {'$or': [{'from_address': address}, {'to_address': address}]}

        if start_block:
            _filter['block_number'] = {'$gte': start_block}

        collection = self._get_collection(chain_id, MongoEventsCollections.TRANSACTIONS)
        n_docs = collection.count_documents(_filter)
        return n_docs

    def count_documents_by_from_address(self, chain_id, address, start_block=None):
        filter_ = {'from_address': address}
        if start_block:
            filter_['block_number'] = {'$gte': start_block}

        collection = self._get_collection(chain_id, MongoEventsCollections.TRANSACTIONS)
        n_docs = collection.count_documents(filter_)
        return n_docs

    def get_latest_transactions(self, chain_id, limit=10):
        collection = self.bnb_db[MongoEventsCollections.TRANSACTIONS]
        if chain_id == Chain.ETH:
            collection = self.ethereum_db[MongoEventsCollections.TRANSACTIONS]

        if chain_id == Chain.FTM:
            collection = self.fantom_db[MongoEventsCollections.TRANSACTIONS]
        _return = {
            TxConstants.id: False,
        }
        for key in TxConstants.data:
            _return[key] = True
        cursor = collection.find({}, _return).sort(TxConstants.block_number, -1).limit(limit)
        return list(cursor)

    def get_transactions_from_address(self, chain_id, from_address=None, to_address=None, start_block=None,
                                      start_block_timestamp=None, limit=None, projection=None):
        projection = self.get_projection_statement(projection)
        _filter = {}
        if from_address is not None:
            _filter['from_address'] = from_address
        if to_address is not None:
            _filter['to_address'] = to_address
        if start_block:
            _filter['block_number'] = {'$gte': start_block}
        if start_block_timestamp:
            _filter['block_timestamp'] = {'$gte': start_block_timestamp}

        collection = self._get_collection(chain_id)

        cursor = collection.find(_filter, projection).sort(TxConstants.block_number, pymongo.DESCENDING)
        if limit is not None:
            cursor = cursor.limit(limit)
        return cursor.batch_size(1000)

    def get_transactions_between_block(self, chain_id, start_block, end_block):
        filter_ = {
            'block_number': {
                "$gte": start_block,
                "$lte": end_block
            }
        }
        collection = self._get_collection(chain_id)
        projection = ["from_address", "to_address"]
        cursor = collection.find(filter_, projection).batch_size(10000)
        return cursor
