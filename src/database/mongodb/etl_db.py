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
    def get_lending_events(self, chain_id, addresses, start_block, end_block, projection=None):
        filter_ = {
            'wallet': {'$in': addresses},
            'block_number': {
                "$gte": start_block,
                "$lte": end_block
            }
        }

        collection = self._get_collection(chain_id, collection_name='lending_events')
        cursor = collection.find(filter_, projection)
        return cursor

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
        block_data = b_collection.find({'timestamp': {'$lte': timestamp}}, projection=['number', 'timestamp']).sort('number', -1).limit(1)

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

    def get_last_synced_block(self, chain_id):
        collection = self._get_collection(chain_id, MongoEventsCollections.COLLECTORS)

        collector = collection.find_one({'_id': 'streaming_collector'})
        if collector:
            return collector['last_updated_at_block_number']

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_transactions_by_address(self, chain_id, address, is_contract=False, only_from=False, projection=TxConstants.data,
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

    def get_transactions_from_address(self, chain_id, from_address=None, to_address=None, start_block=None, start_block_timestamp=None, limit=None, projection=None):
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

    def get_sort_txs_in_range(self, chain_id, start_timestamp, end_timestamp):
        filter_ = {
            'block_timestamp': {
                "$gte": start_timestamp,
                "$lte": end_timestamp
            }
        }
        collection = self._get_collection(chain_id)
        projection = ["from_address", "to_address"]
        cursor = collection.find(filter_, projection).batch_size(10000)
        return cursor

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

    def get_great_transactions(self, chain_id, start_timestamp, end_timestamp, token_price, threshold):
        blocks = Blocks().block_numbers(chain_id=chain_id, timestamps=[start_timestamp, end_timestamp])
        start_block = blocks.get(start_timestamp)
        end_block = blocks.get(end_timestamp)

        filter_ = {
            'block_number': {
                "$gte": start_block,
                "$lte": end_block
            },
            'value': {'$ne': "0"}
        }
        collection = self._get_collection(chain_id)
        cursor = collection.find(filter_, projection=['hash', 'value', 'block_number']).batch_size(1000)

        txs = []
        for doc in cursor:
            event_type = 'transfer'

            amount = int(doc['value']) / 10 ** 18
            value = amount * token_price
            if value > threshold:
                txs.append({
                    'id': doc['hash'],
                    'chain': chain_id,
                    'type': event_type,
                    'block_number': doc['block_number'],
                    'amount': amount,
                    'value': value
                })

        txs.sort(key=lambda x: x['block_number'])
        return txs

    def get_hot_wallet_transactions(self, chain_id, wallet_address, wallets, token_price, start_timestamp, end_timestamp, threshold):
        blocks = Blocks().block_numbers(chain_id=chain_id, timestamps=[start_timestamp, end_timestamp])
        start_block = blocks.get(start_timestamp)
        end_block = blocks.get(end_timestamp)

        filter_ = {
            'block_number': {
                "$gte": start_block,
                "$lte": end_block
            },
            'value': {'$ne': "0"},
            '$or': [{'from_address': wallet_address}, {'to_address': wallet_address}]
        }
        collection = self._get_collection(chain_id)
        projection = ['hash', 'value', 'block_number', 'from_address', 'to_address']
        cursor = collection.find(filter_, projection=projection).batch_size(1000)

        great_events = []
        inflow = {'amount': 0, 'volume': 0}
        outflow = {'amount': 0, 'volume': 0}
        for doc in cursor:
            if doc['from_address'] == wallet_address:
                if doc['to_address'] in wallets:
                    continue
                else:
                    event_type = 'outflow'
            else:
                if doc['from_address'] in wallets:
                    continue
                else:
                    event_type = 'inflow'

            amount = int(doc['value']) / 10 ** 18
            volume = amount * token_price
            if volume > 0:
                if event_type == 'inflow':
                    inflow['amount'] += amount
                    inflow['volume'] += volume
                else:
                    outflow['amount'] += amount
                    outflow['volume'] += volume

            if volume > threshold:
                if volume > threshold:
                    if event_type == 'outflow':
                        wallet_address = doc['to_address']
                    else:
                        wallet_address = doc['from_address']
                great_events.append({
                    'id': doc['hash'],
                    'chain': chain_id,
                    'type': event_type,
                    'block_number': doc['block_number'],
                    'volume': volume,
                    'token_address': BNB,
                    'amount': amount,
                    'wallet_address': wallet_address
                })

        great_events.sort(key=lambda x: x['block_number'])
        return great_events, inflow, outflow

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_transactions_info(self, address: str, chain_ids: List[str] = None):
        """
        Get the first transaction (return transaction hash),
        and get total number of transactions
        """
        if chain_ids is None:
            chain_ids = Chain.evm_and_tron_chains()

        filter_statement = {"$or": [{"from_address": address}, {"to_address": address}]}

        list_res = []
        for chain_id in chain_ids:
            collections = self._get_collection(chain_id)
            cursor = collections.find(filter_statement, {"hash": 1, "block_timestamp": 1}).sort("block_number", 1)
            docs = list(cursor.limit(1))
            if docs:
                first_tx = docs[0]
                first_tx['chain_id'] = chain_id
                list_res.append(first_tx)

        if len(list_res) > 0:
            res = min(list_res, key=lambda x: x['block_timestamp'])
            # For other version
            # total_trans = 0
            # for doc in list_res:
            #     collection = self._get_collection(doc['chain_id'])
            #     total_trans += collection.count_documents({"from_address": address})

            return {
                'theFirstTransactions': {
                    'chainId': res['chain_id'],
                    'txHash': res['hash']
                },
                # 'totalNumberOfTransactions': total_trans
            }

        return {
            'theFirstTransactions': None,
            # 'totalNumberOfTransactions': 0
        }

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_multi_transactions_info(self, address_keys):
        addresses_by_chain = {}
        for key in address_keys:
            chain_id, address = key.split('_')
            if chain_id not in addresses_by_chain:
                addresses_by_chain[chain_id] = []
            addresses_by_chain[chain_id].append(address)

        list_res = []
        for chain_id, addresses in addresses_by_chain.items():
            filter_statement = {"$or": [{"from_address": {"$in": addresses}}, {"to_address": {"$in": addresses}}]}

            collection = self._get_collection(chain_id)
            cursor = collection.find(filter_statement, {"hash": 1, "block_timestamp": 1}).sort("block_number", 1)
            docs = list(cursor.limit(1))

            if docs:
                first_tx = docs[0]
                first_tx['chain_id'] = chain_id
                list_res.append(first_tx)

        if not list_res:
            return {
                'theFirstTransactions': None,
                # 'totalNumberOfTransactions': 0
            }

        res = min(list_res, key=lambda x: x['block_timestamp'])
        # For other version
        # total_trans = 0
        # for doc in list_res:
        #     chain_id = doc['chain_id']
        #     collection = self._get_collection(chain_id)
        #     total_trans += collection.count_documents({"from_address": {'$in': addresses_by_chain.get(chain_id, [])}})

        return {
            'theFirstTransactions': {
                'chainId': res['chain_id'],
                'txHash': res['hash']
            },
            # 'totalNumberOfTransactions': total_trans
        }

    def get_wallet_info_30days_v2(self, address: str, chain_ids=None):
        address = address.lower()
        now_time = time.time() - 100
        last_time = now_time - TimeConstants.A_DAY*29
        last_time = round_timestamp(last_time)

        if chain_ids is None:
            chain_ids = Chain.evm_and_tron_chains()
        elif not isinstance(chain_ids, list):
            return [chain_ids]

        daily_transaction = {}
        while last_time < now_time:
            next_time = last_time + TimeConstants.A_DAY
            if next_time > now_time:
                next_time = now_time
            for chain_id in chain_ids:
                collections = self._get_collection(chain_id)
                blocks = Blocks().block_numbers(chain_id=chain_id, timestamps=[last_time, next_time])
                filter_1 = {
                    'block_number': {
                        "$gte": blocks[last_time],
                        "$lte": blocks[next_time]
                    },
                    'from_address': address
                }

                filter_2 = {
                    'block_number': {
                        "$gte": blocks[last_time],
                        "$lte": blocks[next_time]
                    },
                    'to_address': address
                }
                count = collections.count_documents(filter=filter_1)
                vol_tran = collections.find(filter=filter_2, projection={'value': 1})
                volume = 0
                res = {}
                for x in vol_tran:
                    volume += (int(x['value']) / 1e18)

                res['count'] = count
                res['volume'] = volume
                daily_transaction[last_time] = res
            last_time += TimeConstants.A_DAY

        return {
            'addresses': address,
            'dailyTransactions': daily_transaction
        }

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_liquidations_by_address(self, address, chain_ids):
        filter_statement = {
            'event_type': 'LIQUIDATE',
            '$or': [
                {'liquidator': address},
                {'user': address}
            ]
        }

        liquidates = []
        for chain_id in chain_ids:
            collection = self._get_collection(chain_id, collection_name='lending_events')
            cursor = collection.find(filter_statement).sort('block_number', -1)

            for doc in cursor:
                doc['chain_id'] = chain_id
                liquidates.append(doc)

        liquidates.sort(key=lambda x: x['block_number'], reverse=True)
        return liquidates

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_liquidations_multiple_addresses(self, address_keys):
        addresses_by_chain = {}
        for key in address_keys:
            chain_id, address = key.split('_')
            if chain_id not in addresses_by_chain:
                addresses_by_chain[chain_id] = []
            addresses_by_chain[chain_id].append(address)

        liquidates = []
        for chain_id, addresses in addresses_by_chain.items():
            filter_statement = {
                'event_type': 'LIQUIDATE',
                '$or': [
                    {'liquidator': {'$in': addresses}},
                    {'user': {'$in': addresses}}
                ]
            }
            collection = self._get_collection(chain_id, collection_name='lending_events')
            cursor = collection.find(filter_statement)
            for doc in cursor:
                doc['chain_id'] = chain_id
                liquidates.append(doc)

        liquidates.sort(key=lambda x: x['block_number'], reverse=True)
        return liquidates


def get_transaction_queries(addresses, from_address=None, to_address=None, receive=True, send=True):
    if not receive and not send:
        return

    if from_address and to_address:
        if ((from_address in addresses) and send) or ((to_address in addresses) and receive):
            _filter = {'from_address': from_address, 'to_address': to_address}
        else:
            return
    elif from_address is not None:
        _filter = address_filter(addresses, filter_address=from_address, is_from=True, receive=receive, send=send)
    elif to_address is not None:
        _filter = address_filter(addresses, filter_address=to_address, is_from=False, receive=receive, send=send)
    else:
        _filter = address_filter(addresses, receive=receive, send=send)

    return _filter


def equal_filter(key, values: List[str]):
    if len(values) == 1:
        _filter = {key: values[0]}
    else:
        _filter = {key: {'$in': values}}

    return _filter


def address_filter(addresses, filter_address=None, is_from=True, receive=True, send=True) -> Optional[dict]:
    address_key_1 = 'to_address'
    address_key_2 = 'from_address'
    if not is_from:
        address_key_1, address_key_2 = address_key_2, address_key_1
        receive, send = send, receive

    if filter_address is None:
        operators = []
        if receive:
            operators.append(equal_filter(key=address_key_1, values=addresses))
        if send:
            operators.append(equal_filter(key=address_key_2, values=addresses))

        _filter = {
            '$or': operators
        }
    elif filter_address in addresses:
        if send:
            _filter = equal_filter(key=address_key_2, values=[filter_address])
        else:
            _filter = None
    else:
        if receive:
            _filter = {
                **equal_filter(key=address_key_1, values=addresses),
                **equal_filter(key=address_key_2, values=[filter_address])
            }
        else:
            _filter = None

    return _filter
