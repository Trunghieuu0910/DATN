import time

import redis
from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware

from src.constants.network_constants import ProviderURI
from src.constants.time_constants import TimeConstants
from src.decorators.time_exe import sync_log_time_exe, TimeExeTag
from src.services.blockchain.eth.eth_services import EthService
from src.services.cached.constants import CachedKeys
from src.utils.logger_utils import get_logger
from src.utils.singleton import SingletonMeta

logger = get_logger('Blocks number')


class Blocks(metaclass=SingletonMeta):
    def __init__(self):
        self.eth_services = {}
        for chain_id, provider_uri in ProviderURI.archive_providers.items():
            _w3 = Web3(HTTPProvider(provider_uri))
            _w3.middleware_onion.inject(geth_poa_middleware, layer=0)

            self.eth_services[chain_id] = EthService(_w3)


    @sync_log_time_exe(tag=TimeExeTag.blockchain)
    def block_numbers(self, chain_id, timestamps):
        if not timestamps:
            return []

        return_list = True
        if not isinstance(timestamps, list):
            timestamps = [timestamps]
            return_list = False

        if chain_id not in self.eth_services:
            raise ValueError(f'Chain {chain_id} not support')

        service = self.eth_services[chain_id]

        blocks = {}
        for timestamp in timestamps:
            block_number = service.get_block_for_timestamp(timestamp)

            blocks[timestamp] = block_number

        return blocks if return_list else blocks[timestamps[0]]

    # For another version
    # def clean(self, chain_id):
    #     expired = []
    #     for timestamp, block in self.blocks[chain_id].items():
    #         if block.is_expired:
    #             expired.append(timestamp)
    #
    #     for timestamp in expired:
    #         del self.blocks[chain_id][timestamp]


class BlockNumber:
    def __init__(self, number, timestamp, ex=TimeConstants.A_DAY):
        self.number = number
        self.timestamp = timestamp
        self.expire = int(time.time()) + ex

    def is_expired(self):
        return int(time.time()) > self.expire
