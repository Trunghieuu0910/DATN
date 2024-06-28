import asyncio
import time

import aiohttp

from src.database.mongodb.mongodb import MongoDB
from src.database.mongodb.etl_db import BlockchainETL
from src.utils.logger_utils import get_logger
from src.utils.file_utils import write_error_file
from src.constants.network_constants import Chain
from moralis import evm_api
import requests

logger = get_logger('Transaction Analysis')


class TransactionsAnalysis:
    def __init__(self, api_key="UXDJAIUUK1SUNGCDTFC2Q9MCYU95JQHTGS"):
        self.bnb_price = 600
        # self.api_key = api_key
        # self._db = MongoDB()
        # self.etl_db = BlockchainETL()

    def count_tx_by_addresses(self, chain_id):
        sum = 0
        cursor = self._db.get_social_users_with_country(projection=['address', 'addresses'])
        for c in cursor:
            address = c.get('addresses', {}).get('ethereum')
            if not address:
                address = c.get('address')

            if address:
                count = self.etl_db.count_documents_by_from_address(chain_id=chain_id, address=address)
                if count > 20:
                    sum += 1
                    print(sum)

    def get_tx_by_addresses(self, chain_id, count=0):
        users = self._db.get_social_users_with_country(projection=['address', 'addresses'], skip=count)
        for c in users:
            address = c.get('addresses', {}).get('ethereum')
            if not address:
                address = c.get('address')

            cursor = self.etl_db.get_transactions_from_address(chain_id=chain_id,
                                                               from_address=address,
                                                               projection=['hash', 'block_timestamp', 'value', 'gas',
                                                                           'gas_price'])

            transactions = {}
            for tx in cursor:
                trans = {tx.get('hash'): {'timestamp': tx.get('block_timestamp'),
                                          'value': tx.get('value'),
                                          'gas': tx.get('gas'),
                                          'gas_price': tx.get('gas_price')
                                          }
                         }

                transactions.update(trans)

            c[chain_id] = transactions
            self._db.update_social_user(c)
            logger.info(f"Execute user {count} with id {c.get('_id')} on {chain_id}")
            count += 1

    def get_transactions_by_api(self, cursor, start=0, end=0):
        count = start
        if end == 0 or end > len(cursor):
            end = len(cursor)
        cursor = cursor[start: end]
        for doc in cursor:
            address = doc.get('address', None)
            if not address:
                address = doc.get('addresses').get('ethereum')

            print(f"Execute address {address} {count}")
            count += 1
            try:
                url = f'https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&page=1&offset=10000&sort=asc&apikey={self.api_key}'
                response = requests.get(url)
                data = dict(response.json())
                transactions = data.get('result')
                user = {'0x1': {}, '_id': '0x1_' + address, 'chainId': '0x1', 'regional': doc.get('regional'),
                        'address': address}
                for tx in transactions:
                    if tx.get('from') == address:
                        user['0x1'][tx.get('hash')] = {'timestamp': tx.get('timeStamp'), 'value': tx.get('value', 0),
                                                       'gas': tx.get('gas', 0), 'gas_price': doc.get('gasPrice', 0)}

                self._db.update_social_user(user)
                print(user)
            except Exception as e:
                print(f"ERROR address {address} {count}")
                logger.exception(e)

    def get_balance_by_api(self, cursor, start=0, end=0):
        count = 0
        sub_count = 0
        list_address = ""
        _ids = {}
        for doc in cursor:
            address = doc.get('address', None)
            if not address:
                address = doc.get('addresses').get('ethereum')

            _ids[address] = doc.get('_id')
            print(f"Execute address {address} {count}")
            count += 1
            list_address += address + ","
            sub_count += 1

            if sub_count == 20:
                list_address = list_address[:len(list_address) - 1]
                url = f"https://api.polygonscan.com/api?module=account&action=balancemulti&address={list_address}&tag=latest&apikey={self.api_key}"
                response = requests.get(url)
                data = dict(response.json())
                res = data.get('result')
                for wallet in res:
                    wallet_address = wallet.get('account')
                    balance = wallet.get('balance', 0)
                    balance = int(balance) / 10 ** 18 * 0.68
                    _id = "0x89_" + wallet_address
                    user = {"_id": _id, 'balanceUSD': balance}
                    print(user)
                    self._db.update_social_user(user)
                sub_count = 0
                list_address = ""

        if list_address:
            list_address = list_address[:len(list_address) - 1]
            url = f"https://api.polygonscan.com/api?module=account&action=balancemulti&address={list_address}&tag=latest&apikey={self.api_key}"
            response = requests.get(url)
            data = dict(response.json())
            res = data.get('result')
            for wallet in res:
                wallet_address = wallet.get('account')
                balance = wallet.get('balance', 0)
                balance = int(balance) / 10 ** 18 * 0.68
                _id = "0x89_" + wallet_address
                user = {"_id": _id, 'balanceUSD': balance}
                print(user)
                self._db.update_social_user(user)

    def get_tokens_of_wallets_moralis(self, cursor):
        count = 0
        for doc in cursor:
            # address = doc.get('address', None)
            # if not address:
            #     address = doc.get('addresses').get('ethereum')

            address = doc
            print(f"Execute address {address} {count}")
            count += 1

            params = {
                "chain": "bsc",
                "address": "0xc0c585a420e28236ea041369a643ec0032ec60c9"
            }

            result = evm_api.token.get_wallet_token_balances(
                api_key=self.api_key,
                params=params,
            )

            print(result)

    def get_tokens_of_wallets_chainbase(self, cursor, start=0, end=0):
        count = 0
        #Execute address 0x065ed96171f05b889508c16036dd7c2494c751cc 35849 on 0x89

        for doc in cursor:
            page = 1
            tokens = doc.get('newTokens', [])
            address = doc.get('address', None)
            chain_id = doc.get('chainId')
            if not address:
                address = doc.get('addresses').get('ethereum')

            print(f"Execute address {address} {count} on {chain_id}")
            count += 1

            data = self.get_data(address, page, chain_id)
            if not data:
                continue
            try:
                tokens = tokens + data
                while len(data) == 100:
                    page += 1
                    data = self.get_data(address, page, chain_id)
                    tokens = tokens + data
                user = {"_id": doc.get('_id'), "newTokens": tokens, 'address': address}
                print(len(tokens))
                self._db.update_social_user(user)
            except:
                print("Continue")
                key = chain_id + "_" + address
                write_error_file('polygon1.txt', key)

    def get_data(self, address, page, chain_id):
        if chain_id == '0x1':
            chain = 1
        elif chain_id == '0x89':
            chain = 137

        url = f"https://api.chainbase.online/v1/account/tokens?chain_id={chain}&address={address}&limit=100&page={page}"
        headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

        response = requests.get(url, headers=headers)
        res = response.json()
        if res.get('message') == "ok":
            data = res.get('data')
            return data
        else:
            key = chain_id + "_" + address
            print("Write error")
            write_error_file('polygon1.txt', key)
            return []

    def get_info_all_chain_of_address(self, results, address):
        logger.info("Execute crawl token")
        results['0x1_token'] = []
        urls = {'0x1': f"https://api.chainbase.online/v1/account/tokens?chain_id=1&address={address}&limit=100&page=1"}
        if not results.get('0x89_token'):
            results['0x89_token'] = []
        if not results.get('0x38_token'):
            results['0x38_token'] = []
        if len(results.get('0x89_token', [])) == 100:
            urls['0x89'] = f"https://api.chainbase.online/v1/account/tokens?chain_id=137&address={address}&limit=100&page=2"
        if len(results.get('0x38_token', [])) == 100:
            urls['0x38'] = f"https://api.chainbase.online/v1/account/tokens?chain_id=56&address={address}&limit=100&page=2"

        headers = {
            "accept": "application/json",
            "x-api-key": "2g5g889KGLeY7I3k8lSqQ98Aq5l"
        }
        copy_urls = urls.copy()
        while copy_urls:
            for chain_id, url in urls.items():
                response = requests.get(url, headers=headers)
                res = response.json()
                if res.get('message') == "ok":
                    data = res.get('data', [])
                    if not data:
                        data = []
                    copy_urls.pop(chain_id)
                    if len(data) == 100:
                        index = url.rfind('=')
                        page = int(url[index+1:]) + 1
                        url = url[:index+1] + str(page)
                        copy_urls[chain_id] = url
                    results[f'{chain_id}_token'] += data
                else:
                    copy_urls.pop(chain_id)

            urls = copy_urls.copy()

        return results

    async def fetch(self, session, url):
        if 'chainbase' in url:
            chain_id = ''
            if 'chain_id=137' in url:
                chain_id = Chain.POLYGON
            elif 'chain_id=1' in url:
                chain_id = Chain.ETH
            elif 'chain_id=56' in url:
                chain_id = Chain.BSC
            headers = {
                "accept": "application/json",
                "x-api-key": "2g5g889KGLeY7I3k8lSqQ98Aq5l"
            }
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    res = await response.json()
                else:
                    res = {}
                data = []
                if res.get('message') == "ok":
                    data = res.get('data')
                    if not data:
                        data = []
                return {f'{chain_id}_token': data}
        else:
            async with session.get(url) as response:
                data = await response.json()
                chain_id = ''
                if 'etherscan' in url:
                    chain_id = Chain.ETH
                elif 'bscscan' in url:
                    chain_id = Chain.BSC
                elif 'polygon' in url:
                    chain_id = Chain.POLYGON

                results = data.get('result')
                if isinstance(results, str):
                    results = int(results) / 10 ** 18
                    results = results * Chain.token_price.get(chain_id)
                    return {f'{chain_id}_balance': results}
                res = []
                for result in results:
                    res.append({'time_stamp': result.get('timeStamp'),
                                'hash': result.get('hash'),
                                'from': result.get('from'),
                                'to': result.get('to'),
                                'value': result.get('value')})

                return {f"{chain_id}_tx": res}

    async def get_tx_each_chain_of_address(self, address):
        logger.info("Execute crawl Transactions")
        urls = [
            f"https://api.chainbase.online/v1/account/tokens?chain_id=137&address={address}&limit=100&page=1",
            f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&page=1&offset=10000&sort=asc&apikey={Chain.api_key.get(Chain.ETH)}",
            f"https://api.bscscan.com/api?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&page=1&offset=10000&sort=asc&apikey={Chain.api_key.get(Chain.BSC)}",
            f"https://api.polygonscan.com/api?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&page=1&offset=10000&sort=asc&apikey={Chain.api_key.get(Chain.POLYGON)}",
            f"https://api.chainbase.online/v1/account/tokens?chain_id=56&address={address}&limit=100&page=1",

        ]
        async with aiohttp.ClientSession() as session:
            tasks = []
            for url in urls:
                tasks.append(self.fetch(session, url))
            all_response = await asyncio.gather(*tasks)
            res = {}
            for response in all_response:
                res.update(response)

            return res
