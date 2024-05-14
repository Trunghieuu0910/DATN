import time

from src.database.mongodb.mongodb import MongoDB
from src.database.mongodb.etl_db import BlockchainETL
from src.utils.logger_utils import get_logger

from moralis import evm_api
import requests

logger = get_logger('Transaction Analysis')


class TransactionsAnalysis:
    def __init__(self, api_key="UXDJAIUUK1SUNGCDTFC2Q9MCYU95JQHTGS"):
        self.bnb_price = 600
        self.api_key = api_key
        self._db = MongoDB()
        self.etl_db = BlockchainETL()

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

    def get_min_max_gas_price_of_txs(self, chain_id='0x38'):
        cursor = self._db.get_social_users_by_filter(projection=[chain_id])
        res = {}
        for doc in cursor:
            txs = doc.get(chain_id)
            list_hashes = []
            for hash, value in txs.items():
                list_hashes.append(hash)

            tx_cursor = self.etl_db.get_transactions_by_hashes(chain_id=chain_id, hashes=list_hashes)

            for tx in tx_cursor:
                block_number = tx.get('block_number')
                if block_number in res:
                    continue
                tx_in_block_number = self.etl_db.get_transactions_by_block(block_number=block_number, chain_id=chain_id)
                min_gas = 9999999999999999999
                max_gas = 0
                logger.info(f"Execute block {block_number}")
                for tx_in_block in tx_in_block_number:
                    gas_tx = tx_in_block.get('gas_price')
                    gas_tx = int(gas_tx)
                    if gas_tx <= 0:
                        continue
                    if gas_tx < min_gas:
                        min_gas = gas_tx
                    if gas_tx > max_gas:
                        max_gas = gas_tx

                res[block_number] = {'min': min_gas, 'max': max_gas}

            return res

    def get_transactions_by_api(self, cursor):
        count = 0

        for doc in cursor:
            address = doc.get('address', None)
            if not address:
                address = doc.get('addresses').get('ethereum')

            print(f"Execute address {address} {count}")
            count += 1
            if count < 100:
                continue
            try:
                url = f'https://api.bscscan.com/api?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&page=1&offset=10000&sort=asc&apikey={self.api_key}'
                response = requests.get(url)
                data = dict(response.json())
                transactions = data.get('result')
                doc['0x38'] = {}
                for tx in transactions:
                    if tx.get('from') == address:
                        doc['0x38'][tx.get('hash')] = {'timestamp': tx.get('timeStamp'), 'value': tx.get('value', 0),
                                                       'gas': tx.get('gas', 0), 'gas_price': doc.get('gasPrice', 0)}

                self._db.update_social_user(doc)
            except:
                print(f"ERROR address {address} {count}")

    def get_balance_by_api(self, cursor):
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
                url = f"https://api.bscscan.com/api?module=account&action=balancemulti&address={list_address}&tag=latest&apikey={self.api_key}"
                response = requests.get(url)
                data = dict(response.json())
                res = data.get('result')
                for wallet in res:
                    wallet_address = wallet.get('account')
                    balance = wallet.get('balance', 0)
                    balance = int(balance) / 10 ** 18 * 600
                    _id = _ids.get(wallet_address)
                    user = {"_id": _id, 'balanceUSD': balance}
                    print(user)
                    self._db.update_social_user(user)

                sub_count = 0
                list_address = ""

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

    def get_tokens_of_wallets_chainbase(self, cursor):
        count = 0
        for doc in cursor:
            page = 1
            tokens = doc.get('newTokens', [])
            address = doc.get('address', None)
            if not address:
                address = doc.get('addresses').get('ethereum')

            print(f"Execute address {address} {count}")
            count += 1

            data = self.get_data(address, page)
            if not data:
                data = []
            try:
                tokens = tokens + data
                while len(data) == 100:
                    page += 1
                    data = self.get_data(address, page)
                    tokens = tokens + data
                user = {"_id": doc.get('_id'), "newTokens": tokens}
                print(len(tokens))
                self._db.update_social_user(user)
            except:
                print("Continue")

    @classmethod
    def get_data(cls, address, page):

        url = f"https://api.chainbase.online/v1/account/tokens?chain_id=56&address={address}&limit=100&page={page}"
        headers = {
            "accept": "application/json",
            "x-api-key": "2g5g889KGLeY7I3k8lSqQ98Aq5l"
        }

        response = requests.get(url, headers=headers)
        res = response.json()
        data = res.get('data')
        return data
