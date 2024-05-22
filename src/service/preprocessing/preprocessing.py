import pandas as pd

from src.database.mongodb.mongodb import MongoDB
from src.database.mongodb.etl_db import BlockchainETL
from src.database.mongodb.klg import MongoDBKLG
from src.constants.network_constants import Chain
from src.utils.file_utils import write_flat_dict_to_csv, write_json_file, open_json_file_to_dict, \
    read_csv_to_list_of_dicts, write_error_file
from src.utils.logger_utils import get_logger

logger = get_logger("Preprocessing")

# chain_ids = ['0x38', '0x1', '0xfa', '0x89', '0xa4b1', '0xa', '0xa86a']
chain_ids = ['0x38']


class Preprocessing:
    def __init__(self, chain_id='0x38', file_path='wallet_0x89.csv'):
        self.db = MongoDB()
        self.klg = MongoDBKLG()
        self.etl_db = BlockchainETL()
        self.chain_id = chain_id
        self.file_path = file_path

    def preprocessing(self, chain_id):
        count = 0
        cursor = self.db.get_social_users_by_filter({'flag': {"$in": [0, 1, 3]}, 'chainId': chain_id})
        for doc in cursor:
            logger.info(f"Execute wallet {doc.get('_id')} with number {count}")
            dict_doc = self.get_doc_information(doc)
            if dict_doc.get('total_tx', 0) > 20:
                write_flat_dict_to_csv(file_path=self.file_path, flattened_dict=dict_doc)
            count += 1

    def preprocessing_v2(self):
        count = 0
        processed_wallets = {}
        cursor = self.db.get_social_users_by_filter({'flag': {"$in": [0, 1, 3]}})
        for doc in cursor:
            logger.info(f"Execute wallet {doc.get('_id')} with number {count}")
            address = doc.get('address', None)
            if not address:
                address = doc.get('addresses', {}).get('ethereum')

            if address in processed_wallets:
                continue

            ids = [doc.get('_id'), '0x1_' + address, "0x89_" + address]
            wallets = self.db.get_social_users_by_ids(ids=ids)
            res = self.get_doc_information_v2(cursor=wallets)
            if res.get('total_tx', 0) > 20:
                write_flat_dict_to_csv(file_path=self.file_path, flattened_dict=res)

            processed_wallets[address] = True
            count += 1

    def get_doc_information_v2(self, cursor):
        wallets = []
        for doc in cursor:
            dict_doc = self.get_doc_information(doc)
            wallets.append(dict_doc)

        if len(wallets) == 1:
            return wallets[0]

        res = wallets[0].copy()
        for i in range(1, len(wallets)):
            wallet = wallets[i]
            for k, v in wallet.items():
                if isinstance(v, int) or isinstance(v, float):
                    res[k] += v

        return res

    def get_doc_information(self, doc):
        chain_id = doc.get('chainId')
        value_ne_0 = 0
        total_tx = 0
        total_value = 0
        time_tx = []
        mean = 0
        address = doc.get('address', None)
        if not address:
            address = doc.get('addresses', {}).get('ethereum')

        balance = doc.get('balanceUSD')
        dict_doc = {'address': address, 'balance': balance, 'label': doc.get('regional')}
        txs_list = doc.get(chain_id, {})
        for hash, value in txs_list.items():
            time_tx.append(value.get('timestamp'))
            total_tx += 1
            value_tx = value.get('value')

            if value_tx != 0:
                value_ne_0 += 1
                total_value += int(value_tx) / 10 ** 18 * Chain.token_price.get(chain_id)

        if value_ne_0 > 0:
            mean = total_value / value_ne_0

        dict_doc.update({'total_tx': total_tx, 'mean': mean})
        df = pd.DataFrame({'epoch_time': time_tx})
        df['datetime'] = pd.to_datetime(df['epoch_time'], unit='s')

        df['hour'] = df['datetime'].dt.hour
        hour_counts = df['datetime'].dt.hour.value_counts()
        hour_dict = hour_counts.to_dict()

        for i in range(0, 24):
            if i not in hour_dict:
                hour_dict[i] = 0

        hour_dict = dict(sorted(hour_dict.items(), key=lambda x: x[0]))
        # if total_tx > 0:
        #     for k, v in hour_dict.items():
        #         v = v / total_tx
        #         hour_dict[k] = v
        dict_doc.update(hour_dict)

        return dict_doc

    def get_total_tokens_information(self, chain_id):
        cursor = self.db.get_social_users_by_filter(filter_={"flag": {"$in": [0, 1, 3]}, 'chainId': chain_id}, projection=['newTokens'])
        count_tokens = {}
        for doc in cursor:
            tokens = doc.get('newTokens', [])
            if not tokens:
                tokens = []
            for token in tokens:
                contract_address = token.get('contract_address')
                if contract_address not in count_tokens:
                    count_tokens[contract_address] = 1
                else:
                    count_tokens[contract_address] += 1

        count_tokens = dict(sorted(count_tokens.items(), key=lambda x: x[1], reverse=True))
        tokens = {k: v for k, v in count_tokens.items() if v >= 40}
        # Danh sách các địa chỉ token và số lượng địa chỉ
        write_json_file(f'use_token_{chain_id}.json', tokens)

    def get_token_information(self, chain_id):
        tokens = open_json_file_to_dict(f'token_{chain_id}.json')
        token_addresses = list(tokens.keys())
        len_tokens = len(token_addresses)
        print(len_tokens)
        cursor = self.db.get_social_users_by_filter(filter_={"flag": {"$in": [0, 1, 3]}, 'chainId': chain_id},
                                                    projection=['newTokens', 'address', 'addresses'])
        for doc in cursor:
            address = doc.get('address', None)
            if not address:
                address = doc.get('addresses').get('ethereum')
            wallet_tokens = doc.get('newTokens', [])
            if not wallet_tokens:
                wallet_tokens = []
            wallet_tokens = [doc.get('contract_address') for doc in wallet_tokens]
            wallet = {'address': address}
            for i in range(len_tokens):
                wallet[f'token_{chain_id}_{i}'] = 0

            for token in wallet_tokens:
                if token in token_addresses:
                    address_index = token_addresses.index(token)
                    wallet[f'token_{chain_id}_{address_index}'] = 1

            write_flat_dict_to_csv(f'wallet_token_{chain_id}.csv', wallet)


    def merge_token_and_information(self, chain_id):
        wallets = read_csv_to_list_of_dicts(f'wallet_{chain_id}.csv')
        tokens = read_csv_to_list_of_dicts(f'token_{chain_id}.csv')
        len_wallets = len(wallets)
        count = 0
        for wallet in wallets:
            address = wallet.get('address')
            count += 1
            logger.info(f"Execute {address} {count}/{len_wallets}")
            flag = True
            for token in tokens:
                if token.get('address') == address:
                    wallet.update(token)
                    write_flat_dict_to_csv('train1.csv', wallet)
                    flag = False
                    break

            if flag:
                write_error_file('wallet.txt', address)

    def merge_token_and_information_v2(self):
        wallets = read_csv_to_list_of_dicts('wallet.csv')
        tokens_0x1 = read_csv_to_list_of_dicts('token_0x1.csv')
        tokens_0x38 = read_csv_to_list_of_dicts('token_0x38.csv')
        tokens_0x89 = read_csv_to_list_of_dicts('token_0x89.csv')
        all_tokens = {'0x38': tokens_0x38, '0x89': tokens_0x89, '0x1': tokens_0x1}
        len_wallets = len(wallets)
        count = 0
        for wallet in wallets:
            address = wallet.get('address')
            count += 1
            logger.info(f"Execute {address} {count}/{len_wallets}")

            for chain_id, tokens in all_tokens.items():
                flag = True
                for token in tokens:
                    if token.get('address') == address:
                        wallet.update(token)
                        flag = False
                        break
                if flag:
                    wallet_tokens = {}
                    for i in range(len(tokens)):
                        wallet_tokens[f'token_{chain_id}_{i}'] = 0

                    wallet.update(wallet_tokens)

            write_flat_dict_to_csv('train1.csv', wallet)

