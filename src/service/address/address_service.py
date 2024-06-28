import pandas as pd

from src.constants.network_constants import Chain
from src.service.crawler.crawl_scan import ScanCrawler
from src.utils.file_utils import open_json_file_to_dict


class AddressService:
    def __init__(self):
        # self.db = MongoDB()
        # self.tx_ana = TransactionsAnalysis()
        self.scan_crawler = ScanCrawler()
        self.used_token = open_json_file_to_dict('index.json')

    def get_information_of_address(self, information, address):
        chain_ids = []
        tx_0x38 = information.get('0x38_tx')
        tx_0x1 = information.get('0x1_tx')
        tx_0x89 = information.get('0x89_tx')
        if len(tx_0x38) > 0:
            chain_ids.append(Chain.BSC)
        if len(tx_0x1) > 0:
            chain_ids.append(Chain.ETH)
        if len(tx_0x89) > 0:
            chain_ids.append(Chain.POLYGON)
        time_tx = []
        total_tx = 0
        value_ne_0 = 0
        total_tx_ne_0 = 0
        mean = 0
        means = []

        tx = {'0x1': tx_0x1, '0x38': tx_0x38, '0x89': tx_0x89}
        for chain_id, txs in tx.items():
            for tx in txs:
                if tx.get('from').lower() != address.lower():
                    continue

                total_tx += 1
                time_tx.append(tx.get('time_stamp'))
                value_tx = int(tx.get('value')) / 10 ** 18 * Chain.token_price.get(chain_id)

                if value_tx != 0:
                    means.append(value_tx)
                    value_ne_0 += value_tx
                    total_tx_ne_0 += 1

        if total_tx < 20:
            return None, None

        if value_ne_0 > 0:
            mean = value_ne_0 / total_tx_ne_0

        total_balance = self.scan_crawler.crawl_multichain_balance(address)
        df = pd.DataFrame({'epoch_time': time_tx})
        df['datetime'] = pd.to_datetime(df['epoch_time'], unit='s')

        df['hour'] = df['datetime'].dt.hour
        hour_counts = df['datetime'].dt.hour.value_counts()
        hour_dict = hour_counts.to_dict()

        for i in range(0, 24):
            if i not in hour_dict:
                hour_dict[i] = 0

        hour_dict = dict(sorted(hour_dict.items(), key=lambda x: x[0]))
        res = {'address': address, 'total_tx': total_tx, 'balance': total_balance, 'mean': mean, 'means': means}
        res.update(hour_dict)
        # self.db.update_address(res)
        token = self.token_information_of_wallets(information)
        return res, token

    def token_information_of_wallets(self, information):
        all_tokens = {'0x38': information.get('0x38_token', []), '0x1': information.get('0x1_token'),
                      '0x89': information.get('0x89_token', [])}
        res = {}
        for index in self.used_token.values():
            res[index] = 0
        for chain_id, tokens in all_tokens.items():
            for token in tokens:
                key = chain_id + "_" + token.get('contract_address')
                if key in self.used_token:
                    res[self.used_token[key]] = 1

        return res
