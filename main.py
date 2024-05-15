from src.database.mongodb.mongodb import MongoDB
from src.service.transaction.TransactionAnalysis import TransactionsAnalysis
from src.utils.file_utils import open_json_file_to_dict
from src.service.crawler.crawl_scan import ScanCrawler

scan = ScanCrawler()

wallets = open_json_file_to_dict('wallets.json')
# scan.crawl_balance_usd(cursor=wallets, start=0, end=32000)
anan = TransactionsAnalysis(api_key='2g5g889KGLeY7I3k8lSqQ98Aq5l')

anan.get_tokens_of_wallets_chainbase(wallets, start=0, end=32000)

