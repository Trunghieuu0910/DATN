from src.database.mongodb.mongodb import MongoDB
from src.service.transaction.TransactionAnalysis import TransactionsAnalysis
from src.utils.file_utils import open_json_file_to_dict
from src.service.crawler.crawl_scan import ScanCrawler
from src.database.mongodb.mongodb import MongoDB

db = MongoDB()
cursor = db.get_social_users_by_filter(filter_={'chainId': '0x89'}, projection=['address'])


anan = TransactionsAnalysis(api_key='2g5g889KGLeY7I3k8lSqQ98Aq5l')
anan.get_tokens_of_wallets_chainbase(cursor)

# scan = ScanCrawler()
# scan.crawl_balance_usd(cursor=cursor)