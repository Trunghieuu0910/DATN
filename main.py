from src.database.mongodb.mongodb import MongoDB
from src.service.transaction.TransactionAnalysis import TransactionsAnalysis
from src.utils.file_utils import open_json_file_to_dict
from src.service.crawler.crawl_scan import ScanCrawler
from src.database.mongodb.mongodb import MongoDB

db = MongoDB()
cursor = db.get_social_users_by_filter(filter_={'chainId': '0x1'}, projection=['address', 'balanceUSD'])

# anan = TransactionsAnalysis(api_key='G1JCHT1P7ZWFNB8T2EBZU916VZ35H8R69M')
# anan.get_balance_by_api(cursor)

scan = ScanCrawler()
scan.crawl_balance_usd(cursor=cursor)