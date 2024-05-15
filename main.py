from src.database.mongodb.mongodb import MongoDB
from src.service.transaction.TransactionAnalysis import TransactionsAnalysis
from src.utils.file_utils import open_json_file_to_dict
from src.service.crawler.crawl_scan import ScanCrawler

scan = ScanCrawler()

wallets = open_json_file_to_dict('wallets.json')
# scan.crawl_balance_usd(cursor=wallets, start=0, end=32000)
anan = TransactionsAnalysis(api_key='G1JCHT1P7ZWFNB8T2EBZU916VZ35H8R69M')

anan.get_transactions_by_api(wallets, start=0, end=1)

