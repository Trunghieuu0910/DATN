from src.database.mongodb.mongodb import MongoDB
from src.service.transaction.TransactionAnalysis import TransactionsAnalysis
from src.utils.file_utils import open_json_file_to_dict
from src.service.crawler.crawl_scan import ScanCrawler
from src.database.mongodb.mongodb import MongoDB

db = MongoDB()
cursor = db.get_social_users_by_filter(filter_={'chainId': '0x1'}, projection=['address'])

anan = TransactionsAnalysis(api_key='FFTC9MFXXAUGGPT4MFD81PIPSY42W7V4VW')
#
anan.get_balance_by_api(cursor)

