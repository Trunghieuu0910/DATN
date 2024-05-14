from src.database.mongodb.mongodb import MongoDB
from src.service.transaction.TransactionAnalysis import TransactionsAnalysis
from src.service.crawler.crawl_scan import ScanCrawler
from src.service.preprocessing.preprocessing import Preprocessing

crawler = ScanCrawler()
pre = Preprocessing()
db = MongoDB()
transactions = TransactionsAnalysis()


# pre.merge_token_and_information()
# #
# cursor = db.get_social_users_by_filter(filter_={'flag': {"$in": [0, 1]}}, projection=['0x38', 'regional'])
#
# len_txs = {}
# for doc in cursor:
#     txs = doc.get('0x38')
#     len_txs[doc.get('_id')] = len(txs)
#
# len_txs = dict(sorted(len_txs.items(), key=lambda x: x[1], reverse=True))
#
# print("HEHE")
#
from src.service.chart.chart_service import ChartService

chart = ChartService()

chart.chart_by_regional()

# cursor = db.get_social_users_by_filter(filter_={'flag': 3}, projection=['address', '0x38'])
# count = 0
# for doc in cursor:
#     txs = doc.get('0x38', {})
#     if len(txs) < 20:
#         user = {'_id': doc.get('_id'), 'flag': -1}
#         db.update_social_user(user)
#
# print(count)

# transactions.get_tokens_of_wallets_chainbase(cursor)
# crawler.crawl_balance_usd(cursor)