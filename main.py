from src.database.mongodb.mongodb import MongoDB
from src.service.transaction.TransactionAnalysis import TransactionsAnalysis
from src.utils.file_utils import open_json_file_to_dict
from src.service.crawler.crawl_scan import ScanCrawler
from src.database.mongodb.mongodb import MongoDB

db = MongoDB()
cursor = db.get_social_users_by_filter(filter_={'chainId': '0x89'}, projection=['0x89'])

wallets = []
count = 0
for doc in cursor:
    count += 1
    txs = doc.get('0x89')
    print(f"Execute {doc.get('_id')} {count}")
    if len(txs) < 20:
        print(f"Remove {doc.get('_id')} with len {len(txs)}")
        db.social_users_col.delete_one(filter={'_id': doc.get('_id')})

# anan = TransactionsAnalysis(api_key='G1JCHT1P7ZWFNB8T2EBZU916VZ35H8R69M')
# anan.get_balance_by_api(cursor)

# scan = ScanCrawler()
# scan.crawl_balance_usd(cursor=cursor)