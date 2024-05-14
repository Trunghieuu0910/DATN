from src.database.mongodb.mongodb import MongoDB
from src.database.mongodb.community_db import MongoDBCommunity
from src.utils.file_utils import write_json_file

class CheckDuplicate:
    def __init__(self):
        self.db = MongoDB()
        self.community_db = MongoDBCommunity()

    def check_duplicate(self):
        for skip in range(100000, 180000, 10000):
            cursor = self.community_db.get_social_quest_n_users(skip=105000)
            ids = []
            for doc in cursor:
                ids.append(doc.get('_id'))
            check_cursor = self.db.get_social_users_by_ids(ids=ids, projection=['twitter'])
            if len(list(check_cursor)) == 0:
                return skip
        return 0

    def write_new_users(self, skip):
        cursor = self.community_db.get_social_quest_n_users(skip=skip)
        res = []
        for doc in cursor:
            print(doc)
            if doc.get('twitter', None):
                res.append(doc)
                print(doc)

        write_json_file('questN.json', res)