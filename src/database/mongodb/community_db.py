from pymongo import MongoClient

from src.constants.mongodb_constants import MongoDBCommunityCollections
from src.utils.logger_utils import get_logger
from config import MongoDBCommunityConfig

logger = get_logger('MongoDB Community')


class MongoDBCommunity:
    def __init__(self, connection_url=None, database=MongoDBCommunityConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBCommunityConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)
        self.mongo_db = self.connection[database]

        self._social_users = self.mongo_db[MongoDBCommunityCollections.social_users]

    def get_social_zealy_users(self, limit=1000, skip=None):
        filter_ = {'idZealy': {'$exists': True}}
        cursor = self._social_users.find(filter_, projection={'_id'}).skip(skip).limit(limit)

        return cursor

    def get_social_quest_n_users(self, limit=None, skip=None):
        filter_ = {'idQuestN': {'$exists': True}}
        if not limit:
            cursor = self._social_users.find(filter_).skip(skip)
        else:
            cursor = self._social_users.find(filter_).skip(skip).limit(limit)

        return cursor

    def get_social_user_by_id(self, _id):
        filter_ = {'_id': _id}
        cursor = self._social_users.find_one(filter_)

        return cursor

    def get_social_users_by_ids(self, ids):
        filter_ = {'_id': {"$in": ids}}
        cursor = self._social_users.find(filter_, projection={"_id"})

        return cursor

    def count_social_users_by_filter(self, filter_):
        check = self._social_users.count_documents(filter_)
        return check