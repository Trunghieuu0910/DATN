import sys
from pymongo import MongoClient, UpdateOne
from config import MongoDBConfig
from src.utils.logger_utils import get_logger
from src.constants.mongodb_constants import MongoDBCollections

logger = get_logger('MongoDB')


class MongoDB:
    def __init__(self, connection_url=None, database=MongoDBConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBConfig.CONNECTION_URL
        try:
            self.connection = MongoClient(connection_url)
            self.mongo_db = self.connection[database]
        except Exception as e:
            logger.exception(f"Failed to connect to MongoDB: {connection_url}: {e}")
            sys.exit(1)

        self.social_users_col = self.mongo_db[MongoDBCollections.social_users]
        self.user_col = self.mongo_db[MongoDBCollections.users]
        self.addresses_col = self.mongo_db[MongoDBCollections.addresses]

    @staticmethod
    def get_projection_statement(projection: list = None):
        if projection is None:
            return None

        projection_statements = {}
        for field in projection:
            projection_statements[field] = True

        return projection_statements

    #######################
    #     Social User     #
    #######################

    def get_social_users_by_filter(self, filter_=None, projection=None, skip=None):
        projection = self.get_projection_statement(projection)
        if not filter_:
            filter_ = {}
        if skip:
            cursor = self.social_users_col.find(filter=filter_, projection=projection).skip(skip)
        else:
            cursor = self.social_users_col.find(filter=filter_, projection=projection)

        return cursor

    def update_social_user(self, user):
        try:
            self.social_users_col.update_one({"_id": user.get("_id")}, {"$set": user}, upsert=True)
        except Exception as e:
            logger.exception(e)

    def get_social_users(self, skip=0, limit=10000):
        cursor = self.social_users_col.find(filter={}, projection={"twitter": 1}).skip(skip).limit(limit)
        return cursor

    def get_social_users_with_information(self, skip=0, limit=10000):
        filter_ = {'information': {'$exists': True}}
        cursor = self.social_users_col.find(filter=filter_).skip(skip).limit(limit)

        return cursor

    def get_social_users_with_country(self, skip=0, limit=None, projection=None):
        projection = self.get_projection_statement(projection)
        filter_ = {'country': {'$exists': True}}
        if limit:
            cursor = self.social_users_col.find(filter=filter_, projection=projection).skip(skip).limit(limit)
        else:
            cursor = self.social_users_col.find(filter=filter_, projection=projection).skip(skip)

        return cursor

    def get_regex_social_users(self, key):
        filter_ = {'information.homeLocation.name': {'$regex': key}}

        cursor = self.social_users_col.find(filter_)

        return cursor

    def get_social_users_by_country(self, country, projection=None):
        projection = self.get_projection_statement(projection)
        filter_ = {'country.country': country}

        cursor = self.social_users_col.find(filter=filter_, projection=projection)

        return cursor

    def get_social_users_by_regional(self, regional, projection=None):
        projection = self.get_projection_statement(projection)
        filter_ = {'regional': regional, 'flag': {"$in": [0, 1]} }

        cursor = self.social_users_col.find(filter=filter_, projection=projection)

        return cursor

    def delete_social_users_by_country(self, country):

        cursor = self.social_users_col.delete_many(filter={"country.country": country})
        logger.info(f"Delte {cursor.deleted_count} users")

    def get_social_users_without_timezone(self, projection=None):

        projection = self.get_projection_statement(projection)
        filter_ = {'timezone': {'$exists': False}}
        cursor = self.social_users_col.find(filter=filter_, projection=projection)

        return cursor

    def get_social_users_by_timezone(self, timezone, projection=None):
        projection = self.get_projection_statement(projection=projection)

        filter_ = {"timezone": timezone}

        cursor = self.social_users_col.find(filter=filter_, projection=projection)

        return cursor

    def get_social_users_by_ids(self, ids, projection=None):
        projection = self.get_projection_statement(projection=projection)
        filter_ = {'_id': {"$in": ids}}

        cursor = self.social_users_col.find(filter=filter_, projection=projection)

        return cursor

    def get_social_users_by_regionals(self, regionals, projection=None):
        projection = self.get_projection_statement(projection)

        filter_ = {"regional": {"$in": regionals}}

        cursor = self.social_users_col.find(filter=filter_, projection=projection)

        return cursor

    #######################
    #        User        #
    #######################

    def get_users_with_twitter(self, projection=None, skip=0):
        projection = self.get_projection_statement(projection)

        filter_ = {'twitter': {'$exists': True}, 'duplicate': {'$exists': False}, 'information': {'$exists': False}}

        cursor = self.user_col.find(filter_, projection).skip(skip)
        len_cursor = self.user_col.count_documents(filter_)

        return cursor, len_cursor

    def update_user(self, user):
        try:
            self.user_col.update_one({"_id": user.get("_id")}, {"$set": user}, upsert=True)
        except Exception as e:
            logger.exception(e)

    def get_user_by_filter(self, filter_=None, projection=None):
        projection = self.get_projection_statement(projection)

        if not filter_:
            filter_ = {}
        cursor = self.user_col.find(filter_, projection)

        return cursor

    def update_address(self, document):
        try:
            doc = {}
            for k, v in document.items():
                doc[str(k)] = v

            self.addresses_col.update_one({'_id': doc.get('address')}, {"$set": doc}, upsert=True)
        except Exception as e:
            logger.exception(e)

    def get_address(self, address):
        doc = self.addresses_col.find_one({'_id': address})

        return doc
