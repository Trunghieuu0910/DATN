import os

from dotenv import load_dotenv

load_dotenv()


class MongoDBConfig:
    CONNECTION_URL = os.getenv("MONGODB_CONNECTION_URL", 'mongodb://localhost:27017/')
    DATABASE = os.getenv('MONGODB_DATABASE', 'cdp_database')


class BlockchainETLConfig:
    CONNECTION_URL = os.environ.get('BLOCKCHAIN_ETL_CONNECTION_URL',
                                    "")
    TEST_CONNECTION_URL = os.environ.get('TEST_BLOCKCHAIN_ETL_CONNECTION_URL')

    BNB_DATABASE = os.environ.get("BNB_DATABASE") or "blockchain_etl"
    ETHEREUM_DATABASE = os.environ.get("ETHEREUM_DATABASE") or "ethereum_blockchain_etl"
    FANTOM_DATABASE = os.environ.get("FANTOM_DATABASE") or "ftm_blockchain_etl"
    POLYGON_DATABASE = os.environ.get("POLYGON_DATABASE") or "polygon_blockchain_etl"
    ARBITRUM_DATABASE = os.environ.get("ARBITRUM_DATABASE") or "arbitrum_blockchain_etl"
    OPTIMISM_DATABASE = os.environ.get("OPTIMISM_DATABASE") or "optimism_blockchain_etl"
    AVALANCHE_DATABASE = os.environ.get("AVALANCHE_DATABASE") or "avalanche_blockchain_etl"
    TRON_DATABASE = os.environ.get("TRON_DATABASE") or "tron_blockchain_etl"


class MongoDBKLGConfig:
    CONNECTION_URL = os.getenv("MONGODBKLG_CONNECTION_URL")
    DATABASE = os.getenv('MONGODB_DATABASE', 'knowledge_graph')


class MongoDBCommunityConfig:
    CONNECTION_URL = os.getenv("MONGODB_COMMUNITY_CONNECTION_URL",'')
    DATABASE = os.getenv('MONGODB_COMMUNITY_DATABASE', 'CommunityDatabase')
