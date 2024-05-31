import os
from textwrap import dedent
from dotenv import load_dotenv

load_dotenv()


class Config:
    RUN_SETTING = {
        'host': os.environ.get('SERVER_HOST', 'localhost'),
        'port': int(os.environ.get('SERVER_PORT', 8080)),
        'debug': os.getenv('DEBUG', False),
        "access_log": False,
        "auto_reload": True,
        'workers': int(os.getenv('SERVER_WORKERS', 4))
    }
    # uWSGI를 통해 배포되어야 하므로, production level에선 run setting을 건드리지 않음

    SECRET = os.environ.get('SECRET_KEY', 'example project')
    JWT_PASSWORD = os.getenv('JWT_PASSWORD', 'dev123')
    EXPIRATION_JWT = 2592000  # 1 month
    RESPONSE_TIMEOUT = 900  # seconds

    SERVER_NAME = os.getenv('SERVER_NAME')

    # To reorder swagger tags
    raw = {
        'tags': [
            {'name': 'Common'}, {'name': 'Ranking'}, {'name': 'Entity'},
            {'name': 'Search'}, {'name': 'Wallet'}, {'name': 'Token'}, {'name': 'NFT'},
            {'name': 'Lending'}, {'name': 'Cexes'}, {'name': 'Dexes'}, {'name': 'Whales'},
            {'name': 'Recommend'}, {'name': 'Score'}, {'name': 'CDP'}, {'name': 'Explore Users'},
            {'name': 'Social Media'}
        ]
    }
    if SERVER_NAME:
        raw['servers'] = [{'url': SERVER_NAME}]

    FALLBACK_ERROR_FORMAT = 'json'

    OAS_UI_DEFAULT = 'swagger'
    SWAGGER_UI_CONFIGURATION = {
        'apisSorter': "alpha",
        'docExpansion': "list",
        'operationsSorter': "alpha"
    }

    API_HOST = os.getenv('API_HOST', '0.0.0.0:8096')
    API_BASEPATH = os.getenv('API_BASEPATH', '')
    API_SCHEMES = os.getenv('API_SCHEMES', 'http')
    API_VERSION = os.getenv('API_VERSION', '0.1.0')
    API_TITLE = os.getenv('API_TITLE', 'Centic Data')
    API_CONTACT_EMAIL = os.getenv('API_CONTACT_EMAIL', 'example@gmail.com')

    API_DESCRIPTION = os.getenv('API_DESCRIPTION', dedent(
        """
        ## Explore the Entity-Oriented API

        Power your AI application with Centic Data API focus on entity: Wallet, digital assets, DApps, and Foundation.

        Come with us to accelerate your Growth.
        - Our Centic API has a rate limit of 200 calls/5 minutes.
        - Need something more powerful? [Contact us](https://centic.io).
        """
    ))


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
    CONNECTION_URL = os.getenv("MONGODB_COMMUNITY_CONNECTION_URL", '')
    DATABASE = os.getenv('MONGODB_COMMUNITY_DATABASE', 'CommunityDatabase')
