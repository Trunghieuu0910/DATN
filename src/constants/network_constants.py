import os

from dotenv import load_dotenv

load_dotenv()


class NetworkType:
    BSC = 'bsc'
    ETH = 'ethereum'
    POLYGON = 'polygon'


class Chain:
    BSC = '0x38'
    ETH = '0x1'
    POLYGON = '0x89'

    mapping = {
        NetworkType.BSC: BSC,
        NetworkType.ETH: ETH,
        NetworkType.POLYGON: POLYGON,
    }

    chain_names = {
        BSC: 'BSC',
        ETH: 'Ethereum',
        POLYGON: 'Polygon'
    }

    token_price = {
        BSC: 580,
        ETH: 2980,
        POLYGON: 0.68
    }

    country = ['africa', 'america', 'europe', 'jp_kr_cn', 'southeast_asia', 'southern_asia']

    api_key = {
        BSC: os.getenv('BSC_API_KEY'),
        ETH: os.getenv('ETH_API_KEY'),
        POLYGON: os.getenv('POLYGON_API_KEY')
    }

    web3_scan = {
        ETH: 'https://api.etherscan.io',
        BSC: 'https://api.bscscan.com',
        POLYGON: 'https://api.polygonscan.com'
    }

    explorers = {
        BSC: 'https://bscscan.com/',
        ETH: 'https://etherscan.io/',
        POLYGON: 'https://polygonscan.com/',
    }

    estimate_block_time = {
        BSC: 3,
        ETH: 12,
        POLYGON: 2
    }

    @classmethod
    def get_all_chain_id(cls):
        return [
            cls.BSC, cls.ETH, cls.POLYGON
        ]

    @classmethod
    def evm_chains(cls):
        return [
            cls.BSC, cls.ETH, cls.POLYGON,
        ]


class ProviderURI:
    query_batch_size = int(os.getenv('BLOCKCHAIN_QUERY_BATCH_SIZE', 100))

    bsc_provider_uri = os.getenv('BSC_PROVIDER_URI', 'https://rpc.ankr.com/bsc').split(',')
    eth_provider_uri = os.getenv('ETH_PROVIDER_URI', 'https://rpc.ankr.com/eth').split(',')
    ftm_provider_uri = os.getenv('FTM_PROVIDER_URI', 'https://rpc.ankr.com/fantom').split(',')
    polygon_provider_uri = os.getenv('POLYGON_PROVIDER_URI', 'https://rpc.ankr.com/polygon').split(',')
    arbitrum_provider_uri = os.getenv('ARBITRUM_PROVIDER_URI', 'https://rpc.ankr.com/arbitrum').split(',')
    optimism_provider_uri = os.getenv('OPTIMISM_PROVIDER_URI', 'https://rpc.ankr.com/optimism').split(',')
    avalanche_provider_uri = os.getenv('AVALANCHE_PROVIDER_URI', 'https://rpc.ankr.com/avalanche').split(',')
    tron_provider_uri = os.getenv('TRON_PROVIDER_URI', 'https://rpc.ankr.com/tron_jsonrpc').split(',')
    cronos_provider_uri = os.getenv('CRONOS_PROVIDER_URI').split(',')
    solana_provider_uri = os.getenv('SOLANA_PROVIDER_URI').split(',')
    polkadot_provider_uri = os.getenv('POLKADOT_PROVIDER_URI').split(',')

    mapping = {
        Chain.BSC: bsc_provider_uri[0],
        Chain.ETH: eth_provider_uri[0],
        Chain.POLYGON: polygon_provider_uri[0],
    }

    pools = {
        Chain.BSC: bsc_provider_uri,
        Chain.ETH: eth_provider_uri,
        Chain.POLYGON: polygon_provider_uri,
    }

    archive_providers = {
        Chain.BSC: os.getenv('BSC_ARCHIVE_PROVIDER_URI', 'https://rpc.ankr.com/bsc'),
        Chain.ETH: os.getenv('ETH_ARCHIVE_PROVIDER_URI', 'https://rpc.ankr.com/eth'),
        Chain.POLYGON: os.getenv('POLYGON_ARCHIVE_PROVIDER_URI', 'https://rpc.ankr.com/polygon'),
    }


def BNB():
    return None