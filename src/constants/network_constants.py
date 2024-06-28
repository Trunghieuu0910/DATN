import os

from dotenv import load_dotenv

load_dotenv()


class NetworkType:
    BSC = 'bsc'
    ETH = 'ethereum'
    FTM = 'ftm'
    POLYGON = 'polygon'
    ARBITRUM = 'arbitrum'
    OPTIMISM = 'optimism'
    AVALANCHE = 'avalanche'
    AVALANCHE_X = 'avalanche_x'
    AVALANCHE_P = 'avalanche_p'
    TRON = 'tron'
    CRONOS = 'cronos'
    SOLANA = 'solana'
    POLKADOT = 'polkadot'


class Chain:
    BSC = '0x38'
    ETH = '0x1'
    FTM = '0xfa'
    POLYGON = '0x89'
    ARBITRUM = '0xa4b1'
    OPTIMISM = '0xa'
    AVALANCHE = '0xa86a'
    AVALANCHE_X = 'x-avax'
    AVALANCHE_P = 'p-avax'
    TRON = '0x2b6653dc'
    CRONOS = '0x19'
    SOLANA = 'solana'
    POLKADOT = 'polkadot'

    mapping = {
        NetworkType.BSC: BSC,
        NetworkType.ETH: ETH,
        NetworkType.FTM: FTM,
        NetworkType.POLYGON: POLYGON,
        NetworkType.ARBITRUM: ARBITRUM,
        NetworkType.OPTIMISM: OPTIMISM,
        NetworkType.AVALANCHE: AVALANCHE,
        NetworkType.AVALANCHE_X: AVALANCHE_X,
        NetworkType.AVALANCHE_P: AVALANCHE_P,
        NetworkType.TRON: TRON,
        NetworkType.CRONOS: CRONOS,
        NetworkType.SOLANA: SOLANA,
        NetworkType.POLKADOT: POLKADOT
    }

    chain_names = {
        BSC: 'BSC',
        ETH: 'Ethereum',
        FTM: 'Fantom',
        POLYGON: 'Polygon',
        ARBITRUM: 'Arbitrum',
        OPTIMISM: 'Optimism',
        AVALANCHE: 'Avalanche C-Chain',
        AVALANCHE_X: 'Avalanche X-Chain',
        AVALANCHE_P: 'Avalanche P-Chain',
        TRON: 'Tron',
        CRONOS: 'Cronos',
        SOLANA: 'Solana',
        POLKADOT: 'Polkadot'
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
        FTM: 'https://ftmscan.com/',
        POLYGON: 'https://polygonscan.com/',
        ARBITRUM: 'https://arbiscan.io/',
        OPTIMISM: 'https://optimistic.etherscan.io/',
        AVALANCHE: 'https://snowtrace.io/',
        TRON: 'https://tronscan.org/',
        CRONOS: 'https://cronoscan.com/'
    }

    estimate_block_time = {
        BSC: 3,
        ETH: 12,
        FTM: 1,
        POLYGON: 2,
        ARBITRUM: 0.3,
        OPTIMISM: 1,  # TODO: check
        AVALANCHE: 2,
        TRON: 3,
        CRONOS: 6
    }

    @classmethod
    def get_all_chain_id(cls):
        return [
            cls.BSC, cls.ETH, cls.FTM, cls.POLYGON, cls.ARBITRUM, cls.OPTIMISM, cls.AVALANCHE,
            cls.AVALANCHE_X, cls.AVALANCHE_P, cls.TRON, cls.CRONOS, cls.SOLANA, cls.POLKADOT
        ]

    @classmethod
    def evm_chains(cls):
        return [
            cls.BSC, cls.ETH, cls.FTM, cls.POLYGON,
            cls.ARBITRUM, cls.OPTIMISM, cls.AVALANCHE, cls.CRONOS
        ]

    @classmethod
    def evm_and_tron_chains(cls):
        return [
            cls.BSC, cls.ETH, cls.FTM, cls.POLYGON,
            cls.ARBITRUM, cls.OPTIMISM, cls.AVALANCHE, cls.TRON, cls.CRONOS
        ]

    @classmethod
    def non_evm_chains(cls):
        return [cls.POLKADOT, cls.SOLANA, cls.AVALANCHE_X, cls.AVALANCHE_P, cls.TRON]

    @classmethod
    def non_evm_ignore_tron_chains(cls):
        return [cls.POLKADOT, cls.SOLANA, cls.AVALANCHE_X, cls.AVALANCHE_P]


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
        Chain.FTM: ftm_provider_uri[0],
        Chain.POLYGON: polygon_provider_uri[0],
        Chain.ARBITRUM: arbitrum_provider_uri[0],
        Chain.OPTIMISM: optimism_provider_uri[0],
        Chain.AVALANCHE: avalanche_provider_uri[0],
        Chain.TRON: tron_provider_uri[0],
        Chain.CRONOS: cronos_provider_uri[0],
        Chain.SOLANA: solana_provider_uri[0],
        Chain.POLKADOT: polkadot_provider_uri[0]
    }

    pools = {
        Chain.BSC: bsc_provider_uri,
        Chain.ETH: eth_provider_uri,
        Chain.FTM: ftm_provider_uri,
        Chain.POLYGON: polygon_provider_uri,
        Chain.ARBITRUM: arbitrum_provider_uri,
        Chain.OPTIMISM: optimism_provider_uri,
        Chain.AVALANCHE: avalanche_provider_uri,
        Chain.TRON: tron_provider_uri,
        Chain.CRONOS: cronos_provider_uri,
        Chain.SOLANA: solana_provider_uri,
        Chain.POLKADOT: polkadot_provider_uri
    }

    archive_providers = {
        Chain.BSC: os.getenv('BSC_ARCHIVE_PROVIDER_URI', 'https://rpc.ankr.com/bsc'),
        Chain.ETH: os.getenv('ETH_ARCHIVE_PROVIDER_URI', 'https://rpc.ankr.com/eth'),
        Chain.POLYGON: os.getenv('POLYGON_ARCHIVE_PROVIDER_URI', 'https://rpc.ankr.com/polygon'),
        Chain.FTM: os.getenv('FTM_ARCHIVE_PROVIDER_URI', 'https://rpc.ankr.com/fantom'),
        Chain.ARBITRUM: os.getenv('ARBITRUM_ARCHIVE_PROVIDER_URI', 'https://rpc.ankr.com/arbitrum'),
        Chain.OPTIMISM: os.getenv('OPTIMISM_ARCHIVE_PROVIDER_URI', 'https://rpc.ankr.com/optimism'),
        Chain.AVALANCHE: os.getenv('AVALANCHE_ARCHIVE_PROVIDER_URI', 'https://rpc.ankr.com/avalanche'),
        Chain.TRON: os.getenv('TRON_ARCHIVE_PROVIDER_URI', 'https://rpc.ankr.com/tron_jsonrpc'),
        Chain.CRONOS: os.getenv('CRONOS_ARCHIVE_PROVIDER_URI'),
        Chain.SOLANA: os.getenv('SOLANA_ARCHIVE_PROVIDER_URI'),
        Chain.POLKADOT: os.getenv('POLKADOT_ARCHIVE_PROVIDER_URI')
    }


def BNB():
    return None