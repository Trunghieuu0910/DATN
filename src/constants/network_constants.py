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


class ChainsAnkr:
    """Chain name for Ankr API"""
    arbitrum = '0xa4b1'
    avalanche = '0xa86a'
    bsc = '0x38'
    eth = '0x1'
    fantom = '0xfa'
    polygon = '0x89'
    syscoin = '0x57'
    optimism = '0xa'
    tron = '0x2b6653dc'

    reversed_mapping = {
        bsc: 'bsc',
        eth: 'eth',
        fantom: 'fantom',
        polygon: 'polygon',
        avalanche: 'avalanche',
        arbitrum: 'arbitrum',
        optimism: 'optimism',
        tron: 'tron'
    }

    @staticmethod
    def get_chain_name(chain_id):
        return ChainsAnkr.reversed_mapping.get(chain_id)


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


class NativeTokens:
    bnb = {
        'id': 'binancecoin',
        'type': 'token',
        'name': 'BNB',
        'symbol': 'BNB',
        'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FBNB.png?alt=media&token=b0a77aea-6f98-4916-9dbf-ffdc9b44c2c3'
    }
    ether = {
        'id': 'ethereum',
        'type': 'token',
        'name': 'Ether',
        'symbol': 'ETH',
        'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FETH.png?alt=media&token=55db834b-029b-4237-9b30-f5fd28d7b2f4'
    }
    ftm = {
        'id': 'fantom',
        'type': 'token',
        'name': 'Fantom',
        'symbol': 'FTM',
        'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FFTM.png?alt=media&token=0fc3758c-9aa3-491b-904b-46fabb097447'
    }
    matic = {
        'id': 'matic',
        'type': 'token',
        'name': 'Matic',
        'symbol': 'MATIC',
        'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FMATIC.png?alt=media&token=f3dd80ba-b045-40ba-9c8c-ee0d9617d798'
    }
    avax = {
        'id': 'avalanche-2',
        'type': 'token',
        'name': 'Avalanche',
        'symbol': 'AVAX',
        'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FAVAX.png?alt=media&token=1e01b02f-0fb2-4887-b84d-837a4e2880dd'
    }
    tron = {
        'id': 'tron',
        'type': 'token',
        'name': 'Tron',
        'symbol': 'TRX',
        'imgUrl': 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FTRX.png?alt=media&token=85e1e5a3-26bc-433b-81dd-f2733c7ffe80'
    }
    cronos = {
        'id': 'crypto-com-chain',
        'type': 'token',
        'name': 'Cronos',
        'symbol': 'CRO',
        'imgUrl': 'https://assets.coingecko.com/coins/images/7310/large/cro_token_logo.png'
    }
    solana = {
        'id': 'solana',
        'type': 'token',
        'name': 'Solana',
        'symbol': 'SOL',
        'imgUrl': 'https://assets.coingecko.com/coins/images/4128/large/solana.png'
    }
    polkadot = {
        'id': 'polkadot',
        'type': 'token',
        'name': 'Polkadot',
        'symbol': 'DOT',
        'imgUrl': 'https://assets.coingecko.com/coins/images/12171/large/polkadot.png'
    }
    mapping = {
        Chain.ETH: ether,
        Chain.BSC: bnb,
        Chain.POLYGON: matic,
        Chain.FTM: ftm,
        Chain.ARBITRUM: ether,
        Chain.OPTIMISM: ether,
        Chain.AVALANCHE: avax,
        Chain.TRON: tron,
        Chain.CRONOS: cronos,
        Chain.SOLANA: solana,
        Chain.POLKADOT: polkadot
    }


class DefiServiceQuery:
    token_balance = 'token_balance'
    nft_balance = 'nft_balance'
    deposit_borrow = 'deposit_borrow'
    staking_reward = 'staking_reward'
    protocol_reward = 'protocol_reward'
    protocol_apy = 'protocol_apy'
    dex_user_info = 'dex_user_info'
    lp_token_list = 'lp_token_list'
    farming_lp_token_list = 'farming_lp_token_list'
    lp_token_info = 'lp_token_info'
    token_pair_balance = 'token_pair_balance'
    dex_user_nft = 'dex_user_nft'


DEFAULT_CREDIT_SCORE = 300

BNB = '0x0000000000000000000000000000000000000000'
WBNB = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'

EMPTY_TOKEN_IMG = 'https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2Fempty-token.png?alt=media&token=2f9dfcc1-88a0-472c-a51f-4babc0c583f0'

WRAPPED_NATIVE_TOKENS = {
    Chain.BSC: '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c',
    Chain.ETH: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    Chain.FTM: '0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83',
    Chain.POLYGON: '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270',
    Chain.ARBITRUM: '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
    Chain.OPTIMISM: '0x4200000000000000000000000000000000000006',
    Chain.AVALANCHE: '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7',
    Chain.TRON: '0x891cdb91d149f23b1a45d9c5ca78a88d0cb44c18',
    Chain.CRONOS: '0x5c7f8a570d578ed84e63fdfa7b1ee72deae1ae23'
}

COMMON_NOT_SUPPORTED = [Chain.AVALANCHE_X, Chain.AVALANCHE_P, Chain.SOLANA, Chain.POLKADOT, Chain.CRONOS]
PROFILER_NOT_SUPPORTED = [Chain.AVALANCHE_X, Chain.AVALANCHE_P, Chain.SOLANA, Chain.POLKADOT, Chain.TRON, Chain.CRONOS]
PROFILE_NOT_SUPPORTED = [Chain.AVALANCHE_X, Chain.AVALANCHE_P, Chain.SOLANA, Chain.POLKADOT, Chain.TRON]
EVM_NOT_SUPPORTED = [Chain.AVALANCHE_X, Chain.AVALANCHE_P, Chain.SOLANA, Chain.POLKADOT]
