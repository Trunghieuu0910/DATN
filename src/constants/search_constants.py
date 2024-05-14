class SearchConstants:
    transaction = 'transaction'
    block = 'block'
    wallet = 'wallet'
    token = 'token'
    contract = 'contract'
    project = 'project'
    text = 'text'


class Tags:
    address = 'address'
    contract = 'contract'
    dapp = 'dapp'
    lending = 'lending'
    vault = 'vault'
    dex = 'dex'
    token = 'token'
    pair = 'pair'
    nft = 'nft'

    all = [contract, dapp, lending, vault, dex, token, pair]


class QueryTypes:
    demo = 'demo'
    profile = 'profile'
    wallet = 'wallet'
    token = 'token'
    project = 'project'


class DataSources:
    coingecko = 'coingecko'
    defillama = 'defillama'
    opensea = 'opensea'
    coinmarketcap = 'coinmarketcap'


class ProjectTypes:
    defi = 'defi'
    nft = 'nft'
    exchange = 'exchange'

    all = [defi, nft, exchange]


class RelationshipType:
    # Wallet
    deposit = 'deposit'  # => Lending
    borrow = 'borrow'  # => Lending
    swap = 'swap'  # => Vault
    transfer = 'transfer'  # => Wallet
    liquidate = 'liquidate'  # => Wallet
    call_contract = 'call_contract'  # => Contract
    hold = 'hold'  # => Token
    use = 'use'  # => DApp
    tracker = 'tracker'  # => Contract

    # Project
    release = 'release'  # => Token
    subproject = 'subproject'  # => Project
    has_contract = 'has_contract'  # => Contract

    # Contract
    forked_from = 'forked_from'  # => Contract
    addon_contract = 'addon_contract'  # => Contract
    support = 'support'  # => Token
    reward = 'reward'  # => Token
    include = 'include'  # => Token
    exchange = 'exchange'  # => Project


class NotificationType:
    cex_exchange_event = 'cex_exchange_event'
    dex_exchange_event = 'dex_exchange_event'
    token_price = 'token_price'
    exchange_event = [cex_exchange_event, dex_exchange_event]


class WalletTags:
    centralized_exchange_deposit_wallet = 'centralized_exchange_deposit_wallet'
    centralized_exchange_wallet = 'centralized_exchange_wallet'
    lending_wallet = 'lending_wallet'
    nft_users = 'nft_users'

    _dapp_users = '*_users'
    _token_holders = '*_holders'
    _lending_depositors = '*_depositors'
    _lending_borrowers = '*_borrowers'
    _lending_high_risk = '*_high_risk'
    _liquidated_wallet = 'liquidated_wallet_*'

    weights = {
        centralized_exchange_wallet: 1,
        centralized_exchange_deposit_wallet: 2,
        lending_wallet: 100,
        nft_users: 10,
        _dapp_users: 5,
        _token_holders: 6,
        _lending_depositors: 4,
        _lending_borrowers: 3,
        _lending_high_risk: 5,
        # _liquidated_wallet: 2
    }


class ConfigNotification:
    exchanges_config = [{'txLarger': 1000000}]

    defi_config = [{'txLarger': 1000000}]

    nft_config = [{'value': 10000, 'isAbove': True, 'unit': 'usd'},
                  {'value': 100, 'isAbove': True, 'unit': 'percent', 'period': 24},
                  {'value': 10000, 'isAbove': False, 'unit': 'usd'},
                  {'value': 100, 'isAbove': False, 'unit': 'percent', 'period': 24}]

    token_config = [{'value': 10000, 'isAbove': True, 'unit': 'usd'},
                    {'value': 100, 'isAbove': True, 'unit': 'percent', 'period': 24},
                    {'value': 10000, 'isAbove': False, 'unit': 'usd'},
                    {'value': 100, 'isAbove': False, 'unit': 'percent', 'period': 24}]

    wallet_config = [{'value': 1000000, 'isAbove': True, 'unit': 'usd'},
                     {'value': 100, 'isAbove': True, 'unit': 'percent', 'period': 24},
                     {'value': 1000000, 'isAbove': False, 'unit': 'usd'},
                     {'value': 100, 'isAbove': False, 'unit': 'percent', 'period': 24},
                     {"txLarger": 1000000}, {"healthRate": 1.2}]

    @classmethod
    def get_config(cls, _type):
        if _type == 'token':
            return cls.token_config
        elif _type == 'nft':
            return cls.nft_config
        elif _type == 'exchange':
            return cls.exchanges_config
        elif _type == 'wallet':
            return cls.wallet_config
        elif _type == 'defi':
            return cls.defi_config
        else:
            return []
