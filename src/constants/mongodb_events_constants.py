class TxConstants:
    id = "_id"
    type = "type"
    hash = "hash"
    from_address = "from_address"
    to_address = "to_address"
    value = "value"
    gas = "gas"
    receipt_gas_used = 'receipt_gas_used'
    gas_price = "gas_price"
    block_number = "block_number"
    block_timestamp = "block_timestamp"
    status = "receipt_status"
    receipt_contract_address = 'receipt_contract_address'
    transaction_type = "transaction_type"
    input = "input"
    data = [type, hash, from_address, to_address, value, gas, receipt_gas_used, gas_price,
            block_number, block_timestamp, status, transaction_type, input, receipt_contract_address]


class BlockConstants:
    id = "_id"
    type = "type"
    number = "number"
    hash = "hash"
    miner = "miner"
    size = "size"
    gas_limit = "gas_limit"
    gas_used = "gas_used"
    timestamp = "timestamp"
    transaction_count = "transaction_count"
    difficult = "difficulty"
    total_difficult = "total_difficulty"
    data = [type, number, hash, miner, size, gas_limit, gas_used,
            timestamp, difficult, total_difficult, transaction_count]
