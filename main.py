import asyncio
import os
import pickle

import numpy as np
import pandas as pd
from sanic import json, Request
from sanic.exceptions import BadRequest
from sanic_ext import openapi
from web3 import Web3

from config import Config
from src import create_app
from src.constants.network_constants import Chain
from src.database.mongodb.mongodb import MongoDB
from src.misc.log import log
from src.service.address.address_service import AddressService
from src.service.transaction.TransactionAnalysis import TransactionsAnalysis
from src.utils.logger_utils import get_logger

logger = get_logger('Main')
app = create_app(Config)


async def sleep(request):
    await asyncio.sleep(3)  # Sử dụng asyncio.sleep thay vì time.sleep


@app.before_server_start
async def setup_db(_):
    app.ctx.db = MongoDB()
    log(f'Connected to KLG Database {app.ctx.db}')
    file_path = 'models_8.pickle'

    # Mở file pickle và tải mô hình
    with open(file_path, 'rb') as file:
        app.ctx.model = pickle.load(file)


@app.route("/ping", methods={'GET'})
# @openapi.exclude()
@openapi.tag("Ping")
@openapi.summary("Ping server !")
async def hello_world(request: Request):
    response = json({
        "description": "Success",
        "status": 200,
        "message": f"App {request.app.name}: Hello, World !!!"
    })
    return response


@app.route('/address/<address>', methods=['GET'])
@openapi.tag('Address')
@openapi.summary('Get a address location')
@openapi.parameter(name="address", description="Address", location="path", required=True)
async def get_address_location(request: Request, address):
    if not Web3.is_address(address):
        raise BadRequest('Invalid address')

    db_: MongoDB = request.app.ctx.db
    doc = db_.get_address(address=address)
    if doc:
        if doc.get('regional', None):
            return json({
                address: doc.get('regional')
            })

    tx_anan = TransactionsAnalysis()
    address_service = AddressService()
    infor = await tx_anan.get_tx_each_chain_of_address(address)
    infor = tx_anan.get_info_all_chain_of_address(infor, address)
    infor, token = address_service.get_information_of_address(infor, address)
    if infor is None:
        raise BadRequest(f'Address {address} is not enough information to predict')
    update_infor = infor.copy()
    update_infor['transactions'] = {}
    for i in range(24):
        update_infor['transactions'][str(i)] = update_infor.pop(i)

    infor.update(token)
    del infor['address']
    del infor['means']
    model = request.app.ctx.model
    regional = get_regional(model, infor)
    update_infor.update({'regional': regional})
    db_.update_address(update_infor)
    res = {address: regional}
    return json(res)


@app.route('/addresses/', methods=['POST'])
@openapi.tag('Address')
@openapi.summary('Get a address location')
async def get_addresses_location(request: Request):
    addresses = request.json.get("addresses")
    if not addresses:
        raise BadRequest("Not addresses to execute")

    for address in addresses:
        address = address.lower()
        address = address.strip()
        if not Web3.is_address(address):
            raise BadRequest(f'Invalid address on address {address}')

    db_: MongoDB = request.app.ctx.db
    cursor = db_.get_addresses(addresses=addresses)
    res = []
    if cursor:
        for doc in cursor:
            res.append({'address': doc.get('address'), 'regional': doc.get('regional')})
            addresses.remove(doc.get('address'))

        if len(addresses) == 0:
            return json(res)

    tx_anan = TransactionsAnalysis()
    address_service = AddressService()
    for address in addresses:
        infor = await tx_anan.get_tx_each_chain_of_address(address)
        infor = tx_anan.get_info_all_chain_of_address(infor, address)
        infor, token = address_service.get_information_of_address(infor, address)
        if infor is None:
            raise BadRequest(f'Address {address} is not enough information to predict')
        update_infor = infor.copy()
        update_infor['transactions'] = {}
        for i in range(24):
            update_infor['transactions'][str(i)] = update_infor.pop(i)

        infor.update(token)
        del infor['address']
        del infor['means']
        model = request.app.ctx.model
        regional = get_regional(model, infor)
        update_infor.update({'regional': regional})
        db_.update_address(update_infor)
        res.append({'address': address, 'regional': regional})
    return json(res)


@app.route('/transactions/', methods=['POST'])
@openapi.tag('Address')
@openapi.summary('Get a address location')
async def get_transaction_of_address(request: Request):

    addresses = request.json.get("addresses")
    if not addresses:
        raise BadRequest("Not addresses to execute")
    print(addresses)
    for address in addresses:
        if not Web3.is_address(address):
            raise BadRequest('Invalid address')

    db_: MongoDB = request.app.ctx.db
    import time
    start = int(time.time())
    end = start + 30
    while True:
        cursor = db_.get_addresses(addresses=addresses)
        if int(time.time()) > end:
            raise BadRequest('Time out')
        elif len(cursor) < len(addresses):
            await asyncio.sleep(3)
        elif len(cursor) == len(addresses):
            break

    regionals = {}
    for doc in cursor:
        regional = doc.get('regional')
        if regional not in regionals:
            regionals[regional] = {'transactions': {}, 'means': []}
        transactions = doc.get('transactions')
        print(f"{doc.get('_id')} {transactions}")
        for time, value in transactions.items():
            if time not in regionals[regional]['transactions']:
                regionals[regional]['transactions'][time] = value
            else:
                regionals[regional]['transactions'][time] += value

        means = doc.get('means', [])
        regionals[regional]['means'] += means

    return json(regionals)


def get_regional(model, infor):
    df = pd.DataFrame(infor, index=[0])
    prediction = model.predict(df)
    max_index = np.argmax(prediction)
    max_index = int(max_index)
    regional = Chain.country[max_index]
    if regional == 'jp_kr_cn':
        regional = 'southern_asia'
    return regional


if __name__ == '__main__':
    if 'SECRET_KEY' not in os.environ:
        log(message='SECRET KEY is not set in the environment variable.',
            keyword='WARN')

    try:
        app.run(**app.config['RUN_SETTING'])
    except (KeyError, OSError):
        log('End Server...')
