import os
import time

from sanic import json, Request, Unauthorized
from sanic_ext import openapi
from src import create_app
from config import Config
from src.misc.log import log
from src.utils.logger_utils import get_logger

logger = get_logger('Main')
app = create_app(Config)
# app.ext.openapi.raw(Config.raw)


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

if __name__ == '__main__':
    if 'SECRET_KEY' not in os.environ:
        log(message='SECRET KEY is not set in the environment variable.',
            keyword='WARN')

    try:
        app.run(**app.config['RUN_SETTING'])
    except (KeyError, OSError):
        log('End Server...')
