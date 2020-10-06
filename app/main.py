from datetime import datetime
from decimal import *

import numpy as np
import pymongo
from binance.client import Client
from binance.enums import *
from binance.websockets import BinanceSocketManager
from core.algo import Config, ZI_DCT0, TradeStrategy
from pymongo.errors import DuplicateKeyError

from app.settings import *

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)
getcontext().prec = 5


class Position:
    def __init__(self, price):
        self.price = price


mongo_client = pymongo.MongoClient(MONGO_URL)
db = mongo_client['bat-price-watcher']
roi_db = mongo_client['bat-price-roi']
state_db = mongo_client['bat-price-state']
state_symbols = state_db.collection_names(include_system_collections=False)
logger.info('Symbol with persisted state {}'.format(str(state_symbols)))

client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)

bm = BinanceSocketManager(client)


def create_runner(trade_strategy, LAMBDA, symbol):
    symbol_state_collection = state_db[symbol]
    symbol_states = symbol_state_collection.find({'S': trade_strategy.name, 'L': str(round(Decimal(LAMBDA), 4))}).sort(
        [('_id', pymongo.DESCENDING)]).limit(1)
    dct0_runner = None
    if symbol_states.count() > 0:
        state = symbol_states[0]
        initial_mode = state['E']
        initial_p_ext = state['p_ext']
        config = Config(trade_strategy, LAMBDA, initial_mode, initial_p_ext)
        dct0_runner = ZI_DCT0(logger, config)
    return dct0_runner


start = 0.001
stop = 0.1
step = 0.001
lambdas = np.arange(start, stop, step)

create_runner_params = [(trade_strategy, LAMBDA, symbol) for trade_strategy in TradeStrategy for LAMBDA in lambdas for
                        symbol in SYMBOLS]

start_time = datetime.now()
dct0_runners = [create_runner(*create_runner_param) for create_runner_param in create_runner_params]
dct0_runners = list(filter(None.__ne__, dct0_runners))
logger.info('Created runners in {}'.format(datetime.now() - start_time))


def run(dct0_runner: ZI_DCT0, SYMBOL, p_t, t):
    dct0_runner.observe(p_t, t)
    state_collection = state_db[SYMBOL]
    LAMBDA = dct0_runner.config.delta_p
    strategy_name = dct0_runner.config.strategy.name
    current_event_name = dct0_runner.current_event.name
    start_dc = dct0_runner.t_start_dc
    state = {'L': str(round(Decimal(LAMBDA), 4)), 'S': strategy_name, 'E': current_event_name, 't_s': start_dc,
             't_e': t, 'p_ext': dct0_runner.p_start_dc, 'p_t': p_t}
    if dct0_runner.is_buy_signaled():
        state_collection.update_one({'_id': t}, {'$set': state}, upsert=True)
    elif dct0_runner.is_sell_signaled():
        state_collection.update_one({'_id': t}, {'$set': state}, upsert=True)


def process_kline(event):
    start_time = datetime.now()
    p_t = float(event['k']['c'])
    t = event['E']
    price_doc = {'_id': t, 'p': p_t}
    SYMBOL = event['s']
    try:
        symbol_collection = db[SYMBOL]
        symbol_collection.insert_one(price_doc)
    except DuplicateKeyError:
        logger.error('Duplicate event {}'.format(price_doc['_id']))

    [run(dct0_runner, SYMBOL, p_t, t) for dct0_runner in dct0_runners if SYMBOL in state_symbols]
    logger.info('Process kline in {}'.format(datetime.now() - start_time))


def main():
    for symbol in SYMBOLS:
        logger.info('Starting price collector for {}'.format(symbol))
        bm.start_kline_socket(symbol, process_kline, interval=KLINE_INTERVAL_1MINUTE)
    bm.start()


if __name__ == '__main__':
    main()
